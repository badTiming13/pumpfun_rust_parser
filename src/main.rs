use std::{fs, time::Duration};

mod config;
mod db;
mod prelude;
mod pump_amm_utils;
mod pumpfun_utils;
mod types;
mod utils;

use std::error::Error;

type AppResult<T> = Result<T, Box<dyn Error>>;

use clickhouse::Client;
use futures::{SinkExt, StreamExt};
use serde_json::json;
use tokio_tungstenite::connect_async;

use redis::aio::MultiplexedConnection;
use redis::Client as RedisClient;

use crate::utils::publish_event;
use crate::{
    db::{PumpCreateRow, PumpCreatorFeeRow, PumpTradeRow, amm_types::AmmTradeRow},
    prelude::*,
    utils::{
        AmmEventContext, EventContext, InstructionContext, collect_instructions, instructions,
        join_amm_ix_and_events, join_pump_ix_and_events, load_db, match_and_collect,
        match_and_print,
    },
};

#[tokio::main]
async fn main() -> AppResult<()> {
    let (pump_idl, amm_idl) = load_idls();
    let ch_client = load_db();

    let redis_client = RedisClient::open("redis://127.0.0.1/")?;
    let redis_conn = redis_client.get_multiplexed_async_connection().await?;

    // ClickHouse client можно клонировать – внутри он cheap-clone
    let pump_ch = ch_client.clone();
    let amm_ch  = ch_client.clone();

    let pump_idl_clone = pump_idl.clone();
    let amm_idl_clone  = amm_idl.clone();

    // Отдельный коннект под Pumpfun
    let pump_redis_conn = redis_client.get_multiplexed_async_connection().await?;
    // Отдельный коннект под AMM
    let amm_redis_conn  = redis_client.get_multiplexed_async_connection().await?;


    // URL ноды
    let url = "wss://solana-mainnet.core.chainstack.com/3dec72ea492a69e1ea1fa532c2de1af7";

    // Запускаем два независимых таска: Pumpfun и AMM
    let pump_task = tokio::spawn(async move {
        if let Err(e) = run_pumpfun_stream(url, pump_idl_clone, &pump_ch, pump_redis_conn).await {
            eprintln!("Pumpfun stream error: {e}");
        }
    });

    let amm_task = tokio::spawn(async move {
        if let Err(e) = run_amm_stream(url, amm_idl_clone, &amm_ch, amm_redis_conn).await {
            eprintln!("AMM stream error: {e}");
        }
    });

    // Ждём оба таска (они по идее вечные)
    let _ = tokio::join!(pump_task, amm_task);

    Ok(())
}

async fn run_pumpfun_stream(
    url: &str,
    idl: PumpIdl,
    ch_client: &Client,
    mut redis_conn: MultiplexedConnection,
) -> AppResult<()> {
    loop {
        println!("🔌 Connecting Pumpfun stream...");

        let (mut ws_stream, _response) = connect_async(url).await?;
        println!("✅ Pumpfun connected to {url}");

        // Подписка на блоки, где упоминается Pumpfun program
        let subscribe_msg = json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "blockSubscribe",
            "params": [
                {
                    "mentionsAccountOrProgram": PUMPFUN_PROGRAM_ADDRESS
                },
                {
                    "commitment": "confirmed",
                    "encoding": "json",
                    "transactionDetails": "full",
                    "maxSupportedTransactionVersion": 0,
                    "showRewards": false
                }
            ]
        });

        ws_stream
            .send(tokio_tungstenite::tungstenite::Message::Text(
                subscribe_msg.to_string(),
            ))
            .await?;

        println!("📨 Pumpfun subscription sent");

        while let Some(msg) = ws_stream.next().await {
            match msg {
                Ok(tokio_tungstenite::tungstenite::Message::Text(text)) => {
                    // 1) пробуем распарсить как наш BlockNotification
                    let parsed = serde_json::from_str::<BlockNotification>(&text);
                    let Ok(notification) = parsed else {
                        // Скорее всего это ответ на subscribe: {"result":..., "id":1}
                        // или что-то служебное — просто лог и дальше
                        // println!("Pumpfun: non-block message: {text}");
                        continue;
                    };

                    let txs = &notification.params.result.value.block.transactions;

                    if txs.is_empty() {
                        continue;
                    }

                    println!(
                        "🧱 Pumpfun block: {} transactions",
                        txs.len()
                    );

                    if let Err(e) = process_pump_transactions(txs, &idl, ch_client, &mut redis_conn).await {
                        eprintln!("Pumpfun process error: {e}");
                    }
                }

                Ok(tokio_tungstenite::tungstenite::Message::Binary(_bin)) => {
                    // у нас encoding=json, но на всякий случай игнорим бинарь
                    continue;
                }

                Ok(tokio_tungstenite::tungstenite::Message::Ping(p)) => {
                    ws_stream
                        .send(tokio_tungstenite::tungstenite::Message::Pong(p))
                        .await?;
                }

                Ok(tokio_tungstenite::tungstenite::Message::Close(frame)) => {
                    println!("Pumpfun WS closed: {:?}", frame);
                    break; // выйдем из внутреннего while и переподключимся
                }

                Err(e) => {
                    eprintln!("Pumpfun WS error: {e}");
                    break; // переподключиться
                }

                _ => {}
            }
        }

        println!("🔁 Pumpfun reconnect in 3s...");
        tokio::time::sleep(Duration::from_secs(3)).await;
    }
}


async fn run_amm_stream(
    url: &str,
    idl: PumpIdl,
    ch_client: &Client,
    mut redis_conn: MultiplexedConnection,
) -> AppResult<()> {
    loop {
        println!("🔌 Connecting AMM stream...");

        let (mut ws_stream, _response) = connect_async(url).await?;
        println!("✅ AMM connected to {url}");

        let subscribe_msg = json!({
            "jsonrpc": "2.0",
            "id": 2,
            "method": "blockSubscribe",
            "params": [
                {
                    "mentionsAccountOrProgram": PUMPSWAP_PROGRAM_ADDRESS
                },
                {
                    "commitment": "confirmed",
                    "encoding": "json",
                    "transactionDetails": "full",
                    "maxSupportedTransactionVersion": 0,
                    "showRewards": false
                }
            ]
        });

        ws_stream
            .send(tokio_tungstenite::tungstenite::Message::Text(
                subscribe_msg.to_string(),
            ))
            .await?;

        println!("📨 AMM subscription sent");

        while let Some(msg) = ws_stream.next().await {
            match msg {
                Ok(tokio_tungstenite::tungstenite::Message::Text(text)) => {
                    let parsed = serde_json::from_str::<BlockNotification>(&text);
                    let Ok(notification) = parsed else {
                        // ответ на subscribe и т.п.
                        continue;
                    };

                    let txs = &notification.params.result.value.block.transactions;

                    if txs.is_empty() {
                        continue;
                    }

                    println!(
                        "🧱 AMM block: {} transactions",
                        txs.len()
                    );

                    if let Err(e) = process_amm_transactions(txs, &idl, ch_client, &mut redis_conn).await {
                        eprintln!("AMM process error: {e}");
                    }
                }

                Ok(tokio_tungstenite::tungstenite::Message::Ping(p)) => {
                    ws_stream
                        .send(tokio_tungstenite::tungstenite::Message::Pong(p))
                        .await?;
                }

                Ok(tokio_tungstenite::tungstenite::Message::Close(frame)) => {
                    println!("AMM WS closed: {:?}", frame);
                    break;
                }

                Err(e) => {
                    eprintln!("AMM WS error: {e}");
                    break;
                }

                _ => {}
            }
        }

        println!("🔁 AMM reconnect in 3s...");
        tokio::time::sleep(Duration::from_secs(3)).await;
    }
}


// =======================
// PUMPFUN PROCESSING
// =======================

async fn process_pump_transactions(
    transactions: &Vec<Transaction>,
    idl: &PumpIdl,
    ch_client: &Client,
    redis_conn: &mut MultiplexedConnection,
) -> AppResult<()> {
    let mut trade_rows: Vec<PumpTradeRow> = Vec::new();
    let mut create_rows: Vec<PumpCreateRow> = Vec::new();
    let mut fee_rows: Vec<PumpCreatorFeeRow> = Vec::new();

    for (tx_idx, tx) in transactions.iter().enumerate() {
        println!("\n================ PUMPFUN TX #{tx_idx} ================");

        let Some((pump_instructions, pump_inner_instructions)) =
            collect_instructions(tx, PUMPFUN_PROGRAM_ADDRESS)
        else {
            println!("Pumpfun program not found in this tx, skipping");
            continue;
        };

        let signature = tx.transaction.signatures.get(0).unwrap().to_string();
        println!("Signature: {}", signature);

        // 1) Инструкции
        let ix_contexts = instructions(
            tx,
            idl,
            &pump_instructions,
            &pump_inner_instructions,
            tx_idx,
        )?;
        println!("pump ix_contexts: {:#?}", ix_contexts);

        // 2) События
        let mut event_contexts: Vec<EventContext> = Vec::new();
        for (log_idx, line) in tx
            .meta
            .log_messages
            .as_deref()
            .unwrap_or(&[])
            .iter()
            .enumerate()
        {
            if let Some(event) = decode_pump_event_from_log(line)? {
                event_contexts.push(EventContext {
                    log_index: log_idx,
                    raw_log: line.clone(),
                    event,
                });
            }
        }
        println!("pump event_contexts: {:#?}", event_contexts);

        // 3) Джойним
        let joined_actions = join_pump_ix_and_events(&ix_contexts, &event_contexts);

        println!("PUMPFUN joined actions (per logical op):");

        for action in &joined_actions {
            println!("================ PUMPFUN ACTION ================");
            println!("ix_name:   {}", action.ix.ix_name);
            println!("ix_label:  {}", action.ix.label);
            println!("ix_index:  {}", action.ix.ix_index);
            println!("is_inner:  {}", action.ix.is_inner);
            println!("Accounts: {:#?}", &action.ix.accounts);

            match action.event.event {
                PumpEvent::Trade(_) => {
                    if let Some(row) = PumpTradeRow::from_joined(&signature, action) {
                        trade_rows.push(row.clone());
                        publish_event(redis_conn, "pump:trades", &row).await?;
                    }
                }
                PumpEvent::Create(_) => {
                    if let Some(row) = PumpCreateRow::from_joined(&signature, action) {
                        create_rows.push(row.clone());
                        publish_event(redis_conn, "pump:creates", &row).await?;

                    }
                }
                PumpEvent::CollectCreatorFee(_) => {
                    if let Some(row) = PumpCreatorFeeRow::from_joined(&signature, action) {
                        fee_rows.push(row.clone());
                        publish_event(redis_conn, "pump:creator_fees", &row).await?;
                    }
                }
                PumpEvent::SetMetaplexCreator(_)
                | PumpEvent::CompletePumpAmmMigration(_)
                | PumpEvent::Complete(_) => {
                    // пока никуда не пишем
                }
            }
        }
    }

    if !trade_rows.is_empty() {
        println!("Inserting {} pump_trades rows...", trade_rows.len());
        let mut insert = ch_client.insert::<PumpTradeRow>("pump_trades").await?;
        for row in &trade_rows {
            insert.write(row).await?;
        }
        insert.end().await?;
    }

    if !create_rows.is_empty() {
        println!("Inserting {} pump_creates rows...", create_rows.len());
        let mut insert = ch_client
            .insert::<PumpCreateRow>("pump_creates")
            .await?;
        for row in &create_rows {
            insert.write(row).await?;
        }
        insert.end().await?;
    }

    if !fee_rows.is_empty() {
        println!(
            "Inserting {} pump_collect_creator_fees rows...",
            fee_rows.len()
        );
        let mut insert = ch_client
            .insert::<PumpCreatorFeeRow>("pump_collect_creator_fees")
            .await?;
        for row in &fee_rows {
            insert.write(row).await?;
        }
        insert.end().await?;
    }

    Ok(())
}

// =======================
// AMM PROCESSING
// =======================

pub async fn process_amm_transactions(
    transactions: &Vec<Transaction>,
    idl: &PumpIdl,
    ch_client: &Client,
    redis_conn: &mut MultiplexedConnection,
) -> AppResult<()> {
    let mut amm_rows: Vec<AmmTradeRow> = Vec::new();

    for (tx_idx, tx) in transactions.iter().enumerate() {
        println!("\n================ AMM TX #{tx_idx} ================");

        let Some((amm_instructions, amm_inner_instructions)) =
            collect_instructions(tx, PUMPSWAP_PROGRAM_ADDRESS)
        else {
            println!("AMM program not found in this tx, skipping");
            continue;
        };

        let signature = tx.transaction.signatures.get(0).unwrap().to_string();
        println!("Signature: {}", signature);

        // 1) инструкции
        let ix_contexts =
            instructions(tx, idl, &amm_instructions, &amm_inner_instructions, tx_idx)?;
        println!("amm ix_contexts: {:#?}", ix_contexts);

        // 2) события
        let mut amm_event_contexts: Vec<AmmEventContext> = Vec::new();

        for (log_idx, line) in tx
            .meta
            .log_messages
            .as_deref()
            .unwrap_or(&[])
            .iter()
            .enumerate()
        {
            if let Some(event) = decode_amm_event_from_log(line)? {
                amm_event_contexts.push(AmmEventContext {
                    log_index: log_idx,
                    raw_log: line.clone(),
                    event,
                });
            }
        }

        println!("amm event_contexts: {:#?}", amm_event_contexts);

        // 3) join
        let joined_actions = join_amm_ix_and_events(&ix_contexts, &amm_event_contexts);

        println!("AMM joined actions (per logical trade):");
        for action in &joined_actions {
            println!("================ AMM ACTION ================");
            println!("ix_name:   {}", action.ix.ix_name);
            println!("ix_label:  {}", action.ix.label);
            println!("ix_index:  {}", action.ix.ix_index);
            println!("is_inner:  {}", action.ix.is_inner);
            println!("Accounts instructions: {:#?}", &action.ix.accounts);

            // конвертим в ClickHouse-строку
            let row = AmmTradeRow::from_joined(&signature, action);
            amm_rows.push(row.clone());
            publish_event(redis_conn, "amm:trades", &row).await?;
        }
    }

    // 4) вставка в ClickHouse
    if !amm_rows.is_empty() {
        println!("Inserting {} amm_trades rows...", amm_rows.len());
        // как и с pump_*: без имени БД, если в DSN уже pump
        let mut insert = ch_client.insert::<AmmTradeRow>("amm_trades").await?;
        for row in &amm_rows {
            insert.write(row).await?;
        }
        insert.end().await?;
    }

    Ok(())
}

struct PumpSellAction {
    signature: String,
    ix_name: String,
    ix_index: u8,
    is_inner: bool,
    associated_bonding_curve: String,
    associated_user: String,
    bonding_curve: String,
    creator_vault: String,
    event_authority: String,
    fee_config: String,
    fee_program: String,
    fee_recipient: String,
    global: String,
    mint: String,
    program: String,
    system_program: String,
    token_program: String,
    user: String,
    timestamp: i64,
    sol_amount: u64,
    token_amount: u64,
    virtual_sol_reserves: u64,
    virtual_token_reserves: u64,
    real_sol_reserves: u64,
    real_token_reserves: u64,
    fee_basis_points: u16,
    fee: u64,
    creator: String,
    creator_fee_basis_pts: u16,
    creator_fee: u64,
    track_volume: bool,
}
struct PumpBuyAction {
    signature: String,
    ix_name: String,
    ix_index: u8,
    is_inner: bool,
    associated_bonding_curve: String,
    associated_user: String,
    bonding_curve: String,
    creator_vault: String,
    event_authority: String,
    fee_config: String,
    fee_program: String,
    fee_recipient: String,
    global: String,
    global_volume_accumulator: String,
    mint: String,
    program: String,
    system_program: String,
    token_program: String,
    user: String,
    user_volume_accumulator: String,
    timestamp: i64,
    sol_amount: u64,
    token_amount: u64,
    virtual_sol_reserves: u64,
    virtual_token_reserves: u64,
    real_sol_reserves: u64,
    real_token_reserves: u64,
    fee_basis_points: u16,
    fee: u64,
    creator: String,
    creator_fee_basis_pts: u16,
    creator_fee: u64,
    track_volume: bool,
}
struct PumpCreateAction {
    signature: String,
    ix_name: String,
    ix_index: u8,
    is_inner: bool,
    associated_bonding_curve: String,
    associated_user: String,
    bonding_curve: String,
    event_authority: String,
    global: String,
    global_params: String,
    mayhem_program_id: String,
    mayhem_state: String,
    mayhem_token_vault: String,
    mint: String,
    mint_authority: String,
    program: String,
    sol_vault: String,
    system_program: String,
    token_program: String,
    user: String,
    timestamp: i64,
    name: String,
    symbol: String,
    uri: String,
    creator: String,
    virtual_token_reserves: u64,
    virtual_sol_reserves: u64,
    real_token_reserves: u64,
    token_total_supply: u64,
    is_mayhem_mode: bool,
}
struct PumpCollectFeeAction {
    signature: String,
    ix_name: String,
    ix_index: u8,
    is_inner: bool,
    creator: String,
    creator_vault: String,
    event_authority: String,
    program: String,
    system_program: String,
    timestamp: i64,
    creator_fee: u64,
}

struct AmmBuyAction {
    signature: String,
    ix_name: String,
    ix_index: u8,
    is_inner: bool,
    associated_token_program: String,
    base_mint: String,
    base_token_program: String,
    coin_creator_vault_ata: String,
    coin_creator_vault_authority: String,
    event_authority: String,
    fee_config: String,
    fee_program: String,
    global_config: String,
    global_volume_accumulator: String,
    pool: String,
    pool_base_token_account: String,
    pool_quote_token_account: String,
    program: String,
    protocol_fee_recipient: String,
    protocol_fee_recipient_token_account: String,
    quote_mint: String,
    quote_token_program: String,
    system_program: String,
    user: String,
    user_base_token_account: String,
    user_quote_token_account: String,
    user_volume_accumulator: String,
    timestamp: i64,
    base_amount_out: u64,
    max_quote_amount_in: u64,
    user_base_token_reserves: u64,
    user_quote_token_reserves: u64,
    pool_base_token_reserves: u64,
    pool_quote_token_reserves: u64,
    quote_amount_in: u64,
    lp_fee_basis_points: u64,
    lp_fee: u64,
    protocol_fee_basis_points: u64,
    protocol_fee: u64,
    quote_amount_in_with_lp_fee: u64,
    user_quote_amount_in: u64,
    coin_creator_fee_basis_points: u64,
    coin_creator_fee: u64,
    track_volume: bool,
    min_base_amount_out: u64,
}

struct AmmSellAction {
    signature: String,
    ix_name: String,
    ix_index: u8,
    is_inner: bool,
    associated_token_program: String,
    base_mint: String,
    base_token_program: String,
    coin_creator_vault_ata: String,
    coin_creator_vault_authority: String,
    event_authority: String,
    fee_config: String,
    fee_program: String,
    global_config: String,
    pool: String,
    pool_base_token_account: String,
    pool_quote_token_account: String,
    program: String,
    protocol_fee_recipient: String,
    protocol_fee_recipient_token_account: String,
    quote_mint: String,
    quote_token_program: String,
    system_program: String,
    user: String,
    user_base_token_account: String,
    user_quote_token_account: String,
    timestamp: i64,
    base_amount_in: u64,
    min_quote_amount_out: u64,
    user_base_token_reserves: u64,
    user_quote_token_reserves: u64,
    pool_base_token_reserves: u64,
    pool_quote_token_reserves: u64,
    quote_amount_out: u64,
    lp_fee_basis_points: u64,
    lp_fee: u64,
    protocol_fee_basis_points: u64,
    protocol_fee: u64,
    quote_amount_out_without_lp_fee: u64,
    user_quote_amount_out: u64,
    coin_creator: String,
    coin_creator_fee_basis_points: u64,
    coin_creator_fee: u64,
}
