use std::{fs, time::Duration};
use std::error::Error;
use std::sync::atomic::{AtomicU64, Ordering};

mod config;
mod db;
mod prelude;
mod pump_amm_utils;
mod pumpfun_utils;
mod types;
mod utils;

type AppResult<T> = Result<T, Box<dyn Error>>;

use clickhouse::Client;
use futures::{SinkExt, StreamExt};
use serde_json::json;
use tokio_tungstenite::connect_async;

use redis::Client as RedisClient;
use redis::aio::MultiplexedConnection;

use tracing::{debug, error, info, warn};
use tracing_subscriber::EnvFilter;

use crate::utils::{publish_event, publish_event_stream, lag_ms_from_chain_ts};
use crate::{
    db::{PumpCreateRow, PumpCreatorFeeRow, PumpTradeRow, amm_types::AmmTradeRow},
    prelude::*,
    utils::{
        AmmEventContext, EventContext, InstructionContext, collect_instructions, instructions,
        join_amm_ix_and_events, join_pump_ix_and_events, load_db, match_and_collect,
        match_and_print,
    },
};

// как часто логировать lag (каждые N событий)
const LOG_EVERY: u64 = 2000;

// счётчики событий (чтобы не спамить)
static PUMP_TRADES_CNT: AtomicU64 = AtomicU64::new(0);
static PUMP_CREATES_CNT: AtomicU64 = AtomicU64::new(0);
static PUMP_FEES_CNT: AtomicU64 = AtomicU64::new(0);
static AMM_TRADES_CNT: AtomicU64 = AtomicU64::new(0);

#[tokio::main]
async fn main() -> AppResult<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env().add_directive("info".parse().unwrap()))
        .init();

    let (pump_idl, amm_idl) = load_idls();
    let ch_client = load_db();

    let redis_client = RedisClient::open("redis://127.0.0.1/")?;

    // ClickHouse client можно клонировать – внутри он cheap-clone
    let pump_ch = ch_client.clone();
    let amm_ch = ch_client.clone();

    let pump_idl_clone = pump_idl.clone();
    let amm_idl_clone = amm_idl.clone();

    // Отдельный коннект под Pumpfun
    let pump_redis_conn = redis_client.get_multiplexed_async_connection().await?;
    // Отдельный коннект под AMM
    let amm_redis_conn = redis_client.get_multiplexed_async_connection().await?;

    let url = "wss://solana-mainnet.core.chainstack.com/3dec72ea492a69e1ea1fa532c2de1af7";

    let pump_task = tokio::spawn(async move {
        if let Err(e) = run_pumpfun_stream(url, pump_idl_clone, &pump_ch, pump_redis_conn).await {
            error!(error=%e, "Pumpfun stream task crashed");
        }
    });

    let amm_task = tokio::spawn(async move {
        if let Err(e) = run_amm_stream(url, amm_idl_clone, &amm_ch, amm_redis_conn).await {
            error!(error=%e, "AMM stream task crashed");
        }
    });

    let _ = tokio::join!(pump_task, amm_task);
    Ok(())
}

async fn run_pumpfun_stream(
    url: &str,
    idl: PumpIdl,
    _ch_client: &Client,
    mut redis_conn: MultiplexedConnection,
) -> AppResult<()> {
    loop {
        info!("🔌 Connecting Pumpfun stream...");

        let (mut ws_stream, _response) = connect_async(url).await?;
        info!(url=%url, "✅ Pumpfun connected");

        let subscribe_msg = json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "blockSubscribe",
            "params": [
                { "mentionsAccountOrProgram": PUMPFUN_PROGRAM_ADDRESS },
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
            .send(tokio_tungstenite::tungstenite::Message::Text(subscribe_msg.to_string()))
            .await?;

        info!("📨 Pumpfun subscription sent");

        while let Some(msg) = ws_stream.next().await {
            match msg {
                Ok(tokio_tungstenite::tungstenite::Message::Text(text)) => {
                    let parsed = serde_json::from_str::<BlockNotification>(&text);
                    let Ok(notification) = parsed else {
                        continue;
                    };

                    let txs = &notification.params.result.value.block.transactions;
                    let slot: u64 = notification.params.result.value.slot;

                    if txs.is_empty() {
                        continue;
                    }

                    debug!(slot=%slot, txs_len=txs.len(), "Pumpfun block received");

                    if let Err(e) =
                        process_pump_transactions(txs, slot, &idl, &mut redis_conn).await
                    {
                        error!(slot=%slot, error=%e, "Pumpfun process error");
                    }
                }

                Ok(tokio_tungstenite::tungstenite::Message::Binary(_)) => continue,

                Ok(tokio_tungstenite::tungstenite::Message::Ping(p)) => {
                    ws_stream
                        .send(tokio_tungstenite::tungstenite::Message::Pong(p))
                        .await?;
                }

                Ok(tokio_tungstenite::tungstenite::Message::Close(frame)) => {
                    warn!(?frame, "Pumpfun WS closed");
                    break;
                }

                Err(e) => {
                    warn!(error=%e, "Pumpfun WS error");
                    break;
                }

                _ => {}
            }
        }

        info!("🔁 Pumpfun reconnect in 3s...");
        tokio::time::sleep(Duration::from_secs(3)).await;
    }
}

async fn run_amm_stream(
    url: &str,
    idl: PumpIdl,
    _ch_client: &Client,
    mut redis_conn: MultiplexedConnection,
) -> AppResult<()> {
    loop {
        info!("🔌 Connecting AMM stream...");

        let (mut ws_stream, _response) = connect_async(url).await?;
        info!(url=%url, "✅ AMM connected");

        let subscribe_msg = json!({
            "jsonrpc": "2.0",
            "id": 2,
            "method": "blockSubscribe",
            "params": [
                { "mentionsAccountOrProgram": PUMPSWAP_PROGRAM_ADDRESS },
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
            .send(tokio_tungstenite::tungstenite::Message::Text(subscribe_msg.to_string()))
            .await?;

        info!("📨 AMM subscription sent");

        while let Some(msg) = ws_stream.next().await {
            match msg {
                Ok(tokio_tungstenite::tungstenite::Message::Text(text)) => {
                    let parsed = serde_json::from_str::<BlockNotification>(&text);
                    let Ok(notification) = parsed else {
                        continue;
                    };

                    let txs = &notification.params.result.value.block.transactions;
                    let slot: u64 = notification.params.result.value.slot;

                    if txs.is_empty() {
                        continue;
                    }

                    debug!(slot=%slot, txs_len=txs.len(), "AMM block received");

                    if let Err(e) =
                        process_amm_transactions(txs, slot, &idl, &mut redis_conn).await
                    {
                        error!(slot=%slot, error=%e, "AMM process error");
                    }
                }

                Ok(tokio_tungstenite::tungstenite::Message::Ping(p)) => {
                    ws_stream
                        .send(tokio_tungstenite::tungstenite::Message::Pong(p))
                        .await?;
                }

                Ok(tokio_tungstenite::tungstenite::Message::Close(frame)) => {
                    warn!(?frame, "AMM WS closed");
                    break;
                }

                Err(e) => {
                    warn!(error=%e, "AMM WS error");
                    break;
                }

                _ => {}
            }
        }

        info!("🔁 AMM reconnect in 3s...");
        tokio::time::sleep(Duration::from_secs(3)).await;
    }
}

// =======================
// PUMPFUN PROCESSING
// =======================

async fn process_pump_transactions(
    transactions: &Vec<Transaction>,
    slot: u64,
    idl: &PumpIdl,
    redis_conn: &mut MultiplexedConnection,
) -> AppResult<()> {
    for (tx_idx, tx) in transactions.iter().enumerate() {
        debug!(slot=%slot, tx_idx=tx_idx, "Pumpfun tx");

        let Some((pump_instructions, pump_inner_instructions)) =
            collect_instructions(tx, PUMPFUN_PROGRAM_ADDRESS)
        else {
            debug!("Pumpfun program not found in this tx, skipping");
            continue;
        };

        let signature = tx.transaction.signatures.get(0).unwrap().to_string();

        let is_success = tx.meta.err.is_none();
        let tx_error: Option<String> = tx
            .meta
            .err
            .as_ref()
            .map(|e| serde_json::to_string(e).unwrap_or_else(|_| format!("{:?}", e)));

        debug!(
            slot=%slot,
            signature=%signature,
            success=is_success,
            tx_error=?tx.meta.err,
            "Pumpfun tx meta"
        );

        let ix_contexts = instructions(
            tx,
            idl,
            &pump_instructions,
            &pump_inner_instructions,
            tx_idx,
        )?;
        debug!("pump ix_contexts: {:#?}", ix_contexts);

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
        debug!("pump event_contexts: {:#?}", event_contexts);

        let joined_actions = join_pump_ix_and_events(&ix_contexts, &event_contexts);
        debug!(joined_len=joined_actions.len(), "Pumpfun joined actions");

        for action in &joined_actions {
            debug!(
                ix_name=%action.ix.ix_name,
                ix_index=action.ix.ix_index,
                is_inner=action.ix.is_inner,
                "Pumpfun action"
            );

            match action.event.event {
                PumpEvent::Trade(_) => {
                    if let Some(row) = PumpTradeRow::from_joined(
                        &signature,
                        slot,
                        is_success,
                        tx_error.as_deref(),
                        action,
                    ) {
                        // 1) PubSub для бота (как раньше)
                        publish_event(redis_conn, "pump:trades", &row).await?;

                        // 2) Stream для базы
                        publish_event_stream(redis_conn, "db:pump:trades", &row).await?;

                        // lag-лог (редко)
                        let n = PUMP_TRADES_CNT.fetch_add(1, Ordering::Relaxed) + 1;
                        if n % LOG_EVERY == 0 {
                            let lag_ms = lag_ms_from_chain_ts(row.timestamp);
                            info!(channel="pump", kind="trade", every=LOG_EVERY, count=n, lag_ms=lag_ms, "lag");
                        }
                    }
                }

                PumpEvent::Create(_) => {
                    if let Some(row) = PumpCreateRow::from_joined(
                        &signature,
                        slot,
                        is_success,
                        tx_error.as_deref(),
                        action,
                    ) {
                        publish_event(redis_conn, "pump:creates", &row).await?;
                        publish_event_stream(redis_conn, "db:pump:creates", &row).await?;

                        let n = PUMP_CREATES_CNT.fetch_add(1, Ordering::Relaxed) + 1;
                        if n % LOG_EVERY == 0 {
                            let lag_ms = lag_ms_from_chain_ts(row.timestamp);
                            info!(channel="pump", kind="create", every=LOG_EVERY, count=n, lag_ms=lag_ms, "lag");
                        }
                    }
                }

                PumpEvent::CollectCreatorFee(_) => {
                    if let Some(row) = PumpCreatorFeeRow::from_joined(
                        &signature,
                        slot,
                        is_success,
                        tx_error.as_deref(),
                        action,
                    ) {
                        publish_event(redis_conn, "pump:creator_fees", &row).await?;
                        publish_event_stream(redis_conn, "db:pump:creator_fees", &row).await?;

                        let n = PUMP_FEES_CNT.fetch_add(1, Ordering::Relaxed) + 1;
                        if n % LOG_EVERY == 0 {
                            let lag_ms = lag_ms_from_chain_ts(row.timestamp);
                            info!(channel="pump", kind="creator_fee", every=LOG_EVERY, count=n, lag_ms=lag_ms, "lag");
                        }
                    }
                }

                PumpEvent::SetMetaplexCreator(_)
                | PumpEvent::CompletePumpAmmMigration(_)
                | PumpEvent::Complete(_) => {}
            }
        }
    }

    Ok(())
}

// =======================
// AMM PROCESSING
// =======================

pub async fn process_amm_transactions(
    transactions: &Vec<Transaction>,
    slot: u64,
    idl: &PumpIdl,
    redis_conn: &mut MultiplexedConnection,
) -> AppResult<()> {
    for (tx_idx, tx) in transactions.iter().enumerate() {
        debug!(slot=%slot, tx_idx=tx_idx, "AMM tx");

        let Some((amm_instructions, amm_inner_instructions)) =
            collect_instructions(tx, PUMPSWAP_PROGRAM_ADDRESS)
        else {
            debug!("AMM program not found in this tx, skipping");
            continue;
        };

        let signature = tx.transaction.signatures.get(0).unwrap().to_string();

        let is_success = tx.meta.err.is_none();
        let tx_error: Option<String> = tx
            .meta
            .err
            .as_ref()
            .map(|e| serde_json::to_string(e).unwrap_or_else(|_| format!("{:?}", e)));

        debug!(
            slot=%slot,
            signature=%signature,
            success=is_success,
            tx_error=?tx.meta.err,
            "AMM tx meta"
        );

        let ix_contexts =
            instructions(tx, idl, &amm_instructions, &amm_inner_instructions, tx_idx)?;
        debug!("amm ix_contexts: {:#?}", ix_contexts);

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
        debug!("amm event_contexts: {:#?}", amm_event_contexts);

        let joined_actions = join_amm_ix_and_events(&ix_contexts, &amm_event_contexts);
        debug!(joined_len=joined_actions.len(), "AMM joined actions");

        for action in &joined_actions {
            debug!(
                ix_name=%action.ix.ix_name,
                ix_index=action.ix.ix_index,
                is_inner=action.ix.is_inner,
                "AMM action"
            );

            let row =
                AmmTradeRow::from_joined(&signature, slot, is_success, tx_error.as_deref(), action);

            publish_event(redis_conn, "amm:trades", &row).await?;
            publish_event_stream(redis_conn, "db:amm:trades", &row).await?;

            let n = AMM_TRADES_CNT.fetch_add(1, Ordering::Relaxed) + 1;
            if n % LOG_EVERY == 0 {
                let lag_ms = lag_ms_from_chain_ts(row.timestamp);
                info!(channel="amm", kind="trade", every=LOG_EVERY, count=n, lag_ms=lag_ms, "lag");
            }
        }
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
