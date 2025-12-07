mod pump_amm_utils;
mod pumpfun_utils;
mod utils;
mod types;
mod config;
mod db;

use crate::db::{PumpCreateRow, PumpTradeRow, PumpCreatorFeeRow};
use crate::pump_amm_utils::decode_amm_event_from_log;
use crate::pumpfun_utils::decode_pump_event_from_log;
use crate::types::pump_events::PumpEvent;
use crate::utils::{
    collect_program_instructions, find_program_index, load_db, load_idls, match_and_print
};
use crate::config::constants::{PUMPFUN_PROGRAM_ADDRESS, PUMPSWAP_PROGRAM_ADDRESS};

use crate::types::AmmEvent;
use crate::types::block_notification::{BlockNotification, Transaction};
use crate::types::pump_idl::PumpIdl;

use futures::{StreamExt, SinkExt};
use tokio_tungstenite::{connect_async, tungstenite::protocol::Message};
use serde_json::json;

use clickhouse::{Client as ChClient, Row};
use serde::Serialize;


#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let (pump_idl, _amm_idl) = load_idls();
    let ch_client = load_db();
    
    let url = "wss://solana-mainnet.core.chainstack.com/3dec72ea492a69e1ea1fa532c2de1af7";
    let (mut ws_stream, _response) = connect_async(url).await?;
    println!("✅ Connected to {url}");

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
        .send(Message::Text(subscribe_msg.to_string()))
        .await?;
    println!("📨 Sent blockSubscribe (pumpfun)");

    while let Some(msg) = ws_stream.next().await {
        let msg = msg?;

        match msg {
            Message::Text(text) => {
                match serde_json::from_str::<BlockNotification>(&text) {
                    Ok(block_notification) => {
                        let value = &block_notification.params.result.value;
                        let block = &value.block;

                        println!(
                            "\n🔔 New block: slot={}, height={}, txs={}",
                            value.slot,
                            block.block_height,
                            block.transactions.len()
                        );

                        let txs = &block.transactions;
                        if let Err(e) = process_pump_transactions(
                            value.slot,
                            block.block_height,
                            txs,
                            &pump_idl,
                            &ch_client,
                        )
                        .await
                        {
                            eprintln!("❌ Error in process_pump_transactions: {e:?}");
                        }
                    }
                    Err(e) => {
                        eprintln!("(non-block or parse error in BlockNotification): {e:?}");
                        // eprintln!("RAW JSON: {text}");
                    }
                }
            }
            Message::Binary(_bin) => {}
            Message::Ping(p) => {
                ws_stream.send(Message::Pong(p)).await?;
            }
            Message::Pong(_) => {}
            Message::Close(frame) => {
                println!("🔌 WebSocket closed: {:?}", frame);
                break;
            }
            _ => {}
        }
    }

    Ok(())
}

// =======================
// INSERT INTO CLICKHOUSE
// =======================

async fn insert_pump_event(
    client: &ChClient,
    signature_opt: Option<&str>,
    slot: u64,
    block_height: u64,
    tx_index_in_block: u16,
    log_index: u16,
    event: &PumpEvent,
) -> anyhow::Result<()> {
    let signature = signature_opt.unwrap_or("UNKNOWN").to_string();

    match event {
        PumpEvent::Trade(e) => {
            let row = PumpTradeRow {
                slot,
                block_height,
                tx_index_in_block,
                log_index,
                signature,

                mint: e.mint.to_string(),
                // пока не мапим аккаунты из инструкций
                bonding_curve: String::new(),
                associated_bonding_curve: String::new(),
                global: String::new(),
                associated_user: String::new(),
                user: e.user.to_string(),
                fee_recipient: e.fee_recipient.to_string(),
                creator: e.creator.to_string(),
                creator_vault: String::new(),
                token_program: String::new(),
                system_program: String::new(),
                event_authority: String::new(),
                program: String::new(),
                global_volume_accumulator: String::new(),
                user_volume_accumulator: String::new(),
                fee_config: String::new(),
                fee_program: String::new(),

                event_timestamp: e.timestamp,
                ix_name: e.ix_name.clone(),
                is_buy: if e.is_buy { 1 } else { 0 },

                sol_amount: e.sol_amount,
                token_amount: e.token_amount,
                virtual_sol_reserves: e.virtual_sol_reserves,
                virtual_token_reserves: e.virtual_token_reserves,
                real_sol_reserves: e.real_sol_reserves,
                real_token_reserves: e.real_token_reserves,

                // В таблице UInt16, поэтому кастим
                fee_basis_points: e.fee_basis_points as u16,
                fee: e.fee,
                creator_fee_basis_points: e.creator_fee_basis_points as u16,
                creator_fee: e.creator_fee,

                track_volume: if e.track_volume { 1 } else { 0 },
                total_unclaimed_tokens: e.total_unclaimed_tokens,
                total_claimed_tokens: e.total_claimed_tokens,
                current_sol_volume: e.current_sol_volume,
                last_update_timestamp: e.last_update_timestamp,
            };

            let mut insert = client.insert::<PumpTradeRow>("pump_trades").await?;
            insert.write(&row).await?;
            insert.end().await?;
        }

        PumpEvent::Create(e) => {
            let row = PumpCreateRow {
                slot,
                block_height,
                tx_index_in_block,
                log_index,
                signature,

                event_timestamp: e.timestamp,

                name: e.name.clone(),
                symbol: e.symbol.clone(),
                uri: e.uri.clone(),

                mint: e.mint.to_string(),
                bonding_curve: e.bonding_curve.to_string(),
                user: e.user.to_string(),
                creator: e.creator.to_string(),

                virtual_token_reserves: e.virtual_token_reserves,
                virtual_sol_reserves: e.virtual_sol_reserves,
                real_token_reserves: e.real_token_reserves,
                token_total_supply: e.token_total_supply,

                token_program: e.token_program.to_string(),
                is_mayhem_mode: if e.is_mayhem_mode { 1 } else { 0 },

                // пока не мапим остальные аккаунты
                mint_authority: String::new(),
                associated_bonding_curve: String::new(),
                global: String::new(),
                system_program: String::new(),
                associated_token_program: String::new(),
                mayhem_program_id: String::new(),
                global_params: String::new(),
                sol_vault: String::new(),
                mayhem_state: String::new(),
                mayhem_token_vault: String::new(),
                event_authority: String::new(),
                program: String::new(),
            };

            let mut insert = client.insert::<PumpCreateRow>("pump_creates").await?;
            insert.write(&row).await?;
            insert.end().await?;
        }

        PumpEvent::CollectCreatorFee(e) => {
            let row = PumpCreatorFeeRow {
                slot,
                block_height,
                tx_index_in_block,
                log_index,
                signature,

                event_timestamp: e.timestamp,
                creator: e.creator.to_string(),
                creator_fee: e.creator_fee,

                ix_creator: String::new(),
                creator_vault: String::new(),
                system_program: String::new(),
                event_authority: String::new(),
                program: String::new(),
            };

            let mut insert =
                client.insert::<PumpCreatorFeeRow>("pump_creator_fees").await?;
            insert.write(&row).await?;
            insert.end().await?;
        }

        // Остальные пока только логируем
        _ => {}
    }

    Ok(())
}

// =======================
// PUMPFUN PROCESSING
// =======================

async fn process_pump_transactions(
    slot: u64,
    block_height: u64,
    transactions: &Vec<Transaction>,
    idl: &PumpIdl,
    ch_client: &ChClient,
) -> Result<(), Box<dyn std::error::Error>> {
    for (tx_idx, tx) in transactions.iter().enumerate() {
        println!("\n================ PUMPFUN TX #{tx_idx} ================");
        let sig_opt = tx.transaction.signatures.get(0);
        println!("Signature: {:?}", sig_opt);

        let Some(pump_program_index) = find_program_index(tx, PUMPFUN_PROGRAM_ADDRESS) else {
            println!("Pumpfun program not found in this tx, skipping");
            continue;
        };

        let (pump_instructions, pump_inner_instructions) =
            collect_program_instructions(tx, pump_program_index);

        println!("Pumpfun program index: {}", pump_program_index);
        println!("Outer pump instructions count: {}", pump_instructions.len());
        println!(
            "Inner pump instructions count: {}",
            pump_inner_instructions.len()
        );

        for (idx, ix) in pump_instructions.iter().enumerate() {
            match_and_print(tx, ix, &idl, format!("tx #{tx_idx} outer #{idx}"))?;
        }

        for (idx, ix) in pump_inner_instructions.iter().enumerate() {
            match_and_print(tx, ix, &idl, format!("tx #{tx_idx} inner #{idx}"))?;
        }

        let logs = tx.meta.log_messages.as_deref().unwrap_or(&[]);
        let sig_str_opt = sig_opt.map(|s| s.as_str());

        for (log_idx, line) in logs.iter().enumerate() {
            match decode_pump_event_from_log(line) {
                Ok(Some(event)) => {
                    match &event {
                        PumpEvent::Trade(e) => {
                            println!("TradeEvent (log #{log_idx}): {:#?}", e);
                        }
                        PumpEvent::Create(e) => {
                            println!("CreateEvent (log #{log_idx}): {:#?}", e);
                        }
                        PumpEvent::SetMetaplexCreator(e) => {
                            println!("SetMetaplexCreatorEvent (log #{log_idx}): {:#?}", e);
                        }
                        PumpEvent::CompletePumpAmmMigration(e) => {
                            println!("CompletePumpAmmMigrationEvent (log #{log_idx}): {:#?}", e);
                        }
                        PumpEvent::Complete(e) => {
                            println!("CompleteEvent (log #{log_idx}): {:#?}", e);
                        }
                        PumpEvent::CollectCreatorFee(e) => {
                            println!("CollectCreatorFeeEvent (log #{log_idx}): {:#?}", e);
                        }
                    }

                    if let Err(db_err) = insert_pump_event(
                        ch_client,
                        sig_str_opt,
                        slot,
                        block_height,
                        tx_idx as u16,
                        log_idx as u16,
                        &event,
                    )
                    .await
                    {
                        eprintln!(
                            "❌ Failed to insert pumpfun event into ClickHouse, tx={:?}, log_idx={}: {db_err:?}",
                            sig_opt,
                            log_idx
                        );
                    }
                }
                Ok(None) => {}
                Err(e) => {
                    eprintln!(
                        "❌ Pumpfun event decode error in tx {:?}, log #{log_idx}: {e}",
                        sig_opt
                    );
                    eprintln!("   Raw log line: {line}");
                }
            }
        }
    }
    Ok(())
}

// =======================
// AMM (оставляем как было)
// =======================

fn process_amm_transactions(
    transactions: &Vec<Transaction>,
    idl: &PumpIdl,
) -> Result<(), Box<dyn std::error::Error>> {
    for (tx_idx, tx) in transactions.iter().enumerate() {
        println!("\n================ AMM TX #{tx_idx} ================");
        println!("Signature: {:?}", tx.transaction.signatures.get(0));

        let Some(pump_program_index) = find_program_index(tx, PUMPSWAP_PROGRAM_ADDRESS) else {
            println!("AMM program not found in this tx, skipping");
            continue;
        };

        let (pump_instructions, pump_inner_instructions) =
            collect_program_instructions(tx, pump_program_index);

        println!("AMM program index: {}", pump_program_index);
        println!("Outer pump instructions count: {}", pump_instructions.len());
        println!(
            "Inner pump instructions count: {}",
            pump_inner_instructions.len()
        );

        for (idx, ix) in pump_instructions.iter().enumerate() {
            match_and_print(tx, ix, &idl, format!("tx #{tx_idx} outer #{idx}"))?;
        }

        for (idx, ix) in pump_inner_instructions.iter().enumerate() {
            match_and_print(tx, ix, &idl, format!("tx #{tx_idx} inner #{idx}"))?;
        }

        for line in tx.meta.log_messages.as_deref().unwrap_or(&[]) {
            if let Some(event) = decode_amm_event_from_log(line)? {
                match event {
                    AmmEvent::Buy(e) => println!("BuyEvent: {:#?}", e),
                    AmmEvent::Sell(e) => println!("SellEvent: {:#?}", e),
                }
            }
        }
    }
    Ok(())
}
