use std::fs;

mod pump_amm_utils;
mod pumpfun_utils;
mod utils;
mod types;
mod config;

use crate::pump_amm_utils::{
    decode_amm_event_from_log
};

use crate::pumpfun_utils::decode_pump_event_from_log;
use crate::types::pump_events::PumpEvent;
use crate::utils::{
    find_program_index,
    collect_program_instructions,
    map_ix_accounts,
    match_instruction,
    match_and_print,
    load_idls
};

use crate::config::constants::{
    PUMPFUN_PROGRAM_ADDRESS,
    PUMPSWAP_PROGRAM_ADDRESS
};

use crate::types::AmmEvent;
use crate::types::block_notification::{BlockNotification, Instruction, Transaction};
use crate::types::pump_idl::PumpIdl;

use chrono::{DateTime};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let (pump_idl, amm_idl) = load_idls();
    let tx_file_content = fs::read_to_string("./tx.json")?;


    // amm tx
    let block_notification: BlockNotification = serde_json::from_str(&tx_file_content)?;
    let transactions = &block_notification.params.result.value.block.transactions;


    //trying to decode pump tx
    let pump_tx_f_content = fs::read_to_string("./pumpfun_tx.json")?;
    let pump_block_notification: BlockNotification = serde_json::from_str(&pump_tx_f_content)?;
    let pump_txs = &pump_block_notification.params.result.value.block.transactions;
    
    //process_amm_transactions(transactions, &amm_idl);
    //process_pump_transactions(pump_txs, &pump_idl);
    let sol_reserves: f64 = 79760995551.0;
    let token_reserves: f64 = 403580722736262.0;
    let sol_in_lamports: f64 = 49128124.0; 
    let token: f64 = 248429187691.0;
    let mid_price = mid_price_sol(sol_reserves, token_reserves);
    let trade_price = trade_price_sol(sol_in_lamports, token);
    let timestamp: i64 = 1764528870;
    println!("Pumpfun price and mcap tests");
    println!("Mid price SOL: {}", mid_price);
    println!("Trade price SOL: {}", trade_price);
    println!("Mid mcap SOL: {},  Trade mcap SOL: {}", market_cap_sol(mid_price), market_cap_sol(trade_price));
    println!("Trade was executed at: {} (GMT)", format_timestamp_human(timestamp));
    println!("//////////////////////////////////");
    println!("Pumpswap price and mcap tests");
    
    Ok(())
}

//Functions to determine price, mcap and datetime of event for pumpfun (tested successfully), didn't test for pumpswap
fn mid_price_sol(virtual_sol_reserves: f64, virtual_token_reserves: f64) -> f64{
    (virtual_sol_reserves/virtual_token_reserves) * 1e-3
}
fn trade_price_sol(sol_amount: f64, token_amount: f64) -> f64{
    (sol_amount/token_amount) * 1e-3
}
fn market_cap_sol(price: f64) -> f64{
    price * 1_000_000_000f64
}
pub fn format_timestamp_human(ts: i64) -> String {
    let dt = DateTime::from_timestamp(ts, 0)
        .expect("invalid timestamp");

    dt.format("%Y-%m-%d %H:%M:%S").to_string()
}



fn process_pump_transactions(transactions: &Vec<Transaction>, idl: &PumpIdl) -> Result<(), Box<dyn std::error::Error>> {
 for (tx_idx, tx) in transactions.iter().enumerate() {
        println!("\n================ TX #{tx_idx} ================");

        // пытаемся найти индекс программы в этой транзакции
        let Some(pump_program_index) = find_program_index(tx, PUMPFUN_PROGRAM_ADDRESS) else {
            println!("Pump program not found in this tx, skipping");
            continue;
        };

        let (pump_instructions, pump_inner_instructions) =
            collect_program_instructions(tx, pump_program_index);

        println!("Pump program index: {}", pump_program_index);
        println!("Outer pump instructions count: {}", pump_instructions.len());
        println!(
            "Inner pump instructions count: {}",
            pump_inner_instructions.len()
        );

        // --- обрабатываем ВСЕ outer-инструкции ---
        for (idx, ix) in pump_instructions.iter().enumerate() {
            match_and_print(tx, ix, &idl, format!("tx #{tx_idx} outer #{idx}"))?;
        }

        // --- и ВСЕ inner-инструкции ---
        for (idx, ix) in pump_inner_instructions.iter().enumerate() {
            match_and_print(tx, ix, &idl, format!("tx #{tx_idx} inner #{idx}"))?;
        }

        // --- события из логов (BuyEvent / SellEvent) ---
        for line in tx.meta.log_messages.as_deref().unwrap_or(&[]) {
            if let Some(event) = decode_pump_event_from_log(line)? {
                match event {
                    PumpEvent::Trade(e) => {
                        println!("TradeEvent: {:#?}", e);
                    }
                    PumpEvent::Create(e) => {
                        println!("CreateEvent: {:#?}", e);
                    }
                    PumpEvent::SetMetaplexCreator(e) => {
                        println!("SetMetaplexCreatorEvent: {:#?}", e);
                    }
                    PumpEvent::CompletePumpAmmMigration(e) => {
                        println!("CompletePumpAmmMigrationEvent: {:#?}", e);
                    }
                    PumpEvent::Complete(e) => {
                        println!("CompleteEvent: {:#?}", e);
                    }
                    PumpEvent::CollectCreatorFee(e) => {
                        println!("CollectCreatorFeeEvent: {:#?}", e);
                    }
                }
            }
        }
    }
    Ok(())
}


fn process_amm_transactions(transactions: &Vec<Transaction>, idl: &PumpIdl) -> Result<(), Box<dyn std::error::Error>> {
 for (tx_idx, tx) in transactions.iter().enumerate() {
        println!("\n================ TX #{tx_idx} ================");

        // пытаемся найти индекс программы в этой транзакции
        let Some(pump_program_index) = find_program_index(tx, PUMPSWAP_PROGRAM_ADDRESS) else {
            println!("Pump program not found in this tx, skipping");
            continue;
        };

        let (pump_instructions, pump_inner_instructions) =
            collect_program_instructions(tx, pump_program_index);

        println!("Pump program index: {}", pump_program_index);
        println!("Outer pump instructions count: {}", pump_instructions.len());
        println!(
            "Inner pump instructions count: {}",
            pump_inner_instructions.len()
        );

        // --- обрабатываем ВСЕ outer-инструкции ---
        for (idx, ix) in pump_instructions.iter().enumerate() {
            match_and_print(tx, ix, &idl, format!("tx #{tx_idx} outer #{idx}"))?;
        }

        // --- и ВСЕ inner-инструкции ---
        for (idx, ix) in pump_inner_instructions.iter().enumerate() {
            match_and_print(tx, ix, &idl, format!("tx #{tx_idx} inner #{idx}"))?;
        }

        // --- события из логов (BuyEvent / SellEvent) ---
        for line in tx.meta.log_messages.as_deref().unwrap_or(&[]) {
            if let Some(event) = decode_amm_event_from_log(line)? {
                match event {
                    AmmEvent::Buy(e) => {
                        println!("BuyEvent: {:#?}", e);
                    }
                    AmmEvent::Sell(e) => {
                        println!("SellEvent: {:#?}", e);
                    }
                }
            }
        }
    }
    Ok(())
}