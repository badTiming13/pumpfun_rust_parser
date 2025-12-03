use std::fs;

mod pump_amm_utils;
mod pumpfun_utils;
mod utils;
mod types;
mod config;

use crate::pump_amm_utils::{
    decode_amm_event_from_log
};

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


fn main() -> Result<(), Box<dyn std::error::Error>> {
    let (pump_idl, amm_idl) = load_idls();
    let tx_file_content = fs::read_to_string("./tx.json")?;

    let block_notification: BlockNotification = serde_json::from_str(&tx_file_content)?;
    let transactions = &block_notification.params.result.value.block.transactions;

    let pump_instructions: Vec<&String> = pump_idl
        .instructions
        .iter()
        .map(|x| &x.name)
        .collect();

    let pump_events: Vec<&String> = pump_idl
        .events
        .iter()
        .map(|x| &x.name)
        .collect();
    
    println!("{:#?}", pump_events);
    
    //process_amm_transactions(transactions, &amm_idl);


    Ok(())
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