use std::fs;

mod pump_utils;
mod types;

use crate::pump_utils::{
    collect_program_instructions,
    decode_event_from_log,
    find_program_index,
    map_ix_accounts,
    match_amm_instruction,
};
use crate::types::AmmEvent;
use crate::types::block_notification::{BlockNotification, Instruction, Transaction};
use crate::types::pump_idl::PumpIdl;

const PUMPSWAP_PROGRAM_ADDRESS: &str = "pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA";
const PUMPFUN_PROGRAM_ADDRESS: &str = "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P";

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
    
    println!("{:#?}", pump_instructions);
    //process_transactions(transactions, &amm_idl);
   

    Ok(())
}

fn match_and_print(
    tx: &Transaction,
    ix: &Instruction,
    amm_idl: &PumpIdl,
    label: String,
) -> Result<(), Box<dyn std::error::Error>> {
    if let Some((idl_ix, decoded)) = match_amm_instruction(ix, amm_idl)? {
        let discriminator = &decoded[..8];
        println!("\n[{label}] Matched instruction: {}", idl_ix.name);
        println!("Signature: {:?}", tx.transaction.signatures.get(0).unwrap());
        println!("  discriminator: {:?}", discriminator);
        println!("  docs: {:?}", idl_ix.docs);

        let mapped_accounts = map_ix_accounts(tx, ix, idl_ix);
        println!("  accounts:");
        for (name, pk) in mapped_accounts {
            println!("    - {:30} => {}", name, pk);
        }
    } else {
        println!("\n[{label}] Unknown amm instruction (no match in IDL)");
    }

    Ok(())
}

fn load_idls() -> (PumpIdl, PumpIdl) {
    let amm_file_content =
        fs::read_to_string("./idl/pump_amm.json").expect("Couldn't read pump_amm.json");
    let pump_file_content =
        fs::read_to_string("./idl/pump.json").expect("Couldn't read pump.json");
    let amm_idl: PumpIdl =
        serde_json::from_str(&amm_file_content).expect("Serde JSON parse error (amm)");
    let pump_idl: PumpIdl =
        serde_json::from_str(&pump_file_content).expect("Serde JSON parse error (pump)");
    (pump_idl, amm_idl)
}

fn process_transactions(transactions: &Vec<Transaction>, amm_idl: &PumpIdl) -> Result<(), Box<dyn std::error::Error>> {
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
            match_and_print(tx, ix, &amm_idl, format!("tx #{tx_idx} outer #{idx}"))?;
        }

        // --- и ВСЕ inner-инструкции ---
        for (idx, ix) in pump_inner_instructions.iter().enumerate() {
            match_and_print(tx, ix, &amm_idl, format!("tx #{tx_idx} inner #{idx}"))?;
        }

        // --- события из логов (BuyEvent / SellEvent) ---
        for line in tx.meta.log_messages.as_deref().unwrap_or(&[]) {
            if let Some(event) = decode_event_from_log(line)? {
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