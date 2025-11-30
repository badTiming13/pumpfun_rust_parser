use base64::{engine::general_purpose::STANDARD, Engine as _};
use borsh::BorshDeserialize;
use bs58;

use crate::types::block_notification::{Instruction, Transaction};
use crate::types::pump_idl::{Instruction as IdlInstruction, PumpIdl};
use crate::types::{AmmEvent, BuyEvent, SellEvent};

//Pumpswap Events
const BUY_EVENT_DISCRIMINATOR: [u8; 8] = [103, 244, 82, 31, 44, 245, 119, 119];
const SELL_EVENT_DISCRIMINATOR: [u8; 8] = [62, 47, 55, 10, 165, 3, 220, 42];

//Pumpfun Events
const TRADE_EVENT_DISCRIMINATOR: [u8; 8] = [189, 219, 127, 211, 78, 230, 97, 238];
const CREATE_EVENT_DISCRIMINATOR: [u8; 8] = [27, 114, 169, 77, 222, 235, 99, 118];
const SETMETAPLEX_CREATOR_EVENT_DISCRIMINATOR: [u8; 8] = [142, 203, 6, 32, 127, 105, 191, 162];
const COMPLETE_PUMPAMM_MIGRATION_EVENT_DISCRIMINATOR: [u8; 8] = [189, 233, 93, 185, 92, 148, 234, 148];
const COMPLETE_EVENT_DISCRIMINATOR: [u8; 8] = [95, 114, 97, 156, 212, 46, 152, 8];
const COLLECT_CREATORFEE_EVENT_DISCRIMINATOR: [u8; 8] = [122, 2, 127, 1, 14, 191, 12, 175];


/// Collect all accounts: accountKeys + loaded writable + loaded readonly
pub fn get_accounts(tx: &Transaction) -> Vec<&String> {
    let account_keys = &tx.transaction.message.account_keys;
    let loaded_w = &tx.meta.loaded_addresses.writable;
    let loaded_r = &tx.meta.loaded_addresses.readonly;

    account_keys
        .iter()
        .chain(loaded_w.iter())
        .chain(loaded_r.iter())
        .collect()
}

/// find index of program address in allAccounts
pub fn find_program_index(tx: &Transaction, program_address: &str) -> Option<u8> {
    let accounts = get_accounts(tx);

    let pos = accounts
        .iter()
        .position(|address| address.as_str() == program_address)?;

    Some(u8::try_from(pos).expect("account index does not fit into u8"))
}

/// Возвращаем:
/// - все обычные инструкции этой программы
/// - все inner-инструкции этой программы
pub fn collect_program_instructions<'a>(
    tx: &'a Transaction,
    program_index: u8,
) -> (Vec<&'a Instruction>, Vec<&'a Instruction>) {
    let outer: Vec<&Instruction> = tx
        .transaction
        .message
        .instructions
        .iter()
        .filter(|ix| ix.program_id_index == program_index)
        .collect();

    let inner: Vec<&Instruction> = tx
        .meta
        .inner_instructions
        .as_ref()
        .map(|groups| {
            groups
                .iter()
                .flat_map(|group| group.instructions.iter())
                .filter(|ix| ix.program_id_index == program_index)
                .collect()
        })
        .unwrap_or_default();

    (outer, inner)
}

/// Декодирует data, достаёт дискриминатор и ищет соответствующую инструкцию в amm IDL
pub fn match_amm_instruction<'a>(
    ix: &Instruction,
    amm_idl: &'a PumpIdl,
) -> Result<Option<(&'a IdlInstruction, Vec<u8>)>, bs58::decode::Error> {
    let decoded = bs58::decode(&ix.data).into_vec()?;

    if decoded.len() < 8 {
        return Ok(None);
    }

    let discriminator = &decoded[..8];

    let maybe_ix = amm_idl
        .instructions
        .iter()
        .find(|idl_ix| idl_ix.discriminator.as_slice() == discriminator);

    Ok(maybe_ix.map(|idl_ix| (idl_ix, decoded)))
}

/// Декодим BuyEvent / SellEvent из логов (`Program data: ...`)
pub fn decode_event_from_log(line: &str) -> Result<Option<AmmEvent>, Box<dyn std::error::Error>> {
    let prefix = "Program data: ";
    let base64_part = match line.strip_prefix(prefix) {
        Some(s) => s.trim(),
        None => return Ok(None),
    };

    let bytes = STANDARD.decode(base64_part)?;

    if bytes.len() < 8 {
        return Ok(None);
    }

    let (disc, payload) = bytes.split_at(8);

    if disc == BUY_EVENT_DISCRIMINATOR {
        let event = BuyEvent::try_from_slice(payload)?;
        Ok(Some(AmmEvent::Buy(event)))
    } else if disc == SELL_EVENT_DISCRIMINATOR {
        let event = SellEvent::try_from_slice(payload)?;
        Ok(Some(AmmEvent::Sell(event)))
    } else {
        Ok(None)
    }
}

/// Маппинг аккаунтов инструкции:
/// IDL-имя аккаунта → реальный pubkey из транзакции
///
/// Возвращаем владящие строки, чтобы не играться с лайфтаймами `&str` из Value.
pub fn map_ix_accounts(
    tx: &Transaction,
    ix: &Instruction,
    idl_ix: &IdlInstruction,
) -> Vec<(String, String)> {
    let all_accounts = get_accounts(tx);
    let mut mapped = Vec::new();

    // В IDL аккаунты идут в том же порядке, что и в ix.accounts
    for (i, idl_acc) in idl_ix.accounts.iter().enumerate() {
        // индекс аккаунта в message.accountKeys / loaded addresses
        let Some(&account_idx) = ix.accounts.get(i) else {
            continue;
        };
        let idx = account_idx as usize;

        if let Some(pk) = all_accounts.get(idx) {
            // accounts[i] — это serde_json::Value
            let name = idl_acc
                .get("name")
                .and_then(|v| v.as_str())
                .unwrap_or("<unknown>")
                .to_string();

            mapped.push((name, (*pk).clone()));
        }
    }

    mapped
}
