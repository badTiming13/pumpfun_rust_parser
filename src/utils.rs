use base64::{engine::general_purpose::STANDARD, Engine as _};
use borsh::BorshDeserialize;
use bs58;

use crate::types::block_notification::{Instruction, Transaction};
use crate::types::pump_idl::{Instruction as IdlInstruction, PumpIdl};

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
