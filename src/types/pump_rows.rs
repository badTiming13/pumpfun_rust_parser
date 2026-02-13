use serde::Serialize;

use crate::prelude::PumpEvent;
use crate::utils::JoinedPumpAction;
use pump_parser_core::idl::match_ix::AccountMap;

// helpers for accounts
pub fn acc(map: &AccountMap, key: &str) -> String {
    map.get(key).cloned().unwrap_or_default()
}

pub fn acc_opt(map: &AccountMap, key: &str) -> Option<String> {
    map.get(key).cloned()
}

#[derive(Debug, Serialize, Clone)]
pub struct PumpTradeRow {
    pub slot: u64,
    pub is_success: bool,
    pub tx_error: Option<String>,
    pub signature: String,
    pub ix_name: String,
    pub ix_index: u8,
    pub is_inner: bool,
    pub is_buy: bool,

    pub associated_bonding_curve: String,
    pub associated_user: String,
    pub bonding_curve: String,
    pub creator_vault: String,
    pub event_authority: String,
    pub fee_config: String,
    pub fee_program: String,
    pub fee_recipient: String,
    pub global: String,
    pub global_volume_accumulator: Option<String>,
    pub mint: String,
    pub program: String,
    pub system_program: String,
    pub token_program: String,
    pub user: String,
    pub user_volume_accumulator: Option<String>,

    pub timestamp: i64,
    pub sol_amount: u64,
    pub token_amount: u64,
    pub virtual_sol_reserves: u64,
    pub virtual_token_reserves: u64,
    pub real_sol_reserves: u64,
    pub real_token_reserves: u64,
    pub fee_basis_points: u16,
    pub fee: u64,
    pub creator: String,
    pub creator_fee_basis_points: u16,
    pub creator_fee: u64,
    pub track_volume: bool,
}

#[derive(Debug, Serialize, Clone)]
pub struct PumpCreateRow {
    pub slot: u64,
    pub is_success: bool,
    pub tx_error: Option<String>,
    pub signature: String,
    pub ix_name: String,
    pub ix_index: u8,
    pub is_inner: bool,

    pub associated_bonding_curve: String,
    pub associated_user: String,
    pub bonding_curve: String,
    pub event_authority: String,
    pub global: String,
    pub global_params: String,
    pub mayhem_program_id: String,
    pub mayhem_state: String,
    pub mayhem_token_vault: String,
    pub mint: String,
    pub mint_authority: String,
    pub program: String,
    pub sol_vault: String,
    pub system_program: String,
    pub token_program: String,
    pub user: String,

    pub timestamp: i64,
    pub name: String,
    pub symbol: String,
    pub uri: String,
    pub creator: String,
    pub virtual_token_reserves: u64,
    pub virtual_sol_reserves: u64,
    pub real_token_reserves: u64,
    pub token_total_supply: u64,
    pub is_mayhem_mode: bool,
}

#[derive(Debug, Serialize, Clone)]
pub struct PumpAmmMigrationRow {
    pub slot: u64,
    pub is_success: bool,
    pub tx_error: Option<String>,
    pub signature: String,
    pub ix_name: String,
    pub ix_index: u8,
    pub is_inner: bool,

    pub global: String,
    pub withdraw_authority: String,
    pub mint: String,
    pub bonding_curve: String,
    pub associated_bonding_curve: String,
    pub user: String,

    pub pump_amm: String,
    pub pool: String,
    pub pool_authority: String,
    pub pool_base_token_account: String,
    pub pool_quote_token_account: String,
    pub lp_mint: String,

    pub event_authority: String,
    pub program: String,

    pub timestamp: i64,
    pub mint_amount: u64,
    pub sol_amount: u64,
    pub pool_migration_fee: u64,
}

#[derive(Debug, Serialize, Clone)]
pub struct PumpCreatorFeeRow {
    pub slot: u64,
    pub is_success: bool,
    pub tx_error: Option<String>,
    pub signature: String,
    pub ix_name: String,
    pub ix_index: u8,
    pub is_inner: bool,

    pub creator: String,
    pub creator_vault: String,
    pub event_authority: String,
    pub program: String,
    pub system_program: String,

    pub timestamp: i64,
    pub creator_fee: u64,
}

#[derive(Debug, Serialize, Clone)]
pub struct PumpMigrateIxSignal {
    pub slot: u64,
    pub signature: String,
    pub ix_index: u8,
    pub is_inner: bool,

    pub mint: String,
    pub pool: String,
    pub bonding_curve: String,
    pub associated_bonding_curve: String,
    pub user: String,

    pub pool_base_token_account: String,
    pub pool_quote_token_account: String,
}

impl PumpTradeRow {
    pub fn from_joined(
        signature: &str,
        slot: u64,
        is_success: bool,
        tx_error: Option<&str>,
        action: &JoinedPumpAction,
    ) -> Option<Self> {
        let ix = &action.ix;
        let accs = &ix.accounts;

        let PumpEvent::Trade(ev) = &action.event.event else {
            return None;
        };

        Some(Self {
            slot,
            is_success,
            tx_error: tx_error.map(|s| s.to_string()),

            signature: signature.to_string(),
            ix_name: ix.ix_name.clone(),
            ix_index: ix.ix_index as u8,
            is_inner: ix.is_inner,
            is_buy: ev.is_buy,

            associated_bonding_curve: acc(accs, "associated_bonding_curve"),
            associated_user: acc(accs, "associated_user"),
            bonding_curve: acc(accs, "bonding_curve"),
            creator_vault: acc(accs, "creator_vault"),
            event_authority: acc(accs, "event_authority"),
            fee_config: acc(accs, "fee_config"),
            fee_program: acc(accs, "fee_program"),
            fee_recipient: acc(accs, "fee_recipient"),
            global: acc(accs, "global"),
            global_volume_accumulator: acc_opt(accs, "global_volume_accumulator"),
            mint: acc(accs, "mint"),
            program: acc(accs, "program"),
            system_program: acc(accs, "system_program"),
            token_program: acc(accs, "token_program"),
            user: acc(accs, "user"),
            user_volume_accumulator: acc_opt(accs, "user_volume_accumulator"),

            timestamp: ev.timestamp,
            sol_amount: ev.sol_amount,
            token_amount: ev.token_amount,
            virtual_sol_reserves: ev.virtual_sol_reserves,
            virtual_token_reserves: ev.virtual_token_reserves,
            real_sol_reserves: ev.real_sol_reserves,
            real_token_reserves: ev.real_token_reserves,
            fee_basis_points: ev.fee_basis_points as u16,
            fee: ev.fee,
            creator: ev.creator.to_string(),
            creator_fee_basis_points: ev.creator_fee_basis_points as u16,
            creator_fee: ev.creator_fee,
            track_volume: ev.track_volume,
        })
    }
}

impl PumpCreateRow {
    pub fn from_joined(
        signature: &str,
        slot: u64,
        is_success: bool,
        tx_error: Option<&str>,
        action: &JoinedPumpAction,
    ) -> Option<Self> {
        let ix = &action.ix;
        let accs = &ix.accounts;

        let PumpEvent::Create(ev) = &action.event.event else {
            return None;
        };

        Some(Self {
            slot,
            is_success,
            tx_error: tx_error.map(|s| s.to_string()),

            signature: signature.to_string(),
            ix_name: ix.ix_name.clone(),
            ix_index: ix.ix_index as u8,
            is_inner: ix.is_inner,

            associated_bonding_curve: acc(accs, "associated_bonding_curve"),
            associated_user: acc(accs, "associated_user"),
            bonding_curve: acc(accs, "bonding_curve"),
            event_authority: acc(accs, "event_authority"),
            global: acc(accs, "global"),
            global_params: acc(accs, "global_params"),
            mayhem_program_id: acc(accs, "mayhem_program_id"),
            mayhem_state: acc(accs, "mayhem_state"),
            mayhem_token_vault: acc(accs, "mayhem_token_vault"),
            mint: acc(accs, "mint"),
            mint_authority: acc(accs, "mint_authority"),
            program: acc(accs, "program"),
            sol_vault: acc(accs, "sol_vault"),
            system_program: acc(accs, "system_program"),
            token_program: acc(accs, "token_program"),
            user: acc(accs, "user"),

            timestamp: ev.timestamp,
            name: ev.name.clone(),
            symbol: ev.symbol.clone(),
            uri: ev.uri.clone(),
            creator: ev.creator.to_string(),
            virtual_token_reserves: ev.virtual_token_reserves,
            virtual_sol_reserves: ev.virtual_sol_reserves,
            real_token_reserves: ev.real_token_reserves,
            token_total_supply: ev.token_total_supply,
            is_mayhem_mode: ev.is_mayhem_mode,
        })
    }
}

impl PumpCreatorFeeRow {
    pub fn from_joined(
        signature: &str,
        slot: u64,
        is_success: bool,
        tx_error: Option<&str>,
        action: &JoinedPumpAction,
    ) -> Option<Self> {
        let ix = &action.ix;
        let accs = &ix.accounts;

        let PumpEvent::CollectCreatorFee(ev) = &action.event.event else {
            return None;
        };

        Some(Self {
            slot,
            is_success,
            tx_error: tx_error.map(|s| s.to_string()),

            signature: signature.to_string(),
            ix_name: ix.ix_name.clone(),
            ix_index: ix.ix_index as u8,
            is_inner: ix.is_inner,

            creator: acc(accs, "creator"),
            creator_vault: acc(accs, "creator_vault"),
            event_authority: acc(accs, "event_authority"),
            program: acc(accs, "program"),
            system_program: acc(accs, "system_program"),

            timestamp: ev.timestamp,
            creator_fee: ev.creator_fee,
        })
    }
}

impl PumpAmmMigrationRow {
    pub fn from_joined(
        signature: &str,
        slot: u64,
        is_success: bool,
        tx_error: Option<&str>,
        action: &JoinedPumpAction,
    ) -> Option<Self> {
        let ix = &action.ix;
        let accs = &ix.accounts;

        let PumpEvent::CompletePumpAmmMigration(ev) = &action.event.event else {
            return None;
        };

        Some(Self {
            slot,
            is_success,
            tx_error: tx_error.map(|s| s.to_string()),

            signature: signature.to_string(),
            ix_name: ix.ix_name.clone(),
            ix_index: ix.ix_index as u8,
            is_inner: ix.is_inner,

            global: acc(accs, "global"),
            withdraw_authority: acc(accs, "withdraw_authority"),
            mint: acc(accs, "mint"),
            bonding_curve: acc(accs, "bonding_curve"),
            associated_bonding_curve: acc(accs, "associated_bonding_curve"),
            user: acc(accs, "user"),

            pump_amm: acc(accs, "pump_amm"),
            pool: acc(accs, "pool"),
            pool_authority: acc(accs, "pool_authority"),
            pool_base_token_account: acc(accs, "pool_base_token_account"),
            pool_quote_token_account: acc(accs, "pool_quote_token_account"),
            lp_mint: acc(accs, "lp_mint"),

            event_authority: acc(accs, "event_authority"),
            program: acc(accs, "program"),

            timestamp: ev.timestamp,
            mint_amount: ev.mint_amount,
            sol_amount: ev.sol_amount,
            pool_migration_fee: ev.pool_migration_fee,
        })
    }
}
