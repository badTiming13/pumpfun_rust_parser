use serde::Serialize;

use crate::types::AmmEvent;
use crate::utils::JoinedAmmAction;

use super::pump_rows::{acc, acc_opt};

#[derive(Debug, Serialize, Clone)]
pub struct AmmTradeRow {
    pub slot: u64,
    pub is_success: bool,
    pub tx_error: Option<String>,
    pub signature: String,
    pub ix_name: String,
    pub ix_index: u8,
    pub is_inner: bool,
    pub is_buy: bool,
    pub associated_token_program: String,
    pub base_mint: String,
    pub base_token_program: String,
    pub coin_creator_vault_ata: String,
    pub coin_creator_vault_authority: String,
    pub event_authority: String,
    pub fee_config: String,
    pub fee_program: String,
    pub global_config: String,
    pub global_volume_accumulator: Option<String>,
    pub pool: String,
    pub pool_base_token_account: String,
    pub pool_quote_token_account: String,
    pub program: String,
    pub protocol_fee_recipient: String,
    pub protocol_fee_recipient_token_account: String,
    pub quote_mint: String,
    pub quote_token_program: String,
    pub system_program: String,
    pub user: String,
    pub user_base_token_account: String,
    pub user_quote_token_account: String,
    pub user_volume_accumulator: Option<String>,

    pub timestamp: i64,
    pub base_amount_in: u64,
    pub base_amount_out: u64,
    pub min_quote_amount_out: u64,
    pub max_quote_amount_in: u64,
    pub user_base_token_reserves: u64,
    pub user_quote_token_reserves: u64,
    pub pool_base_token_reserves: u64,
    pub pool_quote_token_reserves: u64,
    pub quote_amount_in: u64,
    pub quote_amount_out: u64,
    pub lp_fee_basis_points: u64,
    pub lp_fee: u64,
    pub protocol_fee_basis_points: u64,
    pub protocol_fee: u64,
    pub quote_amount_in_with_lp_fee: u64,
    pub quote_amount_out_without_lp_fee: u64,
    pub user_quote_amount_in: u64,
    pub user_quote_amount_out: u64,
    pub coin_creator: String,
    pub coin_creator_fee_basis_points: u64,
    pub coin_creator_fee: u64,
    pub track_volume: bool,
    pub min_base_amount_out: u64,
}

const SOL_MINT: &str = "So11111111111111111111111111111111111111112";

impl AmmTradeRow {
    pub fn from_joined(
        signature: &str,
        slot: u64,
        is_success: bool,
        tx_error: Option<&str>,
        action: &JoinedAmmAction,
    ) -> Option<Self> {
        let ix = &action.ix;
        let accs = &ix.accounts;

        // keep only token/SOL
        let base_mint = acc(accs, "base_mint");
        let quote_mint = acc(accs, "quote_mint");
        if quote_mint != SOL_MINT || base_mint == SOL_MINT {
            return None;
        }

        let tx_error = tx_error.map(|s| s.to_string());

        match &action.event.event {
            AmmEvent::Sell(ev) => Some(Self {
                slot,
                is_success,
                tx_error: tx_error.clone(),

                signature: signature.to_string(),
                ix_name: ix.ix_name.clone(),
                ix_index: ix.ix_index as u8,
                is_inner: ix.is_inner,
                is_buy: false,

                associated_token_program: acc(accs, "associated_token_program"),
                base_mint,
                base_token_program: acc(accs, "base_token_program"),
                coin_creator_vault_ata: acc(accs, "coin_creator_vault_ata"),
                coin_creator_vault_authority: acc(accs, "coin_creator_vault_authority"),
                event_authority: acc(accs, "event_authority"),
                fee_config: acc(accs, "fee_config"),
                fee_program: acc(accs, "fee_program"),
                global_config: acc(accs, "global_config"),
                global_volume_accumulator: None,
                pool: acc(accs, "pool"),
                pool_base_token_account: acc(accs, "pool_base_token_account"),
                pool_quote_token_account: acc(accs, "pool_quote_token_account"),
                program: acc(accs, "program"),
                protocol_fee_recipient: acc(accs, "protocol_fee_recipient"),
                protocol_fee_recipient_token_account: acc(accs, "protocol_fee_recipient_token_account"),
                quote_mint,
                quote_token_program: acc(accs, "quote_token_program"),
                system_program: acc(accs, "system_program"),
                user: acc(accs, "user"),
                user_base_token_account: acc(accs, "user_base_token_account"),
                user_quote_token_account: acc(accs, "user_quote_token_account"),
                user_volume_accumulator: None,

                timestamp: ev.timestamp,

                base_amount_in: ev.base_amount_in,
                base_amount_out: 0,

                min_quote_amount_out: ev.min_quote_amount_out,
                max_quote_amount_in: 0,

                user_base_token_reserves: ev.user_base_token_reserves,
                user_quote_token_reserves: ev.user_quote_token_reserves,
                pool_base_token_reserves: ev.pool_base_token_reserves,
                pool_quote_token_reserves: ev.pool_quote_token_reserves,

                quote_amount_in: 0,
                quote_amount_out: ev.quote_amount_out,

                lp_fee_basis_points: ev.lp_fee_basis_points,
                lp_fee: ev.lp_fee,
                protocol_fee_basis_points: ev.protocol_fee_basis_points,
                protocol_fee: ev.protocol_fee,

                quote_amount_in_with_lp_fee: 0,
                quote_amount_out_without_lp_fee: ev.quote_amount_out_without_lp_fee,

                user_quote_amount_in: 0,
                user_quote_amount_out: ev.user_quote_amount_out,

                coin_creator: ev.coin_creator.to_string(),
                coin_creator_fee_basis_points: ev.coin_creator_fee_basis_points,
                coin_creator_fee: ev.coin_creator_fee,

                track_volume: false,
                min_base_amount_out: 0,
            }),

            AmmEvent::Buy(ev) => Some(Self {
                slot,
                is_success,
                tx_error: tx_error.clone(),

                signature: signature.to_string(),
                ix_name: ix.ix_name.clone(),
                ix_index: ix.ix_index as u8,
                is_inner: ix.is_inner,
                is_buy: true,

                associated_token_program: acc(accs, "associated_token_program"),
                base_mint,
                base_token_program: acc(accs, "base_token_program"),
                coin_creator_vault_ata: acc(accs, "coin_creator_vault_ata"),
                coin_creator_vault_authority: acc(accs, "coin_creator_vault_authority"),
                event_authority: acc(accs, "event_authority"),
                fee_config: acc(accs, "fee_config"),
                fee_program: acc(accs, "fee_program"),
                global_config: acc(accs, "global_config"),
                global_volume_accumulator: acc_opt(accs, "global_volume_accumulator"),
                pool: acc(accs, "pool"),
                pool_base_token_account: acc(accs, "pool_base_token_account"),
                pool_quote_token_account: acc(accs, "pool_quote_token_account"),
                program: acc(accs, "program"),
                protocol_fee_recipient: acc(accs, "protocol_fee_recipient"),
                protocol_fee_recipient_token_account: acc(accs, "protocol_fee_recipient_token_account"),
                quote_mint,
                quote_token_program: acc(accs, "quote_token_program"),
                system_program: acc(accs, "system_program"),
                user: acc(accs, "user"),
                user_base_token_account: acc(accs, "user_base_token_account"),
                user_quote_token_account: acc(accs, "user_quote_token_account"),
                user_volume_accumulator: acc_opt(accs, "user_volume_accumulator"),

                timestamp: ev.timestamp,

                base_amount_in: 0,
                base_amount_out: ev.base_amount_out,

                min_quote_amount_out: 0,
                max_quote_amount_in: ev.max_quote_amount_in,

                user_base_token_reserves: ev.user_base_token_reserves,
                user_quote_token_reserves: ev.user_quote_token_reserves,
                pool_base_token_reserves: ev.pool_base_token_reserves,
                pool_quote_token_reserves: ev.pool_quote_token_reserves,

                quote_amount_in: ev.quote_amount_in,
                quote_amount_out: 0,

                lp_fee_basis_points: ev.lp_fee_basis_points,
                lp_fee: ev.lp_fee,
                protocol_fee_basis_points: ev.protocol_fee_basis_points,
                protocol_fee: ev.protocol_fee,

                quote_amount_in_with_lp_fee: ev.quote_amount_in_with_lp_fee,
                quote_amount_out_without_lp_fee: 0,

                user_quote_amount_in: ev.user_quote_amount_in,
                user_quote_amount_out: 0,

                coin_creator: ev.coin_creator.to_string(),
                coin_creator_fee_basis_points: ev.coin_creator_fee_basis_points,
                coin_creator_fee: ev.coin_creator_fee,

                track_volume: ev.track_volume,
                min_base_amount_out: ev.min_base_amount_out,
            }),
        }
    }
}
