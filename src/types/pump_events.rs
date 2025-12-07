use borsh::BorshDeserialize;
use std::fmt;

use crate::types::Pubkey;


#[derive(BorshDeserialize, Debug, Clone)]
pub struct TradeEvent{
    pub mint: Pubkey,
    pub sol_amount: u64,
    pub token_amount: u64,
    pub is_buy: bool,
    pub user: Pubkey,
    pub timestamp: i64,
    pub virtual_sol_reserves: u64,
    pub virtual_token_reserves: u64,
    pub real_sol_reserves: u64,
    pub real_token_reserves: u64,
    pub fee_recipient: Pubkey,
    pub fee_basis_points: u64,
    pub fee: u64,
    pub creator: Pubkey,
    pub creator_fee_basis_points: u64,
    pub creator_fee: u64,
    pub track_volume: bool,
    pub total_unclaimed_tokens: u64,
    pub total_claimed_tokens: u64,
    pub current_sol_volume: u64,
    pub last_update_timestamp: i64,
    pub ix_name: String
}

#[derive(BorshDeserialize, Debug, Clone)]
pub struct CreateEvent{
    pub name: String,
    pub symbol: String,
    pub uri: String,
    pub mint: Pubkey,
    pub bonding_curve: Pubkey,
    pub user: Pubkey, 
    pub creator: Pubkey,
    pub timestamp: i64,
    pub virtual_token_reserves: u64,
    pub virtual_sol_reserves: u64,
    pub real_token_reserves: u64,
    pub token_total_supply: u64,
    pub token_program: Pubkey,
    pub is_mayhem_mode: bool
}

#[derive(BorshDeserialize, Debug, Clone)]
pub struct SetMetaplexCreatorEvent{
    pub timestamp: i64,
    pub mint: Pubkey,
    pub bonding_curve: Pubkey,
    pub metadata: Pubkey,
    pub creator: Pubkey
}

#[derive(BorshDeserialize, Debug, Clone)]
pub struct CompletePumpAmmMigrationEvent{
    pub user: Pubkey, 
    pub mint: Pubkey,
    pub mint_amount: u64,
    pub sol_amount: u64,
    pub pool_migration_fee: u64,
    pub bonding_curve: Pubkey,
    pub timestamp: i64,
    pub pool: Pubkey
}

#[derive(BorshDeserialize, Debug, Clone)]
pub struct CompleteEvent{
    pub user: Pubkey,
    pub mint: Pubkey,
    pub bonding_curve: Pubkey,
    pub timestamp: i64
}

#[derive(BorshDeserialize, Debug, Clone)]
pub struct CollectCreatorFeeEvent{
    pub timestamp: i64,
    pub creator: Pubkey,
    pub creator_fee: u64
}

#[derive(BorshDeserialize,Debug, Clone)]
pub enum PumpEvent {
    Trade(TradeEvent),
    Create(CreateEvent),
    SetMetaplexCreator(SetMetaplexCreatorEvent),
    CompletePumpAmmMigration(CompletePumpAmmMigrationEvent),
    Complete(CompleteEvent),
    CollectCreatorFee(CollectCreatorFeeEvent),
}