use borsh::BorshDeserialize;
use std::fmt;

use crate::types::Pubkey;


#[derive(BorshDeserialize, Debug)]
pub struct TradeEvent{
    mint: Pubkey,
    sol_amount: u64,
    token_amount: u64,
    is_buy: bool,
    user: Pubkey,
    timestamp: i64,
    virtual_sol_reserves: u64,
    virtual_token_reserves: u64,
    real_sol_reserves: u64,
    real_token_reserves: u64,
    fee_recipient: Pubkey,
    fee_basis_points: u64,
    fee: u64,
    creator: Pubkey,
    creator_fee_basis_points: u64,
    creator_fee: u64,
    track_volume: bool,
    total_unclaimed_tokens: u64,
    total_claimed_tokens: u64,
    current_sol_volume: u64,
    last_update_timestamp: i64,
    ix_name: String
}

#[derive(BorshDeserialize, Debug)]
pub struct CreateEvent{
    name: String,
    symbol: String,
    uri: String,
    mint: Pubkey,
    bonding_curve: Pubkey,
    user: Pubkey, 
    creator: Pubkey,
    timestamp: i64,
    virtual_token_reserves: u64,
    virtual_sol_reserves: u64,
    real_token_reserves: u64,
    token_total_supply: u64,
    token_program: Pubkey,
    is_mayhem_mode: bool
}

#[derive(BorshDeserialize, Debug)]
pub struct SetMetaplexCreatorEvent{
    timestamp: i64,
    mint: Pubkey,
    bonding_curve: Pubkey,
    metadata: Pubkey,
    creator: Pubkey
}

#[derive(BorshDeserialize, Debug)]
pub struct CompletePumpAmmMigrationEvent{
    user: Pubkey, 
    mint: Pubkey,
    mint_amount: u64,
    sol_amount: u64,
    pool_migration_fee: u64,
    bonding_curve: Pubkey,
    timestamp: i64,
    pool: Pubkey
}

#[derive(BorshDeserialize, Debug)]
pub struct CompleteEvent{
    user: Pubkey,
    mint: Pubkey,
    bonding_curve: Pubkey,
    timestamp: i64
}

#[derive(BorshDeserialize, Debug)]
pub struct CollectCreatorFeeEvent{
    timestamp: i64,
    creator: Pubkey,
    creator_fee: u64
}

#[derive(Debug)]
pub enum PumpEvent {
    Trade(TradeEvent),
    Create(CreateEvent),
    SetMetaplexCreator(SetMetaplexCreatorEvent),
    CompletePumpAmmMigration(CompletePumpAmmMigrationEvent),
    Complete(CompleteEvent),
    CollectCreatorFee(CollectCreatorFeeEvent),
}