use std::fs;

mod config;
mod prelude;
mod pump_amm_utils;
mod pumpfun_utils;
mod types;
mod utils;

use std::error::Error;

type AppResult<T> = Result<T, Box<dyn Error>>;

use crate::{
    prelude::*,
    utils::{
        AmmEventContext, EventContext, InstructionContext, collect_instructions, instructions, join_amm_ix_and_events, join_pump_ix_and_events, load_db, match_and_collect, match_and_print
    },
};

// =======================
// MAIN
// =======================

fn main() -> AppResult<()> {
    let (pump_idl, amm_idl) = load_idls();
    let _ch_client = load_db(); // пока не используем, но пусть инициализируется

    // AMM tx (для теста)
    let tx_file_content = fs::read_to_string("./tx.json")?;
    let block_notification: BlockNotification = serde_json::from_str(&tx_file_content)?;
    let transactions = &block_notification.params.result.value.block.transactions;

    // pumpfun tx
    let pump_tx_f_content = fs::read_to_string("./new_pump.json")?;
    let pump_block_notification: BlockNotification = serde_json::from_str(&pump_tx_f_content)?;
    let pump_txs = &pump_block_notification
        .params
        .result
        .value
        .block
        .transactions;

    // AMM
    //process_amm_transactions(transactions, &amm_idl)?;
    // Pumpfun
    process_pump_transactions(pump_txs, &pump_idl)?;

    Ok(())
}

// =======================
// ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ
// =======================



// =======================
// PUMPFUN PROCESSING
// =======================

fn process_pump_transactions(
    transactions: &Vec<Transaction>,
    idl: &PumpIdl,
) -> AppResult<()> {
    for (tx_idx, tx) in transactions.iter().enumerate() {
        println!("\n================ PUMPFUN TX #{tx_idx} ================");

        let Some((pump_instructions, pump_inner_instructions)) =
            collect_instructions(tx, PUMPFUN_PROGRAM_ADDRESS)
        else {
            println!("Pumpfun program not found in this tx, skipping");
            continue;
        };

        let signature = tx.transaction.signatures.get(0).unwrap();
        println!("Signature: {}", signature);

        // 1) Собираем контексты инструкций (outer + inner)
        let ix_contexts = instructions(
            tx,
            idl,
            &pump_instructions,
            &pump_inner_instructions,
            tx_idx,
        )?;
        println!("pump ix_contexts: {:#?}", ix_contexts);

        // 2) Собираем контексты событий
        let mut event_contexts: Vec<EventContext> = Vec::new();

        for (log_idx, line) in tx
            .meta
            .log_messages
            .as_deref()
            .unwrap_or(&[])
            .iter()
            .enumerate()
        {
            if let Some(event) = decode_pump_event_from_log(line)? {
                event_contexts.push(EventContext {
                    log_index: log_idx,
                    raw_log: line.clone(),
                    event,
                });
            }
        }

        println!("pump event_contexts: {:#?}", event_contexts);

        // 3) Джойним инструкции и события
        let joined_actions = join_pump_ix_and_events(&ix_contexts, &event_contexts);

        println!("PUMPFUN joined actions (per logical op):");
        for action in &joined_actions {
            println!("================ PUMPFUN ACTION ================");

            // --- Информация об инструкции ---
            println!("ix_name:   {}", action.ix.ix_name);
            println!("ix_label:  {}", action.ix.label);
            println!("ix_index:  {}", action.ix.ix_index);
            println!("is_inner:  {}", action.ix.is_inner);

            println!("accounts:");
            for (name, pk) in &action.ix.accounts {
                println!("  - {name:30} => {pk}");
            }

            // --- Полный эвент ---
            match &action.event.event {
                PumpEvent::Trade(ev) => {
                    println!("TradeEvent:");
                    println!("  timestamp:              {}", ev.timestamp);
                    println!("  mint:                   {}", ev.mint);
                    println!("  user:                   {}", ev.user);
                    println!("  is_buy:                 {}", ev.is_buy);
                    println!("  sol_amount:             {}", ev.sol_amount);
                    println!("  token_amount:           {}", ev.token_amount);
                    println!("  virtual_sol_reserves:   {}", ev.virtual_sol_reserves);
                    println!("  virtual_token_reserves: {}", ev.virtual_token_reserves);
                    println!("  real_sol_reserves:      {}", ev.real_sol_reserves);
                    println!("  real_token_reserves:    {}", ev.real_token_reserves);
                    println!("  fee_recipient:          {}", ev.fee_recipient);
                    println!("  fee_basis_points:       {}", ev.fee_basis_points);
                    println!("  fee:                    {}", ev.fee);
                    println!("  creator:                {}", ev.creator);
                    println!("  creator_fee_basis_pts:  {}", ev.creator_fee_basis_points);
                    println!("  creator_fee:            {}", ev.creator_fee);
                    println!("  track_volume:           {}", ev.track_volume);
                    println!("  total_unclaimed_tokens: {}", ev.total_unclaimed_tokens);
                    println!("  total_claimed_tokens:   {}", ev.total_claimed_tokens);
                    println!("  current_sol_volume:     {}", ev.current_sol_volume);
                    println!("  last_update_timestamp:  {}", ev.last_update_timestamp);
                    println!("  ix_name (from event):   {}", ev.ix_name);
                }

                PumpEvent::Create(ev) => {
                    println!("CreateEvent:");
                    println!("  timestamp:           {}", ev.timestamp);
                    println!("  name:                {}", ev.name);
                    println!("  symbol:              {}", ev.symbol);
                    println!("  uri:                 {}", ev.uri);
                    println!("  mint:                {}", ev.mint);
                    println!("  bonding_curve:       {}", ev.bonding_curve);
                    println!("  user:                {}", ev.user);
                    println!("  creator:             {}", ev.creator);
                    println!("  virtual_token_reserves: {}", ev.virtual_token_reserves);
                    println!("  virtual_sol_reserves:   {}", ev.virtual_sol_reserves);
                    println!("  real_token_reserves:    {}", ev.real_token_reserves);
                    println!("  token_total_supply:     {}", ev.token_total_supply);
                    println!("  token_program:          {}", ev.token_program);
                    println!("  is_mayhem_mode:         {}", ev.is_mayhem_mode);
                }

                PumpEvent::CollectCreatorFee(ev) => {
                    println!("CollectCreatorFeeEvent:");
                    println!("  timestamp:   {}", ev.timestamp);
                    println!("  creator:     {}", ev.creator);
                    println!("  creator_fee: {}", ev.creator_fee);
                }

                PumpEvent::SetMetaplexCreator(ev) => {
                    println!("SetMetaplexCreatorEvent: {:#?}", ev);
                }

                PumpEvent::CompletePumpAmmMigration(ev) => {
                    println!("CompletePumpAmmMigrationEvent: {:#?}", ev);
                }

                PumpEvent::Complete(ev) => {
                    println!("CompleteEvent: {:#?}", ev);
                }
            }
        }
    }

    Ok(())
}


// =======================
// AMM PROCESSING
// =======================

fn process_amm_transactions(transactions: &Vec<Transaction>, idl: &PumpIdl) -> AppResult<()> {
    for (tx_idx, tx) in transactions.iter().enumerate() {
        println!("\n================ AMM TX #{tx_idx} ================");

        let Some((amm_instructions, amm_inner_instructions)) =
            collect_instructions(tx, PUMPSWAP_PROGRAM_ADDRESS)
        else {
            println!("AMM program not found in this tx, skipping");
            continue;
        };

        let signature = tx.transaction.signatures.get(0).unwrap();
        println!("Signature: {}", signature);

        // 1) Собираем контексты инструкций (outer + inner)
        let ix_contexts =
            instructions(tx, idl, &amm_instructions, &amm_inner_instructions, tx_idx)?;

        println!("amm ix_contexts: {:#?}", ix_contexts);

        // 2) Собираем контексты событий
        let mut amm_event_contexts: Vec<AmmEventContext> = Vec::new();

        for (log_idx, line) in tx
            .meta
            .log_messages
            .as_deref()
            .unwrap_or(&[])
            .iter()
            .enumerate()
        {
            if let Some(event) = decode_amm_event_from_log(line)? {
                amm_event_contexts.push(AmmEventContext {
                    log_index: log_idx,
                    raw_log: line.clone(),
                    event,
                });
            }
        }

        println!("amm event_contexts: {:#?}", amm_event_contexts);

        // 3) Джойним инструкции и события в "действия"
        let joined_actions = join_amm_ix_and_events(&ix_contexts, &amm_event_contexts);

        println!("AMM joined actions (per logical trade):");
        for action in &joined_actions {
            println!("================ AMM ACTION ================");

            // 1) Информация об инструкции
            println!("ix_name:   {}", action.ix.ix_name);
            println!("ix_label:  {}", action.ix.label);
            println!("ix_index:  {}", action.ix.ix_index);
            println!("is_inner:  {}", action.ix.is_inner);

            println!("accounts:");
            for (name, pk) in &action.ix.accounts {
                println!("  - {name:30} => {pk}");
            }

            // 2) Полный эвент
            match &action.event.event {
                AmmEvent::Sell(ev) => {
                    println!("SellEvent:");
                    println!("timestamp: {}", ev.timestamp);
                    println!("base_amount_in: {}", ev.base_amount_in);
                    println!("min_quote_amount_out: {}",ev.min_quote_amount_out);
                    println!("user_base_token_reserves: {}",ev.user_base_token_reserves);
                    println!("user_quote_token_reserves: {}", ev.user_quote_token_reserves);
                    println!("pool_base_token_reserves:{}",ev.pool_base_token_reserves);
                    println!("  pool_quote_token_reserves: {}",ev.pool_quote_token_reserves);
                    println!("quote_amount_out:{}", ev.quote_amount_out);
                    println!("lp_fee_basis_points: {}",ev.lp_fee_basis_points);
                    println!("lp_fee:{}", ev.lp_fee);
                    println!("protocol_fee_basis_points:{}",ev.protocol_fee_basis_points);
                    println!("protocol_fee:{}", ev.protocol_fee);
                    println!("quote_amount_out_without_lp_fee: {}",ev.quote_amount_out_without_lp_fee);
                    println!("user_quote_amount_out:{}",ev.user_quote_amount_out);
                    println!("pool:{}", ev.pool);
                    println!("user:{}", ev.user);
                    println!("user_base_token_account:{}",ev.user_base_token_account);
                    println!("user_quote_token_account:{}",ev.user_quote_token_account);
                    println!("protocol_fee_recipient:{}",ev.protocol_fee_recipient);
                    println!("protocol_fee_recipient_token_account: {}",ev.protocol_fee_recipient_token_account);
                    println!("coin_creator:{}", ev.coin_creator);
                    println!("coin_creator_fee_basis_points:{}",ev.coin_creator_fee_basis_points);
                    println!("coin_creator_fee:{}", ev.coin_creator_fee);
                }

                AmmEvent::Buy(ev) => {
                    println!("BuyEvent:");
                    println!("timestamp:{}", ev.timestamp);
                    println!("base_amount_out:{}", ev.base_amount_out);
                    println!("max_quote_amount_in:{}", ev.max_quote_amount_in);
                    println!("user_base_token_reserves:  {}",ev.user_base_token_reserves);
                    println!("user_quote_token_reserves: {}",ev.user_quote_token_reserves);
                    println!("pool_base_token_reserves:  {}",ev.pool_base_token_reserves);
                    println!("pool_quote_token_reserves: {}",ev.pool_quote_token_reserves);
                    println!("quote_amount_in:{}", ev.quote_amount_in);
                    println!("lp_fee_basis_points:{}", ev.lp_fee_basis_points);
                    println!("lp_fee:{}", ev.lp_fee);
                    println!("protocol_fee_basis_points: {}",ev.protocol_fee_basis_points);
                    println!("protocol_fee:{}", ev.protocol_fee);
                    println!("quote_amount_in_with_lp_fee: {}",ev.quote_amount_in_with_lp_fee);
                    println!("user_quote_amount_in:{}", ev.user_quote_amount_in);
                    println!("pool:{}", ev.pool);
                    println!("user:{}", ev.user);
                    println!("user_base_token_account:{}",ev.user_base_token_account);
                    println!("user_quote_token_account:  {}",ev.user_quote_token_account);
                    println!("protocol_fee_recipient:{}", ev.protocol_fee_recipient);
                    println!("protocol_fee_recipient_token_account: {}",ev.protocol_fee_recipient_token_account);
                    println!("coin_creator:{}", ev.coin_creator);
                    println!("coin_creator_fee_basis_points: {}",ev.coin_creator_fee_basis_points);
                    println!("coin_creator_fee:{}", ev.coin_creator_fee);
                    println!("track_volume:{}", ev.track_volume);
                    println!("total_unclaimed_tokens:{}",ev.total_unclaimed_tokens);
                    println!("total_claimed_tokens:{}",ev.total_claimed_tokens);
                    println!("current_sol_volume:{}", ev.current_sol_volume);
                    println!("last_update_timestamp:{}",ev.last_update_timestamp);
                    println!("min_base_amount_out:{}",ev.min_base_amount_out);
                    println!("ix_name (from event):{}", ev.ix_name);
                }
            }
        }
    }

    Ok(())
}
