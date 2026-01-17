use crate::AppResult;
use crate::prelude::PumpEvent;
use crate::types::AmmEvent;
use crate::types::block_notification::{Instruction, Transaction};
use crate::types::pump_idl::{Instruction as IdlInstruction, PumpIdl};
use base64::{engine::general_purpose::STANDARD, Engine as _};
use borsh::BorshDeserialize;
use bs58;
use clickhouse::{Client as ChClient, Row};
use redis::aio::MultiplexedConnection;
use redis::AsyncCommands;
use serde::Serialize;
use std::collections::BTreeMap;
use std::fs;
use std::time::{SystemTime, UNIX_EPOCH};
use tracing::{debug, info, warn};

pub type AccountMap = BTreeMap<String, String>;

#[derive(Debug, Clone)]
pub struct InstructionContext {
    pub ix_name: String,
    pub label: String,
    pub ix_index: usize,
    pub is_inner: bool,
    pub accounts: AccountMap,
}

#[derive(Debug, Clone)]
pub struct EventContext {
    pub log_index: usize,
    pub raw_log: String,
    pub event: PumpEvent,
}

#[derive(Debug, Clone)]
pub struct AmmEventContext {
    pub log_index: usize,
    pub raw_log: String,
    pub event: AmmEvent,
}

#[derive(Debug, Clone)]
pub struct JoinedAmmAction {
    pub ix: InstructionContext,
    pub event: AmmEventContext,
}

#[derive(Debug, Clone)]
pub struct JoinedPumpAction {
    pub ix: InstructionContext,
    pub event: EventContext,
}

pub fn join_amm_ix_and_events(
    ix_contexts: &[InstructionContext],
    events: &[AmmEventContext],
) -> Vec<JoinedAmmAction> {
    let mut joined = Vec::new();

    for ev_ctx in events {
        match &ev_ctx.event {
            AmmEvent::Sell(ev) => {
                let pool = ev.pool.to_string();
                let user = ev.user.to_string();

                if let Some(ix_ctx) = ix_contexts.iter().find(|ix| {
                    ix.ix_name == "sell"
                        && ix.accounts.get("pool").map(|s| s.as_str()) == Some(pool.as_str())
                        && ix.accounts.get("user").map(|s| s.as_str()) == Some(user.as_str())
                }) {
                    joined.push(JoinedAmmAction {
                        ix: ix_ctx.clone(),
                        event: ev_ctx.clone(),
                    });
                }
            }

            AmmEvent::Buy(ev) => {
                let pool = ev.pool.to_string();
                let user = ev.user.to_string();
                let ix_name = ev.ix_name.as_str();

                if let Some(ix_ctx) = ix_contexts.iter().find(|ix| {
                    ix.accounts.get("pool").map(|s| s.as_str()) == Some(pool.as_str())
                        && ix.accounts.get("user").map(|s| s.as_str()) == Some(user.as_str())
                        && ix.ix_name == ix_name
                }) {
                    joined.push(JoinedAmmAction {
                        ix: ix_ctx.clone(),
                        event: ev_ctx.clone(),
                    });
                }
            }
        }
    }

    joined
}

pub fn join_pump_ix_and_events(
    ix_contexts: &[InstructionContext],
    events: &[EventContext],
) -> Vec<JoinedPumpAction> {
    let mut joined = Vec::new();

    for ev_ctx in events {
        match &ev_ctx.event {
            PumpEvent::Trade(ev) => {
                let mint = ev.mint.to_string();
                let user = ev.user.to_string();
                let ix_name = ev.ix_name.as_str(); // "buy" или "sell"

                if let Some(ix_ctx) = ix_contexts.iter().find(|ix| {
                    ix.ix_name == ix_name
                        && ix.accounts.get("mint").map(|s| s.as_str()) == Some(mint.as_str())
                        && ix.accounts.get("user").map(|s| s.as_str()) == Some(user.as_str())
                }) {
                    joined.push(JoinedPumpAction {
                        ix: ix_ctx.clone(),
                        event: ev_ctx.clone(),
                    });
                }
            }

            PumpEvent::Create(ev) => {
                let mint = ev.mint.to_string();
                let user = ev.user.to_string();
                let bonding_curve = ev.bonding_curve.to_string();

                if let Some(ix_ctx) = ix_contexts.iter().find(|ix| {
                    ix.ix_name.starts_with("create")
                        && ix.accounts.get("mint").map(|s| s.as_str()) == Some(mint.as_str())
                        && ix.accounts.get("user").map(|s| s.as_str()) == Some(user.as_str())
                        && ix.accounts.get("bonding_curve").map(|s| s.as_str())
                            == Some(bonding_curve.as_str())
                }) {
                    joined.push(JoinedPumpAction {
                        ix: ix_ctx.clone(),
                        event: ev_ctx.clone(),
                    });
                }
            }

            PumpEvent::CollectCreatorFee(ev) => {
                let creator = ev.creator.to_string();

                if let Some(ix_ctx) = ix_contexts.iter().find(|ix| {
                    ix.ix_name.starts_with("collect")
                        && ix.accounts.get("creator").map(|s| s.as_str()) == Some(creator.as_str())
                }) {
                    joined.push(JoinedPumpAction {
                        ix: ix_ctx.clone(),
                        event: ev_ctx.clone(),
                    });
                }
            }

            _ => {}
        }
    }

    joined
}

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
pub fn map_ix_accounts(
    tx: &Transaction,
    ix: &Instruction,
    idl_ix: &IdlInstruction,
) -> Vec<(String, String)> {
    let all_accounts = get_accounts(tx);
    let mut mapped = Vec::new();

    for (i, idl_acc) in idl_ix.accounts.iter().enumerate() {
        let Some(&account_idx) = ix.accounts.get(i) else {
            continue;
        };
        let idx = account_idx as usize;

        if let Some(pk) = all_accounts.get(idx) {
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

/// Декодирует data, достаёт дискриминатор и ищет соответствующую инструкцию в IDL
pub fn match_instruction<'a>(
    ix: &Instruction,
    idl: &'a PumpIdl,
) -> Result<Option<(&'a IdlInstruction, Vec<u8>)>, bs58::decode::Error> {
    let decoded = bs58::decode(&ix.data).into_vec()?;

    if decoded.len() < 8 {
        return Ok(None);
    }

    let discriminator = &decoded[..8];

    let maybe_ix = idl
        .instructions
        .iter()
        .find(|idl_ix| idl_ix.discriminator.as_slice() == discriminator);

    Ok(maybe_ix.map(|idl_ix| (idl_ix, decoded)))
}

pub fn match_and_collect(
    tx: &Transaction,
    ix: &Instruction,
    idl: &PumpIdl,
    label: String,
    ix_index: usize,
    is_inner: bool,
) -> Result<Option<InstructionContext>, Box<dyn std::error::Error>> {
    if let Some((idl_ix, decoded)) = match_instruction(ix, idl)? {
        let discriminator = &decoded[..8];

        debug!(label=%label, ix_name=%idl_ix.name, "Matched instruction");
        debug!(label=%label, signature=?tx.transaction.signatures.get(0), "Signature");
        debug!(label=%label, discriminator=?discriminator, docs=?idl_ix.docs, "IDL details");

        let mapped_accounts = map_ix_accounts(tx, ix, idl_ix);
        debug!(label=%label, accounts_len=mapped_accounts.len(), "Mapped accounts");

        let mut accounts_map = BTreeMap::new();
        for (name, pk) in mapped_accounts {
            debug!(label=%label, account_name=%name, account_pk=%pk, "Account mapping");
            accounts_map.insert(name, pk);
        }

        let ctx = InstructionContext {
            ix_name: idl_ix.name.clone(),
            label,
            ix_index,
            is_inner,
            accounts: accounts_map,
        };

        Ok(Some(ctx))
    } else {
        debug!(label=%label, "Unknown instruction (no match in IDL)");
        Ok(None)
    }
}

pub fn collect_instructions<'a>(
    tx: &'a Transaction,
    addr: &str,
) -> Option<(Vec<&'a Instruction>, Vec<&'a Instruction>)> {
    let Some(program_index) = find_program_index(tx, addr) else {
        return None;
    };

    let (outer, inner) = collect_program_instructions(tx, program_index);

    debug!(program=%addr, program_index=program_index, "Program index");
    debug!(program=%addr, outer_len=outer.len(), inner_len=inner.len(), "Instruction counts");

    Some((outer, inner))
}

pub fn instructions<'a>(
    tx: &'a Transaction,
    idl: &PumpIdl,
    outer_instructions: &[&'a Instruction],
    inner_instructions: &[&'a Instruction],
    tx_idx: usize,
) -> AppResult<Vec<InstructionContext>> {
    let mut ix_contexts: Vec<InstructionContext> = Vec::new();

    for (idx, ix) in outer_instructions.iter().enumerate() {
        if let Some(ctx) = match_and_collect(
            tx,
            ix,
            idl,
            format!("tx #{tx_idx} outer #{idx}"),
            idx,
            false,
        )? {
            ix_contexts.push(ctx);
        }
    }

    for (idx, ix) in inner_instructions.iter().enumerate() {
        if let Some(ctx) =
            match_and_collect(tx, ix, idl, format!("tx #{tx_idx} inner #{idx}"), idx, true)?
        {
            ix_contexts.push(ctx);
        }
    }

    Ok(ix_contexts)
}

pub fn match_and_print(
    tx: &Transaction,
    ix: &Instruction,
    idl: &PumpIdl,
    label: String,
) -> Result<(), Box<dyn std::error::Error>> {
    if let Some((idl_ix, decoded)) = match_instruction(ix, idl)? {
        let discriminator = &decoded[..8];

        debug!(label=%label, ix_name=%idl_ix.name, "Matched instruction");
        debug!(label=%label, signature=?tx.transaction.signatures.get(0), "Signature");
        debug!(label=%label, discriminator=?discriminator, docs=?idl_ix.docs, "IDL details");

        let mapped_accounts = map_ix_accounts(tx, ix, idl_ix);
        debug!(label=%label, accounts_len=mapped_accounts.len(), "Mapped accounts");
        for (name, pk) in mapped_accounts {
            debug!(label=%label, account_name=%name, account_pk=%pk, "Account mapping");
        }
    } else {
        debug!(label=%label, "Unknown instruction (no match in IDL)");
    }

    Ok(())
}

pub fn load_idls() -> (PumpIdl, PumpIdl) {
    let amm_file_content =
        fs::read_to_string("./idl/pump_amm.json").expect("Couldn't read pump_amm.json");
    let pump_file_content = fs::read_to_string("./idl/pump.json").expect("Couldn't read pump.json");

    let amm_idl: PumpIdl =
        serde_json::from_str(&amm_file_content).expect("Serde JSON parse error (amm)");
    let pump_idl: PumpIdl =
        serde_json::from_str(&pump_file_content).expect("Serde JSON parse error (pump)");

    (pump_idl, amm_idl)
}

pub fn load_db() -> ChClient {
    let ch_url =
        std::env::var("CLICKHOUSE_URL").unwrap_or_else(|_| "http://localhost:8123".to_string());
    let ch_db = std::env::var("CLICKHOUSE_DB").unwrap_or_else(|_| "pump".to_string());
    let ch_user = std::env::var("CLICKHOUSE_USER").unwrap_or_else(|_| "default".to_string());
    let ch_password = std::env::var("CLICKHOUSE_PASSWORD").unwrap_or_else(|_| "pump".to_string());

    let ch_client = ChClient::default()
        .with_url(ch_url)
        .with_database(ch_db)
        .with_user(ch_user)
        .with_password(ch_password);

    ch_client
}

pub async fn publish_event<T: Serialize>(
    conn: &mut MultiplexedConnection,
    channel: &str,
    payload: &T,
) -> redis::RedisResult<()> {
    let json = serde_json::to_string(payload).expect("Failed to serialize event to JSON");
    let _: () = conn.publish(channel, json).await?;
    Ok(())
}

// ---- Redis Stream для базы ----
pub async fn publish_event_stream<T: Serialize>(
    conn: &mut MultiplexedConnection,
    stream: &str,
    payload: &T,
) -> redis::RedisResult<()> {
    let observed_at_ms = now_ms();

    let json = serde_json::to_string(&serde_json::json!({
        "observed_at_ms": observed_at_ms,
        "payload": payload
    }))
    .expect("Failed to serialize stream event to JSON");

    let _: String = redis::cmd("XADD")
        .arg(stream)
        .arg("*")
        .arg("data")
        .arg(json)
        .query_async(conn)
        .await?;

    Ok(())
}

pub fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64
}

pub fn ts_to_ms(ts: i64) -> i64 {
    if ts < 2_000_000_000_000 {
        ts * 1000
    } else {
        ts
    }
}

pub fn lag_ms_from_chain_ts(ts: i64) -> i64 {
    now_ms() - ts_to_ms(ts)
}
