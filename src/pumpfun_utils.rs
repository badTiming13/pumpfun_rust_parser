use base64::{engine::general_purpose::STANDARD, Engine as _};
use borsh::BorshDeserialize;

use tracing::{debug, info, warn};
use crate::types::pump_events::{
    CollectCreatorFeeEvent,
    CompleteEvent,
    CompletePumpAmmMigrationEvent,
    CreateEvent,
    PumpEvent,
    SetMetaplexCreatorEvent,
    TradeEvent,
};
use chrono::DateTime;
use std::io;

// Pumpfun Events discriminators
const TRADE_EVENT_DISCRIMINATOR: [u8; 8] = [189, 219, 127, 211, 78, 230, 97, 238];
const CREATE_EVENT_DISCRIMINATOR: [u8; 8] = [27, 114, 169, 77, 222, 235, 99, 118];
const SETMETAPLEX_CREATOR_EVENT_DISCRIMINATOR: [u8; 8] = [142, 203, 6, 32, 127, 105, 191, 162];
const COMPLETE_PUMPAMM_MIGRATION_EVENT_DISCRIMINATOR: [u8; 8] = [189, 233, 93, 185, 92, 148, 234, 148];
const COMPLETE_EVENT_DISCRIMINATOR: [u8; 8] = [95, 114, 97, 156, 212, 46, 152, 8];
const COLLECT_CREATORFEE_EVENT_DISCRIMINATOR: [u8; 8] = [122, 2, 127, 1, 14, 191, 12, 175];

/// Частичный Borsh-декодер:
/// - читает только поля, объявленные в T
/// - НЕ требует, чтобы все байты payload были съедены (в отличие от try_from_slice)
/// - логирует, если после декодинга остался хвост байтов
fn decode_event<T: BorshDeserialize>(
    payload: &[u8],
    base64_part: &str,
    disc_name: &str,
) -> Result<T, Box<dyn std::error::Error>> {
    let mut slice: &[u8] = payload;

    match T::deserialize(&mut slice) {
        Ok(ev) => {
            let leftover = slice.len();
            if leftover > 0 {
                debug!(
                    "⚠️ Borsh decode for {disc_name}: {leftover} extra bytes left (payload_len={}; base64_prefix={}...)",
                    payload.len(),
                    &base64_part[..base64_part.len().min(40)]
                );
            }
            Ok(ev)
        }
        Err(e) => {
            let msg = format!(
                "Borsh decode FAILED for {disc_name}: {e}; payload_len={}; base64={}",
                payload.len(),
                base64_part
            );
            Err(Box::new(io::Error::new(io::ErrorKind::InvalidData, msg)))
        }
    }
}

/// Декодим pumpfun-события из логов (`Program data: ...`)
pub fn decode_pump_event_from_log(
    line: &str,
) -> Result<Option<PumpEvent>, Box<dyn std::error::Error>> {
    let prefix = "Program data: ";
    let base64_part = match line.strip_prefix(prefix) {
        Some(s) => s.trim(),
        None => return Ok(None),
    };

    // ✅ Фильтр: это не Anchor event, а synopsis blob
    if base64_part.starts_with("Synopsis ") {
        return Ok(None);
    }

    // ✅ Мягкий decode: если не base64 — просто не наш event
    let bytes = match STANDARD.decode(base64_part) {
        Ok(b) => b,
        Err(_) => return Ok(None),
    };

    if bytes.len() < 8 {
        return Ok(None);
    }

    let (disc, payload) = bytes.split_at(8);

    if disc == TRADE_EVENT_DISCRIMINATOR {
        let event = decode_event::<TradeEvent>(payload, base64_part, "TradeEvent")?;
        Ok(Some(PumpEvent::Trade(event)))
    } else if disc == CREATE_EVENT_DISCRIMINATOR {
        let event = decode_event::<CreateEvent>(payload, base64_part, "CreateEvent")?;
        Ok(Some(PumpEvent::Create(event)))
    } else if disc == SETMETAPLEX_CREATOR_EVENT_DISCRIMINATOR {
        let event = decode_event::<SetMetaplexCreatorEvent>(
            payload,
            base64_part,
            "SetMetaplexCreatorEvent",
        )?;
        Ok(Some(PumpEvent::SetMetaplexCreator(event)))
    } else if disc == COMPLETE_PUMPAMM_MIGRATION_EVENT_DISCRIMINATOR {
        let event = decode_event::<CompletePumpAmmMigrationEvent>(
            payload,
            base64_part,
            "CompletePumpAmmMigrationEvent",
        )?;
        Ok(Some(PumpEvent::CompletePumpAmmMigration(event)))
    } else if disc == COMPLETE_EVENT_DISCRIMINATOR {
        let event = decode_event::<CompleteEvent>(payload, base64_part, "CompleteEvent")?;
        Ok(Some(PumpEvent::Complete(event)))
    } else if disc == COLLECT_CREATORFEE_EVENT_DISCRIMINATOR {
        let event = decode_event::<CollectCreatorFeeEvent>(
            payload,
            base64_part,
            "CollectCreatorFeeEvent",
        )?;
        Ok(Some(PumpEvent::CollectCreatorFee(event)))
    } else {
        Ok(None)
    }
}

// ==== Helpers для цены/маркета/даты ====

// virtual_* — это резервы на 1e9 токенов, но мы уже обсуждали формулы:
fn mid_price_sol(virtual_sol_reserves: f64, virtual_token_reserves: f64) -> f64 {
    (virtual_sol_reserves / virtual_token_reserves) * 1e-3
}

fn trade_price_sol(sol_amount: f64, token_amount: f64) -> f64 {
    (sol_amount / token_amount) * 1e-3
}

fn market_cap_sol(price: f64) -> f64 {
    price * 1_000_000_000f64
}

pub fn format_timestamp_human(ts: i64) -> String {
    let dt = DateTime::from_timestamp(ts, 0)
        .expect("invalid timestamp");

    dt.format("%Y-%m-%d %H:%M:%S").to_string()
}
