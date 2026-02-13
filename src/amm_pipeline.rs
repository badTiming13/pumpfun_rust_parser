use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

use redis::aio::MultiplexedConnection;
use tracing::{debug, error, info};

use pump_parser_core::extract::amm::extract_amm_block;
use pump_parser_core::types::block_notification::BlockNotification;
use pump_parser_core::types::pump_idl::PumpIdl;

use crate::AppResult;
use crate::types::AmmTradeRow;
use crate::utils::{lag_ms_from_chain_ts, publish_event, publish_event_stream};
use crate::prelude::PUMPSWAP_PROGRAM_ADDRESS;

const LOG_EVERY: u64 = 2000;
static AMM_TRADES_CNT: AtomicU64 = AtomicU64::new(0);

pub async fn process_amm_block(
    notification: &BlockNotification,
    idl: &PumpIdl,
    redis_conn: &mut MultiplexedConnection,
) -> AppResult<()> {
    let t0 = Instant::now();

    let extracted = match extract_amm_block(notification, idl, PUMPSWAP_PROGRAM_ADDRESS) {
        Ok(v) => v,
        Err(e) => {
            error!(error=%e, "extract_amm_block failed");
            return Ok(());
        }
    };

    debug!(
        slot=%extracted.slot,
        joined_len=extracted.joined_actions.len(),
        "amm extracted"
    );

    for action in extracted.joined_actions.iter() {
        let signature = action.event.signature.as_str();

        let is_success = true;
        let tx_error: Option<&str> = None;

        let Some(row) =
            AmmTradeRow::from_joined(signature, extracted.slot, is_success, tx_error, action)
        else {
            continue;
        };

        publish_event(redis_conn, "amm:trades", &row).await?;
        publish_event_stream(redis_conn, "db:amm:trades", &row).await?;

        let n = AMM_TRADES_CNT.fetch_add(1, Ordering::Relaxed) + 1;
        if n % LOG_EVERY == 0 {
            let lag_ms = lag_ms_from_chain_ts(row.timestamp);
            info!(
                channel="amm",
                kind="trade",
                every=LOG_EVERY,
                count=n,
                lag_ms=lag_ms,
                parse_ms=t0.elapsed().as_millis() as u64,
                "lag"
            );
        }
    }

    Ok(())
}
