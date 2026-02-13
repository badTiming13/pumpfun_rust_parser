use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

use redis::aio::MultiplexedConnection;
use tracing::{debug, error, info};

use pump_parser_core::extract::pump::extract_pump_block;
use pump_parser_core::types::block_notification::BlockNotification;
use pump_parser_core::types::pump_idl::PumpIdl;

use crate::AppResult;
use crate::types::{
    PumpAmmMigrationRow, PumpCreateRow, PumpCreatorFeeRow, PumpMigrateIxSignal, PumpTradeRow,
};
use crate::utils::{lag_ms_from_chain_ts, publish_event, publish_event_stream};
use crate::prelude::{PUMPFUN_PROGRAM_ADDRESS, PumpEvent};

const LOG_EVERY: u64 = 2000;

static PUMP_TRADES_CNT: AtomicU64 = AtomicU64::new(0);
static PUMP_CREATES_CNT: AtomicU64 = AtomicU64::new(0);
static PUMP_FEES_CNT: AtomicU64 = AtomicU64::new(0);

pub async fn process_pump_block(
    notification: &BlockNotification,
    idl: &PumpIdl,
    redis_conn: &mut MultiplexedConnection,
) -> AppResult<()> {
    let t0 = Instant::now();

    let extracted = match extract_pump_block(notification, idl, PUMPFUN_PROGRAM_ADDRESS) {
        Ok(v) => v,
        Err(e) => {
            error!(error=%e, "extract_pump_block failed");
            return Ok(());
        }
    };

    debug!(
        slot=%extracted.slot,
        joined_len=extracted.joined_actions.len(),
        migrate_ix_len=extracted.migrate_ix.len(),
        "pump extracted"
    );

    for action in extracted.joined_actions.iter() {
        let signature = action.event.signature.as_str();

        let is_success = true;
        let tx_error: Option<&str> = None;

        match action.event.event {
            PumpEvent::Trade(_) => {
                if let Some(row) = PumpTradeRow::from_joined(
                    signature,
                    extracted.slot,
                    is_success,
                    tx_error,
                    action,
                ) {
                    publish_event(redis_conn, "pump:trades", &row).await?;
                    publish_event_stream(redis_conn, "db:pump:trades", &row).await?;

                    let n = PUMP_TRADES_CNT.fetch_add(1, Ordering::Relaxed) + 1;
                    if n % LOG_EVERY == 0 {
                        let lag_ms = lag_ms_from_chain_ts(row.timestamp);
                        info!(
                            channel="pump",
                            kind="trade",
                            every=LOG_EVERY,
                            count=n,
                            lag_ms=lag_ms,
                            parse_ms=t0.elapsed().as_millis() as u64,
                            "lag"
                        );
                    }
                }
            }

            PumpEvent::Create(_) => {
                if let Some(row) = PumpCreateRow::from_joined(
                    signature,
                    extracted.slot,
                    is_success,
                    tx_error,
                    action,
                ) {
                    publish_event(redis_conn, "pump:creates", &row).await?;
                    publish_event_stream(redis_conn, "db:pump:creates", &row).await?;

                    let n = PUMP_CREATES_CNT.fetch_add(1, Ordering::Relaxed) + 1;
                    if n % LOG_EVERY == 0 {
                        let lag_ms = lag_ms_from_chain_ts(row.timestamp);
                        info!(
                            channel="pump",
                            kind="create",
                            every=LOG_EVERY,
                            count=n,
                            lag_ms=lag_ms,
                            parse_ms=t0.elapsed().as_millis() as u64,
                            "lag"
                        );
                    }
                }
            }

            PumpEvent::CollectCreatorFee(_) => {
                if let Some(row) = PumpCreatorFeeRow::from_joined(
                    signature,
                    extracted.slot,
                    is_success,
                    tx_error,
                    action,
                ) {
                    publish_event(redis_conn, "pump:creator_fees", &row).await?;
                    publish_event_stream(redis_conn, "db:pump:creator_fees", &row).await?;

                    let n = PUMP_FEES_CNT.fetch_add(1, Ordering::Relaxed) + 1;
                    if n % LOG_EVERY == 0 {
                        let lag_ms = lag_ms_from_chain_ts(row.timestamp);
                        info!(
                            channel="pump",
                            kind="creator_fee",
                            every=LOG_EVERY,
                            count=n,
                            lag_ms=lag_ms,
                            parse_ms=t0.elapsed().as_millis() as u64,
                            "lag"
                        );
                    }
                }
            }

            PumpEvent::CompletePumpAmmMigration(_) => {
                // 1) full row
                if let Some(row) = PumpAmmMigrationRow::from_joined(
                    signature,
                    extracted.slot,
                    is_success,
                    tx_error,
                    action,
                ) {
                    publish_event(redis_conn, "pump:migrations", &row).await?;
                    publish_event_stream(redis_conn, "db:pump:migrations", &row).await?;
                }

                let sig = PumpMigrateIxSignal {
                    slot: extracted.slot,
                    signature: signature.to_string(),
                    ix_index: 0,
                    is_inner: false,

                    // остальное возьмёт твой from_joined row выше,
                    // но для signal делаем из accounts, если есть:
                    mint: action.ix.accounts.get("mint").cloned().unwrap_or_default(),
                    pool: action.ix.accounts.get("pool").cloned().unwrap_or_default(),
                    bonding_curve: action
                        .ix
                        .accounts
                        .get("bonding_curve")
                        .cloned()
                        .unwrap_or_default(),
                    associated_bonding_curve: action
                        .ix
                        .accounts
                        .get("associated_bonding_curve")
                        .cloned()
                        .unwrap_or_default(),
                    user: action.ix.accounts.get("user").cloned().unwrap_or_default(),

                    pool_base_token_account: action
                        .ix
                        .accounts
                        .get("pool_base_token_account")
                        .cloned()
                        .unwrap_or_default(),
                    pool_quote_token_account: action
                        .ix
                        .accounts
                        .get("pool_quote_token_account")
                        .cloned()
                        .unwrap_or_default(),
                };

                publish_event(redis_conn, "pump:migrations", &sig).await?;
                publish_event_stream(redis_conn, "db:pump:migrations", &sig).await?;
            }

            PumpEvent::SetMetaplexCreator(_) | PumpEvent::Complete(_) => {}
        }
    }

    Ok(())
}
