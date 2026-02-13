use std::error::Error;
use std::sync::Arc;

mod config;
mod prelude;
mod utils;

mod stream;
mod pump_pipeline;
mod amm_pipeline;
mod types;

type AppResult<T> = Result<T, Box<dyn Error + Send + Sync>>;

use redis::Client as RedisClient;
use tokio::sync::Mutex;
use tracing::{error, info};
use tracing_subscriber::EnvFilter;

use crate::config::constants::{PUMPFUN_PROGRAM_ADDRESS, PUMPSWAP_PROGRAM_ADDRESS};
use crate::utils::load_idls;

#[tokio::main]
async fn main() -> AppResult<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .init();

    let (pump_idl, amm_idl) = load_idls();
    let pump_idl = Arc::new(pump_idl);
    let amm_idl = Arc::new(amm_idl);

    let redis_client = RedisClient::open("redis://127.0.0.1/")?;

    let pump_conn = redis_client.get_multiplexed_async_connection().await?;
    let amm_conn = redis_client.get_multiplexed_async_connection().await?;

    let pump_conn = Arc::new(Mutex::new(pump_conn));
    let amm_conn = Arc::new(Mutex::new(amm_conn));

    let url = "wss://solana-mainnet.core.chainstack.com/3dec72ea492a69e1ea1fa532c2de1af7";

    info!("starting streams...");

    let pump_task = {
        let pump_idl = Arc::clone(&pump_idl);
        let pump_conn = Arc::clone(&pump_conn);
        tokio::spawn(async move {
            let res = stream::run_block_subscribe(
                url,
                PUMPFUN_PROGRAM_ADDRESS,
                1,
                move |notif| {
                    let pump_idl = Arc::clone(&pump_idl);
                    let pump_conn = Arc::clone(&pump_conn);
                    async move {
                        let mut conn = pump_conn.lock().await;
                        pump_pipeline::process_pump_block(&notif, &pump_idl, &mut *conn).await
                    }
                },
            )
            .await;

            if let Err(e) = res {
                error!(error=%e, "pump stream crashed");
            }
        })
    };

    let amm_task = {
        let amm_idl = Arc::clone(&amm_idl);
        let amm_conn = Arc::clone(&amm_conn);
        tokio::spawn(async move {
            let res = stream::run_block_subscribe(
                url,
                PUMPSWAP_PROGRAM_ADDRESS,
                2,
                move |notif| {
                    let amm_idl = Arc::clone(&amm_idl);
                    let amm_conn = Arc::clone(&amm_conn);
                    async move {
                        let mut conn = amm_conn.lock().await;
                        amm_pipeline::process_amm_block(&notif, &amm_idl, &mut *conn).await
                    }
                },
            )
            .await;

            if let Err(e) = res {
                error!(error=%e, "amm stream crashed");
            }
        })
    };

    let _ = tokio::join!(pump_task, amm_task);
    Ok(())
}
