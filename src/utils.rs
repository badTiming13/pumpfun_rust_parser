use redis::aio::MultiplexedConnection;
use redis::AsyncCommands;
use serde::Serialize;
use std::fs;
use std::time::{SystemTime, UNIX_EPOCH};

pub use pump_parser_core::join::join_amm::JoinedAmmAction;
pub use pump_parser_core::join::join_pump::JoinedPumpAction;

// types
pub use pump_parser_core::types::pump_idl::PumpIdl;

pub fn load_idls() -> (PumpIdl, PumpIdl) {
    let amm_file_content =
        fs::read_to_string("./idl/pump_amm.json").expect("Couldn't read pump_amm.json");
    let pump_file_content = fs::read_to_string("./idl/pump.json").expect("Couldn't read pump.json");

    let pump_idl: PumpIdl =
        serde_json::from_str(&pump_file_content).expect("Serde JSON parse error (pump)");
    let amm_idl: PumpIdl =
        serde_json::from_str(&amm_file_content).expect("Serde JSON parse error (amm)");

    (pump_idl, amm_idl)
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
