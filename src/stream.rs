use crate::AppResult;
use futures::{SinkExt, StreamExt};
use serde_json::json;
use tokio::sync::mpsc;
use tokio_tungstenite::connect_async;
use tracing::{info, warn};

use pump_parser_core::types::block_notification::BlockNotification;

pub async fn run_block_subscribe<F, Fut>(
    url: &str,
    mentions_account_or_program: &str,
    id: u64,
    mut on_block: F,
) -> AppResult<()>
where
    F: FnMut(BlockNotification) -> Fut + Send + 'static,
    Fut: std::future::Future<Output = AppResult<()>> + Send + 'static,
{
    
    let (tx, mut rx) = mpsc::channel::<BlockNotification>(1024);

    let _worker = tokio::spawn(async move {
        while let Some(notif) = rx.recv().await {
            if let Err(e) = on_block(notif).await {
                warn!(error=%e, "block handler error");
            }
        }
    });

    loop {
        info!(url=%url, mentions=%mentions_account_or_program, "🔌 connecting ws...");

        let (mut ws_stream, _response) = connect_async(url).await?;
        info!(url=%url, mentions=%mentions_account_or_program, "✅ connected");

        let subscribe_msg = json!({
            "jsonrpc": "2.0",
            "id": id,
            "method": "blockSubscribe",
            "params": [
                { "mentionsAccountOrProgram": mentions_account_or_program },
                {
                    "commitment": "confirmed",
                    "encoding": "json",
                    "transactionDetails": "full",
                    "maxSupportedTransactionVersion": 0,
                    "showRewards": false
                }
            ]
        });

        ws_stream
            .send(tokio_tungstenite::tungstenite::Message::Text(subscribe_msg.to_string()))
            .await?;

        info!(mentions=%mentions_account_or_program, "📨 subscription sent");

        while let Some(msg) = ws_stream.next().await {
            match msg {
                Ok(tokio_tungstenite::tungstenite::Message::Text(text)) => {
                    let Ok(notification) = serde_json::from_str::<BlockNotification>(&text) else {
                        continue;
                    };

                    if let Err(_closed) = tx.send(notification).await {
                        warn!("queue closed");
                        break;
                    }
                }

                Ok(tokio_tungstenite::tungstenite::Message::Ping(p)) => {
                    ws_stream
                        .send(tokio_tungstenite::tungstenite::Message::Pong(p))
                        .await?;
                }

                Ok(tokio_tungstenite::tungstenite::Message::Close(frame)) => {
                    warn!(?frame, mentions=%mentions_account_or_program, "ws closed");
                    break;
                }

                Err(e) => {
                    warn!(error=%e, mentions=%mentions_account_or_program, "ws error");
                    break;
                }

                _ => {}
            }
        }

        info!(mentions=%mentions_account_or_program, "🔁 reconnect in 3s...");
        tokio::time::sleep(std::time::Duration::from_secs(3)).await;
    }

}
