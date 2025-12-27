#### NEWEST UPDATE #### 
Extract Slot and Error from block and transaction respectively, create these 2 new fields for each table, write it. 



## Parsing & decoding AMM transaction works
Added combined events + instruction display for Pumpfun & Pumpswap  


## TODO: 
### 1. create DB connector for Clickhouse
### 2. create WS connection client
### 3. add calculation  of price, market cap
### 4. to be continued ...

## Don't forget to make tests
How much time needed to parse 1 block, 1 tx ? 
How much time needed to save block of transactions ?
CPU and memory consumption

Make optimizations based on the results

## Websocket, how to?
```
use futures::{SinkExt, StreamExt};
use serde_json::json;
use tokio_tungstenite::{connect_async, tungstenite::protocol::Message};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // WebSocket endpoint Helius
    let url = "wss://solana-mainnet.core.chainstack.com/3dec72ea492a69e1ea1fa532c2de1af7";
    let (mut ws_stream, _response) = connect_async(url).await?;
    println!("✅ Connected to {url}");

    // blockSubscribe с mentionsAccountOrProgram (pump.fun program)
    let subscribe_msg = json!({
        "jsonrpc": "2.0",
        "id": 1,
        "method": "blockSubscribe",
        "params": [
            {
                "mentionsAccountOrProgram": "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P"
            },
            {
                "commitment": "confirmed",
                "encoding": "json",              // или "base64", если хочешь сырые данные
                "transactionDetails": "full",    // full / signatures / none
                "maxSupportedTransactionVersion": 0,
                "showRewards": false
            }
        ]
    });

    ws_stream
        .send(Message::Text(subscribe_msg.to_string()))
        .await?;
    println!("📨 Sent blockSubscribe");

    // Читаем блоки
    while let Some(msg) = ws_stream.next().await {
        let msg = msg?;
        if let Message::Text(text) = msg {
            println!("🔔 Got block: {text}");
        }
    }

    Ok(())
}
```
