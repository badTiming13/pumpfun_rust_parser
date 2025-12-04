use serde::{Deserialize, Serialize};
use serde_json::{Number, Value};

#[derive(Serialize, Deserialize, Debug)]
pub struct BlockNotification {
    pub jsonrpc: String,
    pub method: String,
    pub params: NotificationParams,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct NotificationParams {
    pub result: NotificationResult,
    pub subscription: u64,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct NotificationResult {
    pub context: RpcContext,
    pub value: ValueParam,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct RpcContext {
    pub slot: u64,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct ValueParam {
    pub slot: u64,
    pub block: BlockValue,
    pub err: Option<Value>,
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct BlockValue {
    pub previous_blockhash: String,
    pub blockhash: String,
    pub parent_slot: u64,
    pub transactions: Vec<Transaction>,
    pub block_time: i64,
    pub block_height: u64,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct Transaction {
    pub transaction: TransactionValue,
    pub meta: Meta,
    pub version: Option<serde_json::Value>,
}


#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct Meta {
    pub err: Option<Value>,
    pub status: Value,
    pub fee: u64,
    pub pre_balances: Vec<u64>,
    pub post_balances: Vec<u64>,
    #[serde(default)]
    pub inner_instructions: Option<Vec<InnerInstruction>>,
    #[serde(default)]
    pub log_messages: Option<Vec<String>>,
    pub pre_token_balances: Vec<TokenBalance>,
    pub post_token_balances: Vec<TokenBalance>,
    pub rewards: Option<Value>,
    pub loaded_addresses: LoadedAddresses,
    pub compute_units_consumed: u64,
    pub cost_units: u64,
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct InnerInstruction {
    pub index: u8,
    pub instructions: Vec<Instruction>,
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct TokenBalance {
    pub account_index: u8,
    pub mint: String,
    pub ui_token_amount: UiTokenAmount,
    pub owner: String,
    pub program_id: String,
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct UiTokenAmount {
    pub ui_amount: Option<f64>,
    pub decimals: u8,
    pub amount: String,
    pub ui_amount_string: String,
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct LoadedAddresses {
    pub writable: Vec<String>,
    pub readonly: Vec<String>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct TransactionValue {
    pub signatures: Vec<String>,
    pub message: TransactionMessage,
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct TransactionMessage {
    pub header: MessageHeader,
    pub account_keys: Vec<String>,
    pub recent_blockhash: String,
    pub instructions: Vec<Instruction>,
    #[serde(default)]
    pub address_table_lookups: Vec<AddressLookup>,
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct AddressLookup {
    pub account_key: String,
    pub writable_indexes: Vec<u8>,
    pub readonly_indexes: Vec<u8>,
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct MessageHeader {
    pub num_required_signatures: u8,
    pub num_readonly_signed_accounts: u8,
    pub num_readonly_unsigned_accounts: u8,
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct Instruction {
    pub program_id_index: u8,
    pub accounts: Vec<u8>,
    pub data: String,
    pub stack_height: u8,
}
