use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct PumpIdl{
    pub address: String,
    pub metadata: Metadata,
    pub instructions: Vec<Instruction>,
    pub accounts: Vec<Account>,
    pub events: Vec<Event>,
    pub errors: Vec<IdlError>,
    pub types: Vec<IdlTypeDef>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Metadata{
    pub name: String,
    pub version: String,
    pub spec: String,
    pub description: String
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Instruction{
    pub name: String,
    pub docs: Option<Vec<String>>,
    pub discriminator: Vec<u8>,
    pub accounts: Vec<Value>,
    pub args: Vec<Value>
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Account{
    pub name: String,
    pub discriminator: Vec<u8>
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Event{
    pub name: String,
    pub discriminator: Vec<u8>
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct IdlError{
    pub code: u16,
    pub name: String
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct IdlTypeDef{
    pub name: String,
    pub docs: Option<Vec<String>>,
    #[serde(rename = "type")]
    pub r#type: IdlTypeBody,
}
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct IdlTypeBody{
    pub kind: String,
    pub fields: Vec<FieldDef>
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(untagged)]
pub enum FieldType {
    Simple(String),
    Complex(Value),
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(untagged)]
pub enum FieldDef {
    Named(Field),
    Simple(String),
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Field{
    pub name: String,
    #[serde(rename = "type")]
    pub r#type: FieldType
}