use base64::{engine::general_purpose::STANDARD, Engine as _};
use borsh::BorshDeserialize;
use bs58;

use crate::types::block_notification::{Instruction, Transaction};
use crate::types::pump_idl::{Instruction as IdlInstruction, PumpIdl};

//Pumpfun Events
const TRADE_EVENT_DISCRIMINATOR: [u8; 8] = [189, 219, 127, 211, 78, 230, 97, 238];
const CREATE_EVENT_DISCRIMINATOR: [u8; 8] = [27, 114, 169, 77, 222, 235, 99, 118];
const SETMETAPLEX_CREATOR_EVENT_DISCRIMINATOR: [u8; 8] = [142, 203, 6, 32, 127, 105, 191, 162];
const COMPLETE_PUMPAMM_MIGRATION_EVENT_DISCRIMINATOR: [u8; 8] = [189, 233, 93, 185, 92, 148, 234, 148];
const COMPLETE_EVENT_DISCRIMINATOR: [u8; 8] = [95, 114, 97, 156, 212, 46, 152, 8];
const COLLECT_CREATORFEE_EVENT_DISCRIMINATOR: [u8; 8] = [122, 2, 127, 1, 14, 191, 12, 175];

