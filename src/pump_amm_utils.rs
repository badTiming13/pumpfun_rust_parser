use base64::{engine::general_purpose::STANDARD, Engine as _};
use borsh::BorshDeserialize;
use bs58;

use crate::types::block_notification::{Instruction, Transaction};
use crate::types::pump_idl::{Instruction as IdlInstruction, PumpIdl};
use crate::types::{AmmEvent, BuyEvent, SellEvent};

//Pumpswap Events
const BUY_EVENT_DISCRIMINATOR: [u8; 8] = [103, 244, 82, 31, 44, 245, 119, 119];
const SELL_EVENT_DISCRIMINATOR: [u8; 8] = [62, 47, 55, 10, 165, 3, 220, 42];


/// Декодирует data, достаёт дискриминатор и ищет соответствующую инструкцию в amm IDL
pub fn match_instruction<'a>(
    ix: &Instruction,
    idl: &'a PumpIdl,
) -> Result<Option<(&'a IdlInstruction, Vec<u8>)>, bs58::decode::Error> {
    let decoded = bs58::decode(&ix.data).into_vec()?;

    if decoded.len() < 8 {
        return Ok(None);
    }

    let discriminator = &decoded[..8];

    let maybe_ix = idl
        .instructions
        .iter()
        .find(|idl_ix| idl_ix.discriminator.as_slice() == discriminator);

    Ok(maybe_ix.map(|idl_ix| (idl_ix, decoded)))
}

/// Декодим BuyEvent / SellEvent из логов (`Program data: ...`)
pub fn decode_amm_event_from_log(line: &str) -> Result<Option<AmmEvent>, Box<dyn std::error::Error>> {
    let prefix = "Program data: ";
    let base64_part = match line.strip_prefix(prefix) {
        Some(s) => s.trim(),
        None => return Ok(None),
    };

    let bytes = STANDARD.decode(base64_part)?;

    if bytes.len() < 8 {
        return Ok(None);
    }

    let (disc, payload) = bytes.split_at(8);

    if disc == BUY_EVENT_DISCRIMINATOR {
        let event = BuyEvent::try_from_slice(payload)?;
        Ok(Some(AmmEvent::Buy(event)))
    } else if disc == SELL_EVENT_DISCRIMINATOR {
        let event = SellEvent::try_from_slice(payload)?;
        Ok(Some(AmmEvent::Sell(event)))
    } else {
        Ok(None)
    }
}

