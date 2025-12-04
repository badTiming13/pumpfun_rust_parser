use base64::{engine::general_purpose::STANDARD, Engine as _};
use borsh::BorshDeserialize;
use bs58;

use crate::types::block_notification::{Instruction, Transaction};
use crate::types::pump_events::{CollectCreatorFeeEvent, CompleteEvent, CompletePumpAmmMigrationEvent, CreateEvent, PumpEvent, SetMetaplexCreatorEvent, TradeEvent};
use crate::types::pump_idl::{Instruction as IdlInstruction, PumpIdl};

//Pumpfun Events
const TRADE_EVENT_DISCRIMINATOR: [u8; 8] = [189, 219, 127, 211, 78, 230, 97, 238];
const CREATE_EVENT_DISCRIMINATOR: [u8; 8] = [27, 114, 169, 77, 222, 235, 99, 118];
const SETMETAPLEX_CREATOR_EVENT_DISCRIMINATOR: [u8; 8] = [142, 203, 6, 32, 127, 105, 191, 162];
const COMPLETE_PUMPAMM_MIGRATION_EVENT_DISCRIMINATOR: [u8; 8] = [189, 233, 93, 185, 92, 148, 234, 148];
const COMPLETE_EVENT_DISCRIMINATOR: [u8; 8] = [95, 114, 97, 156, 212, 46, 152, 8];
const COLLECT_CREATORFEE_EVENT_DISCRIMINATOR: [u8; 8] = [122, 2, 127, 1, 14, 191, 12, 175];

// Декодим BuyEvent / SellEvent из логов (`Program data: ...`)
// finish decoding events
pub fn decode_pump_event_from_log(line: &str) -> Result<Option<PumpEvent>, Box<dyn std::error::Error>> {
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

    if disc == TRADE_EVENT_DISCRIMINATOR {
        let event = TradeEvent::try_from_slice(payload)?;
        Ok(Some(PumpEvent::Trade(event)))
    } else if disc == CREATE_EVENT_DISCRIMINATOR {
        let event = CreateEvent::try_from_slice(payload)?;
        Ok(Some(PumpEvent::Create(event)))
    } else if disc == SETMETAPLEX_CREATOR_EVENT_DISCRIMINATOR {
        let event = SetMetaplexCreatorEvent::try_from_slice(payload)?;
        Ok(Some(PumpEvent::SetMetaplexCreator(event)))
    } else if disc == COMPLETE_PUMPAMM_MIGRATION_EVENT_DISCRIMINATOR {
        let event = CompletePumpAmmMigrationEvent::try_from_slice(payload)?;
        Ok(Some(PumpEvent::CompletePumpAmmMigration(event)))
    } else if disc == COMPLETE_EVENT_DISCRIMINATOR {
        let event = CompleteEvent::try_from_slice(payload)?;
        Ok(Some(PumpEvent::Complete(event)))
    } else if disc == COLLECT_CREATORFEE_EVENT_DISCRIMINATOR {
        let event = CollectCreatorFeeEvent::try_from_slice(payload)?;
        Ok(Some(PumpEvent::CollectCreatorFee(event)))
    } else {
        Ok(None)
    }
}

