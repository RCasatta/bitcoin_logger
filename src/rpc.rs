use crate::store::{EventOrData, EventType};
use crate::{LoggerOptions, Result};
use bitcoincore_rpc::json::EstimateMode;
use bitcoincore_rpc::RpcApi;
use log::{debug, info};
use serde::{Deserialize, Serialize};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::Sender;
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

/// contains the parameter(blocks) and result(fee) of calling satoshi=estimatesmartfee(blocks)
#[derive(Debug, Serialize, Deserialize, Eq, PartialOrd, PartialEq)]
pub struct Fee(pub u16, pub u64); // blocks and satoshi per kB
pub const BLOCKS: [u16; 24] = [
    1u16, 2, 3, 4, 5, 6, 8, 10, 12, 15, 18, 21, 24, 36, 50, 70, 100, 144, 250, 288, 350, 504, 750,
    1008,
];

/// ask the node rawmempool to double check with computed one? Not sure, maybe save separately
/// ask estimatesmartfee
pub fn start(
    options: &LoggerOptions,
    sender: Sender<Option<EventOrData>>,
    running: Arc<AtomicBool>,
) -> Result<()> {
    info!("start rpc");
    let mut counter = 0u64;

    while running.load(Ordering::SeqCst) {
        if counter % options.estimatesmartfee_every == 0 {
            let now = Instant::now();
            for mode in [EstimateMode::Conservative, EstimateMode::Economical].iter() {
                let mut vec = Vec::with_capacity(BLOCKS.len());
                let client = options.node_config.make_rpc_client()?;
                for block in BLOCKS.iter() {
                    let fee = client.estimate_smart_fee(*block, Some(*mode))?;
                    if let Some(fee_rate) = fee.fee_rate {
                        vec.push(Fee(fee.blocks as u16, fee_rate.as_sat()));
                    }
                }
                let data_type = match mode {
                    EstimateMode::Conservative => EventType::EstimateSmartFeesConservative,
                    EstimateMode::Economical => EventType::EstimateSmartFeesEconomical,
                    _ => unreachable!(),
                };
                let event = EventOrData::event(data_type, serde_cbor::to_vec(&vec)?);
                sender.send(Some(event))?;
            }
            debug!(
                "ask estimatesmartfee took {} millis",
                now.elapsed().as_millis()
            );
        }
        if counter % options.getrawmempool_every == 0 {
            let now = Instant::now();
            let client = options.node_config.make_rpc_client()?;
            let txs = client.get_raw_mempool()?;
            let event = EventOrData::event(EventType::RawMempool, serde_cbor::to_vec(&txs)?);
            sender.send(Some(event))?;
            info!(
                "ask getrawmempool took {} millis",
                now.elapsed().as_millis()
            );
        }
        counter += 1;
        thread::sleep(Duration::from_secs(1));
    }
    info!("end rpc");
    Ok(())
}
