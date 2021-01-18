use crate::store::{EventOrData, EventType};
use crate::{LoggerOptions, Result};
use bitcoin::consensus::deserialize;
use bitcoin::{Block, Transaction};
use log::info;
use lru::LruCache;
use std::convert::TryInto;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::Sender;
use std::sync::Arc;
use zmq::PollEvents;

/// subscribe to zmq topics
pub fn start(
    options: &LoggerOptions,
    sender: Sender<Option<EventOrData>>,
    running: Arc<AtomicBool>,
) -> Result<()> {
    info!("start zmq");
    let zmq_context = zmq::Context::new();
    let mut txid_cache = LruCache::new(500_000); // ~16Mb

    let subscriber = zmq_context.socket(zmq::SUB)?;
    subscriber
        .connect(&options.zmq_address)
        .expect("could not connect to publisher");
    info!("zmq connected");

    subscriber.set_subscribe(b"sequence")?;
    subscriber.set_subscribe(b"rawtx")?;
    subscriber.set_subscribe(b"rawblock")?;

    while running.load(Ordering::SeqCst) {
        if let Ok(events) = subscriber.poll(PollEvents::POLLIN, 1000) {
            if events > 0 {
                let topic = subscriber.recv_msg(0)?;
                let data = subscriber.recv_msg(0)?;
                let seq = subscriber.recv_msg(0)?;
                let _seq = u32::from_le_bytes((&seq[..]).try_into()?); // TODO check if sequential
                let event_or_data = match &topic[..] {
                    b"sequence" => EventOrData::event(EventType::Sequence, data.to_vec()),
                    b"rawtx" => {
                        let tx: Transaction = deserialize(&data)?;
                        let txid = tx.txid();
                        if txid_cache.contains(&txid) {
                            continue;
                        }
                        txid_cache.put(txid, ());
                        EventOrData::Transaction(tx)
                    }
                    b"rawblock" => {
                        let block: Block = deserialize(&data)?;
                        info!("received block {}", block.block_hash());
                        EventOrData::Block(block)
                    }
                    _ => panic!("unexpected topic"),
                };
                sender.send(Some(event_or_data))?;
            }
        }
    }
    info!("end zmq");
    Ok(())
}
