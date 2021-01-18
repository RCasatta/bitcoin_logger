use crate::flush::{BitcoinLog, Data, OutputValues, TxidOutputValues};
use crate::{LoggerOptions, Result, NAME};
use bitcoin::consensus::deserialize;
use bitcoin::{Block, BlockHash, Transaction, Txid};
use bitcoincore_rpc::RpcApi;
use flate2::bufread::DeflateDecoder;
use log::{info, warn};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::fs::File;
use std::io::BufReader;
use std::iter::FromIterator;
use std::path::PathBuf;
use std::sync::mpsc::{Receiver, Sender};
use std::time::{SystemTime, UNIX_EPOCH};

/// contains timestamp, data type and data content
/// this is not a named struct so that serialization is smaller
#[derive(Debug, Serialize, Deserialize, Clone, Eq, PartialEq)]
pub struct Event(pub u64, pub EventType, pub Vec<u8>);

/// https://github.com/bitcoin/bitcoin/blob/master/doc/zmq.md
#[repr(u8)]
#[derive(Debug, Serialize, Deserialize, Clone, Eq, PartialEq)]
pub enum EventType {
    /// Event field contains result of zmqsequencepub
    Sequence = b'S',

    /// Event field  contains [Vec<Fee>] serialized as cbor, results of calling node `estimatesmartfee` with CONSERVATIVE
    EstimateSmartFeesConservative = b'f',

    /// Event field  contains [Vec<Fee>] serialized as cbor, results of calling node `estimatesmartfee` with ECONOMICAL
    EstimateSmartFeesEconomical = b'g',

    /// contains [Vec<Txid>] from `getrawmempool` serialized with cbor
    /// useful to cross-check with mempool maintained with [Sequence::TxAdded], [Sequence::TxRemoved], [Sequence::BlockDisconnected]
    RawMempool = b'm',

    /// contains [Vec<Txid>] from `getrawmempool` serialized with cbor, sent only once at process start
    /// useful to bootstrap the mempool in the reader process
    InitialRawMempool = b'i',

    /// contains [HashHeight] a block hash with his height serialized with cbor
    HashHeight = b'h',

    /// contains [HashHeader] a block hash with the full BlockHeader
    HashHeader = b'H',

    /// start of this process
    Start = b's',

    /// end of this process
    End = b'e',
}

#[derive(Debug, Serialize, Deserialize)]
pub struct HashHeight(pub BlockHash, pub u32);

#[derive(Debug, Serialize, Deserialize)]
pub struct HashHeader(pub BlockHash, pub Vec<u8>);

pub enum EventOrData {
    Event(Event),
    Transaction(Transaction),
    Block(Block),
}

impl EventOrData {
    pub fn event(event_type: EventType, event_data: Vec<u8>) -> Self {
        EventOrData::Event(Event(now(), event_type, event_data))
    }
}

pub fn start(
    options: &LoggerOptions,
    receiver: Receiver<Option<EventOrData>>,
    sender: Sender<Option<Vec<EventOrData>>>,
) -> Result<()> {
    info!("start store");
    let mut data_vector = Vec::with_capacity(options.elements);
    let mut elements = options.elements / 10; // first run with a 1/10th, growing 1/10th each run
    data_vector.push(get_hash_height_event(options)?); // first event is block count
    loop {
        match receiver.recv()? {
            Some(data) => {
                data_vector.push(data);
                if data_vector.len() >= elements {
                    if elements < options.elements {
                        elements += options.elements / 10
                    }
                    info!("send to flush thread and clear");
                    let mut to_send: Vec<EventOrData> = Vec::with_capacity(options.elements);
                    std::mem::swap(&mut data_vector, &mut to_send);
                    sender.send(Some(to_send))?;
                    data_vector.push(get_hash_height_event(options)?); // first event of new vector is block count
                }
            }
            None => {
                sender.send(Some(data_vector))?;
                sender.send(None)?;
                break;
            }
        }
    }
    info!("end store");
    Ok(())
}

pub fn get_hash_height_event(options: &LoggerOptions) -> Result<EventOrData> {
    let client = options.node_config.make_rpc_client()?;
    let best = client.get_best_block_hash()?;
    let height = client.get_block_info(&best)?.height as u32;
    let hash_height = HashHeight(best, height);
    Ok(EventOrData::event(
        EventType::HashHeight,
        serde_cbor::to_vec(&hash_height)?,
    ))
}

/// returns current time in millis since the epoch, truncation to u64 should be save for a while,
/// result has 23 leading zeros as 2020
pub fn now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
}

pub struct Transactions {
    txs: HashMap<Txid, Transaction>,
    txs_output_values: HashMap<Txid, OutputValues>,
    txs_height: HashMap<Txid, u32>,
}

impl From<TxidOutputValues> for OutputValues {
    fn from(v: TxidOutputValues) -> Self {
        v.1
    }
}

impl Transactions {
    pub fn new(data: Data) -> Self {
        let mut txs = HashMap::new();
        let txs_output_values: HashMap<_, _> = data
            .output_values
            .iter()
            .map(|info| (info.0, info.1.clone()))
            .collect();

        let mut txs_height: HashMap<_, _> = data.txs_height.iter().map(|e| (e.0, e.1)).collect();
        let blocks_height: HashMap<_, _> = data.blocks_height.iter().map(|e| (e.0, e.1)).collect();

        for vec in data.blocks.iter() {
            let block: Block = deserialize(vec).unwrap();
            let block_hash = block.block_hash();
            let height = match blocks_height.get(&block_hash) {
                Some(height) => *height,
                None => {
                    warn!("can't found block height for {}", block_hash);
                    continue;
                }
            };
            for tx in block.txdata {
                let txid = tx.txid();
                txs.insert(txid, tx);
                txs_height.insert(txid, height);
            }
        }

        for vec in data.txs.iter() {
            let tx: Transaction = deserialize(vec).unwrap();
            txs.insert(tx.txid(), tx);
        }

        Transactions {
            txs,
            txs_height,
            txs_output_values,
        }
    }

    pub fn get(&self, txid: &Txid) -> Option<&Transaction> {
        self.txs.get(txid)
    }

    pub fn height(&self, txid: &Txid) -> Option<u32> {
        self.txs_height.get(txid).cloned()
    }

    // fee rate in sat/vbytes
    pub fn fee_rate(&self, txid: &Txid) -> Option<f64> {
        let tx = self.txs.get(txid)?;
        let fee = self.absolute_fee(tx)?;
        Some((fee as f64) / (tx.get_weight() as f64 / 4.0))
    }

    // fee rate in sat/bytes
    pub fn fee_rate_bytes(&self, txid: &Txid) -> Option<f64> {
        let tx = self.txs.get(txid)?;
        let fee = self.absolute_fee(tx)?;
        Some((fee as f64) / (tx.get_size() as f64))
    }

    fn absolute_fee(&self, tx: &Transaction) -> Option<u64> {
        let sum_outputs: u64 = tx.output.iter().map(|o| o.value).sum();
        let mut sum_inputs: u64 = 0;
        for input in tx.input.iter() {
            let outputs_values = self.txs_output_values.get(&input.previous_output.txid)?;
            sum_inputs += outputs_values[input.previous_output.vout as usize];
        }
        Some(sum_inputs - sum_outputs)
    }
}

pub struct MempoolBuckets {
    /// contain the number of elements for bucket ith
    buckets: Vec<u32>,
    /// contain the fee rate limits for every bucket ith
    buckets_limits: Vec<f64>,
    /// in which bucket the Txid is in
    tx_bucket: HashMap<Txid, usize>,
}

impl MempoolBuckets {
    pub fn new(increment_percent: u32, upper_limit: f64) -> Self {
        let mut buckets_limits = vec![];
        let increment_percent = 1.0f64 + (increment_percent as f64 / 100.0f64);
        let mut current_value = 1.0f64;
        loop {
            if current_value >= upper_limit {
                break;
            }
            current_value *= increment_percent;
            buckets_limits.push(current_value);
        }
        let buckets = vec![0u32; buckets_limits.len()];

        MempoolBuckets {
            buckets,
            buckets_limits,
            tx_bucket: HashMap::new(),
        }
    }

    pub fn clear(&mut self) {
        self.tx_bucket.clear();
        for el in self.buckets.iter_mut() {
            *el = 0;
        }
    }

    pub fn add(&mut self, txid: Txid, rate: f64) {
        if rate > 1.0 && self.tx_bucket.get(&txid).is_none() {
            // TODO MempoolBuckets use array of indexes to avoid many comparisons?
            let index = self
                .buckets_limits
                .iter()
                .position(|e| e > &rate)
                .unwrap_or(self.buckets_limits.len() - 1);
            self.buckets[index] += 1;
            self.tx_bucket.insert(txid, index);
        }
    }

    pub fn remove(&mut self, txid: &Txid) {
        if let Some(index) = self.tx_bucket.remove(txid) {
            self.buckets[index] -= 1;
        }
    }

    pub fn number_of_buckets(&self) -> usize {
        self.buckets.len()
    }

    pub fn buckets_str(&self) -> String {
        self.buckets
            .iter()
            .map(|e| e.to_string())
            .collect::<Vec<_>>()
            .join(",")
    }

    pub fn len(&self) -> usize {
        self.tx_bucket.len()
    }

    pub fn is_empty(&self) -> bool {
        self.tx_bucket.is_empty()
    }

    pub fn txids_set(&self) -> HashSet<&Txid> {
        HashSet::from_iter(self.tx_bucket.keys())
    }
}

pub fn read_log(file: &PathBuf) -> Result<BitcoinLog> {
    let file = File::open(file)?;
    let buffer = BufReader::new(file);
    let decoder = DeflateDecoder::new(buffer);
    let log: BitcoinLog = serde_cbor::from_reader(decoder)?;
    if log.version != 1 {
        panic!(
            "wrong bitcoin log version, expected:1 found:{}",
            log.version
        );
    }
    if log.name != *NAME {
        panic!("wrong name");
    }
    info!("data.txs: {:?}", log.data.txs.len());
    info!("data.blocks: {:?}", log.data.blocks.len());
    info!("events: {:?}", log.events.len());
    Ok(log)
}

#[cfg(test)]
mod tests {
    use crate::store::{now, MempoolBuckets};
    use bitcoin::Txid;

    #[test]
    fn test_mempool() {
        let mut mempool = MempoolBuckets::new(20, 10.0);
        let txid = Txid::default();
        mempool.add(txid, 5.0);
        assert_eq!(mempool.len(), 1);
        assert_eq!(mempool.number_of_buckets(), 13);
        assert_eq!(mempool.buckets_str(), "0,0,0,0,0,0,0,0,1,0,0,0,0");
        mempool.remove(&txid);
        assert_eq!(mempool.len(), 0);
        assert_eq!(mempool.buckets_str(), "0,0,0,0,0,0,0,0,0,0,0,0,0");
        mempool.remove(&txid);
        assert_eq!(mempool.len(), 0);
        assert_eq!(mempool.buckets_str(), "0,0,0,0,0,0,0,0,0,0,0,0,0");
    }

    #[test]
    fn test_millis() {
        let now = now();
        assert_eq!(now.leading_zeros(), 23);
    }
}
