use crate::store::{Event, EventOrData, EventType, HashHeight};
use crate::{LoggerOptions, Result, NAME};
use bitcoin::consensus::serialize;
use bitcoin::hashes::core::fmt::Formatter;
use bitcoin::hashes::Hash;
use bitcoin::{Block, BlockHash, Transaction, Txid};
use bitcoincore_rpc::{Client, RpcApi};
use flate2::write::DeflateEncoder;
use flate2::Compression;
use log::{info, warn};
use lru::LruCache;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::convert::TryInto;
use std::fmt::Display;
use std::path::PathBuf;
use std::sync::mpsc::Receiver;
use std::time::Instant;
use std::{fmt, fs};

pub fn start(options: &LoggerOptions, receiver: Receiver<Option<Vec<EventOrData>>>) -> Result<()> {
    info!("start flush");
    let client = options.node_config.make_rpc_client()?;
    let mut height_cache = HeightCache::new(&client);
    let mut output_values_cache = OutputValuesCache::new(&client);

    while let Some(data_vector) = receiver.recv()? {
        if data_vector.is_empty() {
            continue;
        }
        info!("start flush with {} elements", data_vector.len());

        // initialize data structures
        let initialize = Instant::now();
        let mut blocks_height: HashMap<BlockHash, u32> = HashMap::new();
        let mut txs_height: HashMap<Txid, u32> = HashMap::new();
        let mut txs_output_values: HashMap<Txid, OutputValues> = HashMap::new();

        let Splitted {
            events,
            mut blocks,
            mut txs,
        } = split(data_vector)?;
        info!(
            "got {} events, {} blocks, {} txs",
            events.len(),
            blocks.len(),
            txs.len()
        );
        for event in events.iter() {
            if event.1 == EventType::Sequence {
                if let Ok(Sequence::BlockConnected(hash)) = parse_sequence(&event.2) {
                    if !blocks.contains_key(&hash) {
                        if let Ok(block) = client.get_block(&hash) {
                            info!(
                                "block {} was in blocks connected but missing, downloaded from node",
                                hash
                            );
                            blocks.insert(hash, block);
                        } else {
                            warn!("block {} was in blocks connected but missing, download from node failed!", hash);
                        }
                    }
                }
            }
        }
        let ts = events.first().map(|e| e.0).unwrap_or(0) / 1000;
        let tx_in_blocks: HashMap<_, _> = blocks
            .values()
            .map(|b| b.txdata.iter().map(|tx| (tx.txid(), tx.clone())))
            .flatten()
            .collect();
        let txid_in_blocks: HashSet<_> = tx_in_blocks.keys().cloned().collect();
        txs.extend(tx_in_blocks);
        info!("initialize {} millis", initialize.elapsed().as_millis());

        // process blocks, populate `blocks_height` and with tx contained in block `txs_height`
        // pre-populate `height_cache` to increase following iterations hit-rate
        let populate_start = Instant::now();
        for (hash, block) in blocks.iter() {
            let block_result = match client.get_block_info(hash) {
                Ok(block_result) => block_result,
                Err(e) => {
                    warn!(
                        "error in get_block_info, probably orphaned block {} {:?}",
                        hash, e
                    );
                    continue;
                }
            };

            let block_result_height = block_result.height as u32;
            blocks_height.insert(*hash, block_result_height);
            height_cache.put_block_height(*hash, block_result_height);
            for tx in block.txdata.iter() {
                let cur_txid = tx.txid();
                txs_height.insert(cur_txid, block_result_height);
                height_cache.put_tx_height(cur_txid, block_result_height);
            }
        }
        info!(
            "populate_start {}s for {} get_block_info",
            populate_start.elapsed().as_secs(),
            blocks.len(),
        );

        // find any txid referenced in events, ask the node if not available in `txs`
        let populate_events = Instant::now();
        let mut tx_in_events = HashSet::new();
        let mut count_get_raw_transaction = 0;
        let mut count_get_raw_transaction_error = 0;
        for event in events.iter() {
            match event.1 {
                EventType::Sequence => {
                    if let Sequence::TxAdded(txid, _) = parse_sequence(&event.2)? {
                        if txs.get(&txid).is_none() {
                            // this is rare because every TxAdded is followed by the Tx, so I should have those
                            count_get_raw_transaction += 1;
                            match client.get_raw_transaction(&txid, None) {
                                Ok(tx) => {
                                    txs.insert(txid, tx);
                                }
                                Err(_) => count_get_raw_transaction_error += 1,
                            }
                        }
                        if txs.get(&txid).is_some() {
                            tx_in_events.insert(txid);
                        }
                    }
                }
                EventType::InitialRawMempool => {
                    let txs_id: Vec<Txid> = serde_cbor::from_slice(&event.2)?;
                    info!("Initial raw mempool has {}", txs_id.len());
                    for txid in txs_id {
                        if txs.get(&txid).is_none() {
                            // this is rare because every TxAdded is followed by the Tx, so I should have those
                            count_get_raw_transaction += 1;
                            match client.get_raw_transaction(&txid, None) {
                                Ok(tx) => {
                                    txs.insert(txid, tx);
                                }
                                Err(_) => count_get_raw_transaction_error += 1,
                            }
                        }
                        if txs.get(&txid).is_some() {
                            tx_in_events.insert(txid);
                        }
                    }
                }
                _ => (),
            }
        }
        info!(
            "populate_events {}s for {} get_raw_transaction with {} errors",
            populate_events.elapsed().as_secs(),
            count_get_raw_transaction,
            count_get_raw_transaction_error,
        );

        // pre-populate `output_values_cache` with known txs (that at this point contains also tx in blocks)
        for tx in txs.values() {
            output_values_cache.put(tx);
        }

        // for every tx seen in events or in blocks, check previous outputs information (so it's possible to compute the fee)
        let download_input = Instant::now();
        let mut download_counter_error = 0;
        let mut total_inputs = 0;
        for event_txid in tx_in_events.iter().chain(txid_in_blocks.iter()) {
            let tx = txs.get(event_txid).unwrap(); // unwrap() safe because previous if txs.get(&txid).is_some() {tx_in_events.insert(txid);} and because txs.extend(tx_in_blocks)
            for input in tx.input.iter() {
                total_inputs += 1;
                let input_txid = &input.previous_output.txid;
                match output_values_cache.get(input_txid) {
                    Ok(output_values) => {
                        txs_output_values.insert(*input_txid, output_values);
                    }
                    Err(_) => {
                        download_counter_error += 1;
                    }
                }
            }
        }
        info!(
            "download_input {}s for total_inputs:{} {} errors:{}",
            download_input.elapsed().as_secs(),
            total_inputs,
            output_values_cache.stat(),
            download_counter_error
        );

        // every tx in txs and previous_txs should know it's confirmation height
        let confirmation_height = Instant::now();
        let mut count_unconfirmed = 0;
        let mut count_ask_cache = 0;
        let mut height_cache_errors = 0;
        for txid in txs.keys().chain(txs_output_values.keys()) {
            if !txs_height.contains_key(txid) {
                count_ask_cache += 1;
                match height_cache.get_tx_height(txid) {
                    Ok(Some(height)) => {
                        txs_height.insert(*txid, height);
                    }
                    Ok(None) => count_unconfirmed += 1,
                    Err(_) => height_cache_errors += 1,
                }
            }
        }
        info!(
            "confirmation_height {}s total:{} count_ask_cache:{} unconfirmed:{} {} errors:{}",
            confirmation_height.elapsed().as_secs(),
            txs.len() + txs_output_values.len(),
            count_ask_cache,
            count_unconfirmed,
            height_cache.stat(),
            height_cache_errors,
        );

        // create and flush data
        let flush = Instant::now();
        let data = Data {
            txs: txs
                .into_iter()
                .filter(|(a, _)| !txid_in_blocks.contains(a))
                .map(|(_, b)| serialize(&b))
                .collect(),
            blocks: blocks.iter().map(|(_, b)| serialize(b)).collect(),
            txs_height: txs_height
                .into_iter()
                .filter(|(t, _)| !txid_in_blocks.contains(t))
                .map(|(t, h)| TxidHeight(t, h))
                .collect(),
            blocks_height: blocks_height
                .into_iter()
                .map(|(t, h)| HashHeight(t, h))
                .collect(),
            output_values: txs_output_values
                .into_iter()
                .map(|(a, b)| TxidOutputValues(a, b))
                .collect(),
        };
        info!("data: {}", data);

        let bitcoin_log = BitcoinLog::new(events, data);
        let final_name = format!("{}.bitcoin_log", ts);
        let mut final_path = options.save_path.clone();
        final_path.push(final_name);

        write_log(&bitcoin_log, &final_path)?;
        info!("flushing took {}s", flush.elapsed().as_secs());
    }
    info!("end flush");
    Ok(())
}

pub fn write_log<T>(bitcoin_log: &T, final_path: &PathBuf) -> Result<()>
where
    T: Serialize,
{
    let mut temp_path = final_path.clone();
    let temp_name = format!(
        "{}.writing",
        final_path.file_name().unwrap().to_str().unwrap()
    );
    temp_path.set_file_name(temp_name);
    let file = fs::File::create(&temp_path)?;
    let zip_encoder = DeflateEncoder::new(file, Compression::best());
    serde_cbor::to_writer(zip_encoder, &bitcoin_log)?;
    fs::rename(temp_path, final_path)?;
    Ok(())
}

#[derive(Debug, Serialize, Deserialize)]
pub struct BitcoinLog {
    pub name: String,
    pub version: u8,
    pub events: Vec<Event>,
    pub data: Data,
}

/// Contains data referenced by Event, also previous output txs
#[derive(Debug, Serialize, Deserialize, Default)]
pub struct Data {
    /// vec containing [bitcoin::Transaction], it's Vec<u8> because consensus serialization it's used
    /// contains all tx referenced by a Sequence::TxAdded events
    pub txs: Vec<Vec<u8>>,
    /// vec containing [bitcoin::Block], it's Vec<u8> because consensus serialization it's used
    /// contains all blocks referenced by a Sequence::BlockConnected events
    pub blocks: Vec<Vec<u8>>,
    /// vec containing output values of previous transactions for `txs` and tx contained
    /// in a block identified by the hash in Sequence::BlockConnected, so that is possible to calculate fee
    pub output_values: Vec<TxidOutputValues>,
    /// contains txid and height of confirmation for confirmed txs not already included in `blocks`
    pub txs_height: Vec<TxidHeight>,
    /// contains block height for very block in `blocks`
    pub blocks_height: Vec<HashHeight>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct TxidHeight(pub Txid, pub u32);

/// Contains txid, it's outputs values, (optionally the tx fee), tx weight, (optionally the confirmed height)
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct TxidOutputValues(pub Txid, pub OutputValues);

pub type OutputValues = Box<[u64]>;

pub fn get_output_values(tx: &Transaction) -> OutputValues {
    tx.output.iter().map(|o| o.value).collect()
}

impl BitcoinLog {
    fn new(events: Vec<Event>, data: Data) -> Self {
        BitcoinLog {
            name: NAME.to_string(),
            version: 1,
            events,
            data,
        }
    }
}

impl Display for Data {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "self.txs_height:{} self.txs:{} self.output_values:{} self.blocks:{} self.blocks_height:{}", self.txs_height.len(), self.txs.len(), self.output_values.len(), self.blocks.len(), self.blocks_height.len())
    }
}

struct Splitted {
    events: Vec<Event>,
    blocks: HashMap<BlockHash, Block>,
    txs: HashMap<Txid, Transaction>,
}

fn split(vec: Vec<EventOrData>) -> Result<Splitted> {
    let mut blocks = HashMap::new();
    let mut txs = HashMap::new();
    let mut events = vec![];
    for el in vec {
        match el {
            EventOrData::Event(event) => {
                events.push(event);
            }
            EventOrData::Block(block) => {
                blocks.insert(block.block_hash(), block);
            }
            EventOrData::Transaction(tx) => {
                txs.insert(tx.txid(), tx);
            }
        }
    }
    Ok(Splitted {
        events,
        blocks,
        txs,
    })
}

/// https://github.com/bitcoin/bitcoin/blob/master/doc/zmq.md
/// <32-byte hash>C :                 Blockhash connected
/// <32-byte hash>D :                 Blockhash disconnected
/// <32-byte hash>R<8-byte LE uint> : Transactionhash removed from mempool for non-block inclusion reason
/// <32-byte hash>A<8-byte LE uint> : Transactionhash added mempool
pub enum Sequence {
    BlockConnected(BlockHash),
    BlockDisconnected(BlockHash),
    TxRemoved(Txid, u64),
    TxAdded(Txid, u64),
}

/// from raw sequence to sequence enum
pub fn parse_sequence(seq: &[u8]) -> Result<Sequence> {
    let mut array: [u8; 32] = seq[0..32].try_into()?;
    array.reverse();
    Ok(match seq[32] {
        b'C' => Sequence::BlockConnected(BlockHash::from_inner(array)),
        b'D' => Sequence::BlockDisconnected(BlockHash::from_inner(array)),
        b'R' => Sequence::TxRemoved(
            Txid::from_inner(array),
            u64::from_le_bytes((&seq[33..]).try_into()?),
        ),
        b'A' => Sequence::TxAdded(
            Txid::from_inner(array),
            u64::from_le_bytes((&seq[33..]).try_into()?),
        ),

        _ => panic!("Unexpected sequence type"),
    })
}

struct OutputValuesCache<'a> {
    cache: LruCache<Txid, OutputValues>,
    client: &'a Client,
    total_requests: u64,
    cache_hit: u64,
}

impl<'a> OutputValuesCache<'a> {
    pub fn new(client: &'a Client) -> Self {
        OutputValuesCache {
            cache: LruCache::new(1_000_000), // ~50Mb with mean of 2.5 outputs
            client,
            total_requests: 0,
            cache_hit: 0,
        }
    }

    pub fn put(&mut self, tx: &Transaction) {
        self.cache.put(tx.txid(), get_output_values(tx));
    }

    pub fn get(&mut self, txid: &Txid) -> Result<OutputValues> {
        self.total_requests += 1;
        match self.cache.get(txid) {
            Some(tx_output_values) => {
                self.cache_hit += 1;
                Ok(tx_output_values.clone())
            }
            None => {
                let tx = self.client.get_raw_transaction(txid, None)?;
                let output_values = get_output_values(&tx);
                self.cache.put(*txid, output_values.clone());
                Ok(output_values)
            }
        }
    }

    pub fn stat(&self) -> String {
        let hit_rate = (self.cache_hit as f64 / self.total_requests as f64) * 100.0;
        format!(
            "output_values_cache size:{} hit:{:.1}%",
            self.cache.len(),
            hit_rate,
        )
    }
}

struct HeightCache<'a> {
    cache: LruCache<Txid, u32>,
    client: &'a Client,
    block_height: HashMap<BlockHash, u32>,
    total_requests: u64,
    cache_hit: u64,
}

impl<'a> HeightCache<'a> {
    pub fn new(client: &'a Client) -> Self {
        HeightCache {
            client,
            cache: LruCache::new(1_000_000), // ~ 36Mb
            block_height: HashMap::new(),
            total_requests: 0,
            cache_hit: 0,
        }
    }

    pub fn put_tx_height(&mut self, txid: Txid, height: u32) {
        self.cache.put(txid, height);
    }

    pub fn put_block_height(&mut self, block_hash: BlockHash, height: u32) {
        self.block_height.insert(block_hash, height);
    }

    fn get_block_height(&mut self, hash: &BlockHash) -> Result<u32> {
        match self.block_height.get(hash) {
            Some(height) => Ok(*height),
            None => {
                let result = self.client.get_block_info(hash)?;
                let height = result.height as u32;
                self.block_height.insert(*hash, height);
                Ok(height)
            }
        }
    }

    pub fn get_tx_height(&mut self, txid: &Txid) -> Result<Option<u32>> {
        self.total_requests += 1;

        match self.cache.get(txid) {
            Some(height) => {
                self.cache_hit += 1;
                Ok(Some(*height))
            }
            None => {
                let tx_info = self.client.get_raw_transaction_info(txid, None)?;
                tx_info
                    .blockhash
                    .map(|block_hash| {
                        let height = self.get_block_height(&block_hash)?;
                        self.cache.put(*txid, height);
                        Ok(height)
                    })
                    .transpose()
            }
        }
    }

    pub fn stat(&self) -> String {
        let hit_rate = (self.cache_hit as f64 / self.total_requests as f64) * 100.0;
        format!(
            "height_cache size:{} hit:{:.1}% block_height:{}",
            self.cache.len(),
            hit_rate,
            self.block_height.len()
        )
    }
}

#[cfg(test)]
mod tests {
    use crate::store::{now, Event, EventType};
    use flate2::read::DeflateDecoder;
    use flate2::write::DeflateEncoder;
    use flate2::Compression;
    use std::io::Cursor;

    #[test]
    fn test_rt() {
        let data = Event(now(), EventType::Start, vec![]);
        let data_vector = vec![data];
        let mut cursor = Cursor::new(vec![]);
        let zip_encoder = DeflateEncoder::new(&mut cursor, Compression::best());
        serde_cbor::to_writer(zip_encoder, &data_vector).unwrap();
        cursor.set_position(0);
        let zip_decoder = DeflateDecoder::new(cursor);
        let rountripped: Vec<Event> = serde_cbor::from_reader(zip_decoder).unwrap();
        assert_eq!(rountripped, data_vector);
    }
}
