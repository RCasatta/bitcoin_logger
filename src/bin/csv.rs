use bitcoin::consensus::deserialize;
use bitcoin::hashes::core::fmt::Formatter;
use bitcoin::{Block, OutPoint, Transaction, Txid};
use bitcoin_logger::buckets::blocks::{BlocksBuckets, BlocksTimes};
use bitcoin_logger::buckets::mempool::{MempoolBuckets, MempoolWeightBuckets};
use bitcoin_logger::flush::{parse_sequence, BitcoinLog, Sequence};
use bitcoin_logger::rpc::Fee;
use bitcoin_logger::store::{read_log, EventType, HashHeight, Transactions};
use bitcoin_logger::CsvOptions;
use bitcoin_logger::Result;
use flate2::{Compression, GzBuilder};
use log::{debug, error, info, trace, warn};
use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::fmt::Display;
use std::fs::File;
use std::io::Write;
use std::iter::FromIterator;
use std::path::PathBuf;
use std::sync::mpsc::{channel, sync_channel, Receiver};
use std::time::Instant;
use std::{fmt, fs, thread};
use structopt::StructOpt;

fn main() -> Result<()> {
    let now = Instant::now();
    env_logger::init_from_env(
        env_logger::Env::default().filter_or(env_logger::DEFAULT_FILTER_ENV, "info"),
    );

    let opt = CsvOptions::from_args();
    info!("Options {:?}", opt);

    if opt.dataset_file.exists() {
        error!("output file {:?} already exists", &opt.dataset_file);
        return Ok(());
    }
    if let Some(fee_file) = opt.fee_file.as_ref() {
        if fee_file.exists() {
            error!("output file {:?} already exists", &fee_file);
            return Ok(());
        }
    }

    let files = read_dir(&opt.load_path)?;
    info!("Processing {:?} files", files.len());

    let concurrency: usize = opt.concurrency.into();
    let (sender, receiver) = sync_channel(opt.concurrency as usize);
    let mut shots = VecDeque::new();
    let h1 = thread::spawn(move || {
        for (i, file) in files.into_iter().enumerate() {
            let (one_shot, one_recv) = channel();
            shots.push_front(one_recv);
            thread::spawn(move || {
                let log = read_log(&file).unwrap();
                one_shot.send(log).unwrap();
            });

            if i >= concurrency - 1 {
                sender.send(shots.pop_back().unwrap().recv().ok()).unwrap();
            }
        }
        while let Some(shot) = shots.pop_back() {
            sender.send(shot.recv().ok()).unwrap();
        }
        sender.send(None).unwrap();
    });

    let h2 = thread::spawn(move || {
        process(receiver, opt).unwrap();
    });

    h1.join().unwrap();
    h2.join().unwrap();
    info!("Time elapsed: {:?}", now.elapsed());

    Ok(())
}

#[derive(Debug, Eq, PartialEq, Clone, Hash)]
struct RowWithoutConfirm {
    txid: Txid,
    timestamp: u64,
    current_height: u32,
    fee_rate: String,
    fee_rate_bytes: String, // formatted f64 so that struct can derive Eq, PartialEq, Hash
    mempool_buckets: String,
    mempool_len: usize,
    blocks_buckets: String,
    last_block_ts: u32,
}

struct SortedBlockFees {
    pub fees: Vec<f64>,
    pub mean: Option<f64>,
    pub percentiles_csv: String,
    pub percentiles: Vec<f64>,
    pub percentiles_tresh: Vec<u8>,
}

impl SortedBlockFees {
    pub fn new(mut fees: Vec<f64>, perc_csv: &PercentileCSV) -> Self {
        fees.sort_by(|a, b| a.partial_cmp(b).unwrap());
        let mean = if fees.is_empty() {
            None
        } else {
            let sum: f64 = fees.iter().sum();
            let len = fees.len();
            Some(sum / len as f64)
        };
        let percentiles_tresh = perc_csv.thresholds.clone();
        let (percentiles, percentiles_csv) = perc_csv.percentile_csv(&fees);
        SortedBlockFees {
            fees,
            mean,
            percentiles_csv,
            percentiles,
            percentiles_tresh,
        }
    }

    pub fn exclude(&self, confirms_in: u32, fee_rate: f64) -> bool {
        //"1,30,45,55,70,99"
        match confirms_in {
            1 => fee_rate < self.percentiles[2] || fee_rate > self.percentiles[3],
            2 => fee_rate < self.percentiles[1] || fee_rate > self.percentiles[4],
            3 => fee_rate < self.percentiles[0] || fee_rate > self.percentiles[5],
            _ => false,
        }
    }
}

impl Display for SortedBlockFees {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        let len = self.fees.len();
        let padding = 5000 - len;
        for el in self.fees.iter() {
            write!(f, "{:.2},", el)?;
        }
        for _ in 0..padding {
            write!(f, ",")?;
        }
        Ok(())
    }
}

fn percentile(values: &[f64], perc: u8) -> Result<f64> {
    if perc == 0 || perc >= 100 {
        return Err("percentile out of range".into());
    }
    if values.windows(2).any(|a| a[0] > a[1]) {
        return Err("values not sorted".into());
    }
    let len = values.len();
    let index = len * perc as usize / 100;
    let value = values[index];
    Ok(value)
}

struct PercentileCSV {
    empty: String,
    thresholds: Vec<u8>,
    last: u8,
}

impl PercentileCSV {
    fn from_str(str: &str) -> Self {
        let thresholds: Vec<_> = str.split(',').map(|e| e.parse::<u8>().unwrap()).collect();
        let last = *thresholds.last().unwrap();
        let empty: String = std::iter::repeat(",").take(thresholds.len() - 1).collect();
        PercentileCSV {
            thresholds,
            empty,
            last,
        }
    }
    fn percentile_csv(&self, fees: &[f64]) -> (Vec<f64>, String) {
        if fees.is_empty() {
            return (vec![], self.empty.clone());
        }
        let mut result = String::new();
        let mut result_val = vec![];
        for t in self.thresholds.iter() {
            let perc = percentile(&fees, *t).unwrap();
            result_val.push(perc);
            result.push_str(&format!("{:.2}", perc));
            if t != &self.last {
                result.push(',');
            }
        }
        (result_val, result)
    }
}

impl RowWithoutConfirm {
    pub fn new(
        txid: Txid,
        timestamp: u64,
        current_height: u32,
        fee_rate: f64,
        fee_rate_bytes: f64,
        mempool_buckets: String,
        mempool_len: usize,
        blocks_buckets: String,
        last_block_ts: u32,
    ) -> Self {
        RowWithoutConfirm {
            txid,
            timestamp,
            current_height,
            fee_rate: format!("{:.2}", fee_rate),
            fee_rate_bytes: format!("{:.2}", fee_rate_bytes),
            mempool_buckets,
            mempool_len,
            blocks_buckets,
            last_block_ts,
        }
    }
}

impl RowWithoutConfirm {
    fn head(buckets: usize, blocks_buckets: usize, use_mempool: bool) -> String {
        let buckets_str: Vec<_> = (0..buckets).map(|i| format!("a{}", i)).collect();
        let blocks_buckets_str: Vec<_> = (0..blocks_buckets).map(|i| format!("b{}", i)).collect();
        let buckets = if use_mempool {
            buckets_str
        } else {
            blocks_buckets_str
        };
        format!("txid,timestamp,current_height,confirms_in,fee_rate,fee_rate_bytes,core_econ,core_cons,mempool_len,parent_in_cpfp,last_block_ts,{}\n", buckets.join(","))
    }

    fn print(
        &self,
        confirms_at: u32,
        econ: Option<f64>,
        cons: Option<f64>,
        parent_in_cpfp: bool,
        use_mempool: bool,
    ) -> String {
        let mut out = String::new();

        out.push_str(&format!("{},", self.txid));
        out.push_str(&(self.timestamp / 1000).to_string());
        out.push(',');
        out.push_str(&self.current_height.to_string());
        out.push(',');
        out.push_str(&format!("{},", confirms_at - self.current_height));
        out.push_str(&self.fee_rate);
        out.push(',');
        out.push_str(&self.fee_rate_bytes);
        out.push(',');
        out.push_str(&unwrap_or_na(econ.as_ref(), true));
        out.push_str(&unwrap_or_na(cons.as_ref(), true));

        out.push_str(&self.mempool_len.to_string());
        out.push(',');
        out.push_str(&(parent_in_cpfp as u8).to_string());
        out.push(',');
        out.push_str(&self.last_block_ts.to_string());
        out.push(',');

        if use_mempool {
            out.push_str(&self.mempool_buckets);
        } else {
            out.push_str(&self.blocks_buckets);
        }
        out.push('\n');
        out
    }
}

struct EstimateSmartFee(pub BTreeMap<u16, f64>); // confirms in block, satoshi/byte

impl EstimateSmartFee {
    pub fn new(fees: Vec<Fee>) -> Self {
        let tree: BTreeMap<_, _> = fees
            .into_iter()
            .map(|e| (e.0, e.1 as f64 / 1000.0))
            .collect();
        EstimateSmartFee(tree)
    }
    pub fn get(&self, block: u16) -> Option<f64> {
        match self.0.get(&block) {
            Some(val) => Some(*val),
            None => {
                let lower = self.0.range(..block).next_back()?.1;
                let upper = self.0.range(block..).next()?.1;
                Some((lower + upper) / 2.0) // TODO adjusted mean
            }
        }
    }
}

fn unwrap_or_na(data: Option<&f64>, with_coma: bool) -> String {
    let coma = if with_coma { "," } else { "" };
    data.map(|r| format!("{:.2}{}", r, coma))
        .unwrap_or(format!("?{}", coma))
}

fn process(receiver: Receiver<Option<BitcoinLog>>, options: CsvOptions) -> crate::Result<()> {
    let mut last_estimate_economical = None;
    let mut last_estimate_conservative = None;

    let filename_str = options.dataset_file.file_name().unwrap().to_str().unwrap();
    let dataset_file = File::create(&options.dataset_file)?;
    let mut gz = GzBuilder::new()
        .filename(filename_str)
        .write(dataset_file, Compression::default());

    let mut blocks_fee_file = options.fee_file.as_ref().map(|f| File::create(f).unwrap());
    let mut total_rows = 0;
    let mut count_input_not_confirmed = 0;
    let mut counter_block_to_confirm: BTreeMap<u32, u32> = BTreeMap::new();
    let mut mempool = MempoolBuckets::new(
        options.buckets_increment as u32,
        options.buckets_limit as f64,
    );
    let mut mempool_w = MempoolWeightBuckets::new(
        options.buckets_increment as u32,
        options.buckets_limit as f64,
    );
    let mut blocks_bucket = BlocksBuckets::new(
        options.buckets_increment as u32,
        options.buckets_limit as f64,
        options.blocks_buckets_to_consider,
    );
    let mut blocks_times = BlocksTimes::new();
    let mut last_ts = 0;
    let mut blocks_fees: HashMap<_, _> = HashMap::new();
    let perc_csv = PercentileCSV::from_str(&options.percentiles);
    gz.write_all(
        RowWithoutConfirm::head(
            mempool.number_of_buckets(),
            blocks_bucket.number_of_buckets(),
            options.use_mempool,
        )
        .as_bytes(),
    )?;
    info!("mempool buckets {}", mempool.number_of_buckets());
    let mut not_yet_confirmed: Vec<RowWithoutConfirm> = vec![]; //TODO should remove very old elements from here (tx that never confirms)
    while let Some(log) = receiver.recv()? {
        info!(
            "Received log with {} events first ts {}",
            log.events.len(),
            log.events.first().unwrap().0 / 1000
        );
        let BitcoinLog {
            data,
            events,
            name: _,
            version: _,
        } = log;

        let blocks: HashMap<_, _> = data
            .blocks
            .iter()
            .map(|b| deserialize::<Block>(b).unwrap())
            .map(|b| (b.block_hash(), b))
            .collect();

        let mut spent_in_blocks: HashMap<u32, HashSet<OutPoint>> = HashMap::new();

        let mut blocks_height: HashMap<_, _> =
            data.blocks_height.iter().map(|e| (e.0, e.1)).collect();
        let mut txs_map = HashMap::new();
        let txs = Transactions::new(&data, &mut txs_map);

        for (hash, block) in blocks.iter() {
            if blocks_height.get(hash).is_none() {
                warn!("blocks_height for {} is none, probably stale", hash);
                continue;
            }

            let h = *blocks_height.get(hash).unwrap();
            blocks_times.add(h, block);
            let prevouts: HashSet<_> = block
                .txdata
                .iter()
                .map(|tx| tx.input.iter().map(|i| i.previous_output))
                .flatten()
                .collect();
            spent_in_blocks.insert(h, prevouts);
            let block_fees_vec: Vec<_> = block
                .txdata
                .iter()
                .filter_map(|tx| txs.fee_rate(&tx.txid()))
                .collect();
            assert_eq!(
                block_fees_vec.len(),
                block.txdata.len() - 1,
                "can't find all fees for block with hash {}",
                hash
            );

            let block_fees = SortedBlockFees::new(block_fees_vec, &perc_csv);
            if let Some(file) = blocks_fee_file.as_mut() {
                file.write_all(block_fees.to_string().as_bytes()).unwrap();
                file.write_all(b"\n").unwrap();
            }
            blocks_fees.insert(h, block_fees);
        }
        debug!("blocks_fees for heights {:?}", blocks_fees.keys());

        if !not_yet_confirmed.is_empty() {
            info!("start not_yet_confirmed {} txs", not_yet_confirmed.len());
            let now_confirmed: HashSet<_> = not_yet_confirmed
                .iter()
                .filter(|x| txs.height(&x.txid).is_some())
                .cloned()
                .collect();
            let now_confirmed_len = now_confirmed.len();
            let now_confirmed_with_block_fees: HashSet<_> = now_confirmed
                .into_iter()
                .filter(|x| blocks_fees.get(&txs.height(&x.txid).unwrap()).is_some())
                .collect();

            info!(
                "{} now confirmed with block fees ({} confirmed but without block_fees)",
                now_confirmed_with_block_fees.len(),
                now_confirmed_len - now_confirmed_with_block_fees.len()
            );
            total_rows += now_confirmed_with_block_fees.len();
            for conf in now_confirmed_with_block_fees.iter() {
                let tx = txs.get(&conf.txid).unwrap();
                let confirms_at = txs.height(&conf.txid).unwrap(); // safe, just checked
                let block_fees = blocks_fees.get(&confirms_at).unwrap(); // safe, just checked

                let confirms_in = confirms_at - conf.current_height;

                if block_fees.exclude(confirms_in, conf.fee_rate.parse::<f64>().unwrap()) {
                    // exclude rows when confirmations = 1,2,3 with percentiles
                    continue;
                }

                *counter_block_to_confirm.entry(confirms_in).or_default() += 1;
                let spent_in_block = spent_in_blocks.get(&confirms_at).unwrap();
                let parent_in_cpfp = has_output_spent_in_block(tx, spent_in_block);
                gz.write_all(
                    conf.print(confirms_at, None, None, parent_in_cpfp, options.use_mempool)
                        .as_bytes(),
                )?;
            }
            not_yet_confirmed.retain(|e| !now_confirmed_with_block_fees.contains(&e));
            info!("end not_yet_confirmed {} txs", not_yet_confirmed.len());
        }

        let mut count_tx_added = 0;
        let mut count_this_batch_confirmed = 0;
        let mut block_found = 0;

        let mut block_hashes = Vec::new();

        for (i, event) in events.iter().enumerate() {
            if i == 0 {
                assert!(
                    last_ts <= event.0,
                    "last_ts:{} event.0:{}",
                    last_ts,
                    event.0
                );
                last_ts = event.0;
            }
            match event.1 {
                EventType::Start => {
                    // reset everything, because raw data are not continuous
                    warn!("Fount Start event! {}", event.0 / 1000);
                    mempool.clear();
                    not_yet_confirmed.clear();
                }
                EventType::EstimateSmartFeesEconomical => {
                    let fees: Vec<Fee> = serde_cbor::from_slice(&event.2).unwrap();
                    last_estimate_economical = Some(EstimateSmartFee::new(fees));
                }
                EventType::EstimateSmartFeesConservative => {
                    let fees: Vec<Fee> = serde_cbor::from_slice(&event.2).unwrap();
                    last_estimate_conservative = Some(EstimateSmartFee::new(fees));
                }
                EventType::HashHeight => {
                    let hash_height: HashHeight = serde_cbor::from_slice(&event.2).unwrap();
                    blocks_height.insert(hash_height.0, hash_height.1);
                    block_hashes.push(hash_height.0)
                }
                EventType::Sequence => match parse_sequence(&event.2) {
                    Ok(Sequence::TxAdded(txid, _)) => {
                        count_tx_added += 1;

                        if let Some(tx) = txs.get(&txid) {
                            let last = block_hashes.last().unwrap();
                            let current_height = match blocks_height.get(last) {
                                Some(height) => *height,
                                None => {
                                    warn!("can't found last {}", last);
                                    continue;
                                }
                            };
                            let input_confirmed_heights: Vec<_> = tx
                                .input
                                .iter()
                                .map(|i| {
                                    txs.height(&i.previous_output.txid)
                                        .unwrap_or(u32::max_value())
                                })
                                .collect();
                            let input_confirmed =
                                input_confirmed_heights.iter().all(|i| *i <= current_height);
                            if !input_confirmed {
                                // tx with inputs not confirmed should be checked as a whole
                                // eg (tx1_fee + tx2_fee) / (tx1_weight + tx2_weight)
                                // not doing for now
                                // TODO check later on if inputs confirms, but this tx not
                                count_input_not_confirmed += 1;
                                continue;
                            }

                            let (fee_rate, fee_rate_bytes) =
                                match (txs.fee_rate(&txid), txs.fee_rate_bytes(&txid)) {
                                    (Some(fee_rate), Some(fee_rate_bytes)) => {
                                        (fee_rate, fee_rate_bytes)
                                    }
                                    _ => {
                                        warn!("fee_rate not found for {}", txid);
                                        continue;
                                    }
                                };
                            mempool_w.add(txid, fee_rate, tx.get_weight() as u64);

                            let input_confirmed_last_x = input_confirmed_heights.iter().all(|i| {
                                let limit =
                                    current_height.saturating_sub(options.blocks_to_consider);
                                *i <= current_height && *i > limit
                            });
                            if input_confirmed_last_x {
                                // light client will download only the last X blocks,
                                // it will be able to compute only this fee rates
                                mempool.add(txid, fee_rate);
                            }
                            // but I can use every seen tx as train data

                            let (mempool_len, mempool_buckets) = if options.mempool_weight {
                                (mempool_w.len(), mempool_w.buckets_str())
                            } else {
                                (mempool.len(), mempool.buckets_str())
                            };

                            let row = RowWithoutConfirm::new(
                                txid,
                                event.0,
                                current_height,
                                fee_rate,
                                fee_rate_bytes,
                                mempool_buckets,
                                mempool_len,
                                blocks_bucket.get_buckets().to_string(),
                                blocks_times.time(current_height),
                            );

                            let confirms_at = match txs.height(&txid) {
                                Some(confirms_at) => confirms_at,
                                None => {
                                    not_yet_confirmed.push(row);
                                    continue;
                                }
                            };
                            let block_fees = match blocks_fees.get(&confirms_at) {
                                Some(block_fees) => block_fees,
                                None => {
                                    not_yet_confirmed.push(row);
                                    continue;
                                }
                            };

                            let confirms_in = confirms_at - current_height;

                            if block_fees.exclude(confirms_in, fee_rate) {
                                // exclude rows when confirmations = 1,2,3 with percentiles
                                continue;
                            }

                            *counter_block_to_confirm.entry(confirms_in).or_default() += 1;

                            let core_fee_rate_economical = last_estimate_economical
                                .as_ref()
                                .and_then(|e| e.get(confirms_in as u16));
                            let core_fee_rate_conservative = last_estimate_conservative
                                .as_ref()
                                .and_then(|e| e.get(confirms_in as u16));
                            let spent_in_block = spent_in_blocks.get(&confirms_at).unwrap();
                            let parent_in_cpfp = has_output_spent_in_block(tx, spent_in_block);
                            gz.write_all(
                                row.print(
                                    confirms_at,
                                    core_fee_rate_economical,
                                    core_fee_rate_conservative,
                                    parent_in_cpfp,
                                    options.use_mempool,
                                )
                                .as_bytes(),
                            )?;

                            count_this_batch_confirmed += 1;
                        }
                    }
                    Ok(Sequence::TxRemoved(txid, _)) => {
                        mempool.remove(&txid);
                    }
                    Ok(Sequence::BlockConnected(block_hash)) => {
                        debug!("block {} connected", block_hash);
                        let block = blocks.get(&block_hash).unwrap_or_else(|| {
                            panic!("missing {}, need to be corrected", block_hash)
                        });
                        blocks_bucket.add(block.clone());
                        for tx in block.txdata.iter() {
                            let txid = tx.txid();
                            mempool.remove(&txid);
                        }
                        block_hashes.push(block_hash);
                        block_found += 1;

                        // highly likely a swing in node estimation after a block event, better to not consider until a new estimate is requested
                        last_estimate_economical = None;
                        last_estimate_conservative = None;
                    }
                    Ok(Sequence::BlockDisconnected(block_hash)) => {
                        block_hashes.pop();
                        blocks_bucket.remove();
                        warn!("block {} disconnected", block_hash);
                    }
                    Err(e) => warn!("cannot parse sequence {:?}", e),
                },
                EventType::RawMempool => {
                    let raw_mempool: Vec<Txid> = serde_cbor::from_slice(&event.2).unwrap();
                    let raw_mempool_set = HashSet::from_iter(raw_mempool.iter());
                    let my_mempool_set = mempool.txids_set();
                    let intersection = raw_mempool_set.intersection(&my_mempool_set).count();
                    let only_rawmempool = raw_mempool_set.difference(&my_mempool_set).count();
                    info!(
                        "rawmempool:{}, my:{} intersect:{} only_rawmempool:{}",
                        raw_mempool_set.len(),
                        my_mempool_set.len(),
                        intersection,
                        only_rawmempool
                    );
                    let only_mine: HashSet<_> =
                        my_mempool_set.difference(&raw_mempool_set).collect();
                    if !only_mine.is_empty() {
                        warn!("only mine total tx: {:?}", only_mine.len());
                        trace!("only mine tx: {:?}", only_mine);
                    }
                }
                EventType::InitialRawMempool => {
                    info!("InitialRawMempool initialization!");
                    let raw_mempool: Vec<Txid> = serde_cbor::from_slice(&event.2).unwrap();
                    for txid in raw_mempool {
                        if let Some(fee_rate) = txs.fee_rate(&txid) {
                            mempool.add(txid, fee_rate);
                            mempool_w.add(
                                txid,
                                fee_rate,
                                txs.get(&txid).unwrap().get_weight() as u64,
                            );
                        }
                    }
                }
                _ => (),
            }
        }
        total_rows += count_this_batch_confirmed;
        info!("{} txs added to mempool", count_tx_added);
        info!("{} txs confirmed in this batch", count_this_batch_confirmed);
        info!("{} blocks", block_found);
        info!("-----------------------")
    }
    debug!("counter_block_to_confirm {:?}", counter_block_to_confirm);
    info!("count_input_not_confirmed {}", count_input_not_confirmed);
    info!("total {}", total_rows);
    Ok(())
}

fn has_output_spent_in_block(tx: &Transaction, spent_in_block: &HashSet<OutPoint>) -> bool {
    let txid = tx.txid();
    for i in 0..tx.output.len() as u32 {
        if spent_in_block.contains(&OutPoint::new(txid, i)) {
            return true;
        }
    }
    false
}

/// Read directory content and return a lexicographically ordered list of files path
fn read_dir(path: &PathBuf) -> Result<Vec<PathBuf>> {
    if !path.exists() {
        panic!("load-path dir doesn't exist");
    }
    let mut result = vec![];
    if path.is_dir() {
        for entry in fs::read_dir(path)? {
            let file = entry?.path();

            if file.is_file()
                && file.extension().is_some()
                && file.extension().unwrap() == "bitcoin_log"
            {
                result.push(file);
            }
        }
    }
    result.sort();
    Ok(result)
}

#[cfg(test)]
mod tests {
    use crate::percentile;

    #[test]
    fn test_percentiles() {
        for i in [50, 99, 1999, 2000, 2001].iter() {
            let vec: Vec<_> = (0..*i).map(|i| i as f64).collect();
            assert_eq!(vec.len(), *i);
            for i in 1..99u8 {
                let perc = percentile(&vec, i).unwrap();
                let expected_els = vec.len() * i as usize / 100;
                let lower = vec.iter().filter(|e| e < &&perc).count();
                assert_eq!(expected_els, lower);
                let upper = vec.iter().filter(|e| e >= &&perc).count();
                assert_eq!(vec.len() - expected_els, upper);
            }
        }
    }
}
