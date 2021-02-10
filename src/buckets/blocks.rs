use crate::buckets::create_buckets_limits;
use crate::store::Transactions;
use bitcoin::Block;
use log::debug;
use std::collections::{HashMap, VecDeque};

pub struct BlocksBuckets {
    last_blocks: VecDeque<Block>, //TODO may use (height, block) and check last 10
    buckets: Option<String>,
    buckets_na: String,
    buckets_limits: Vec<f64>,
    blocks_to_consider: usize,
}

impl BlocksBuckets {
    pub fn new(increment_percent: u32, upper_limit: f64, blocks_to_consider: usize) -> Self {
        let buckets_limits = create_buckets_limits(increment_percent, upper_limit);
        let buckets_na: Vec<_> = buckets_limits.iter().map(|_| "?").collect();
        Self {
            last_blocks: VecDeque::new(),
            buckets: None,
            buckets_na: buckets_na.join(","),
            buckets_limits,
            blocks_to_consider,
        }
    }

    fn full(&self) -> bool {
        self.blocks_to_consider == self.last_blocks.len()
    }

    pub fn add(&mut self, block: Block) {
        if self.full() {
            self.last_blocks.pop_back();
        }
        self.last_blocks.push_front(block);
        if self.full() {
            debug!("full, calculating blocks buckets");
            let mut map = HashMap::new();
            for b in self.last_blocks.iter() {
                for tx in b.txdata.iter() {
                    map.insert(tx.txid(), tx.clone());
                }
            }
            let txs = Transactions::from_txs(map);
            let rates = txs.fee_rates();
            let mut buckets = vec![0u64; self.buckets_limits.len()];
            for rate in rates {
                let index = self
                    .buckets_limits
                    .iter()
                    .position(|e| e > &rate)
                    .unwrap_or(self.buckets_limits.len() - 1);
                buckets[index] += 1;
            }
            let buckets_str: Vec<_> = buckets.iter().map(|e| e.to_string()).collect();
            self.buckets = Some(buckets_str.join(","));
        }
    }

    //TODO should remove by hash
    pub fn remove(&mut self) {
        self.last_blocks.pop_back();
        self.buckets = None;
    }

    pub fn get_buckets(&self) -> &String {
        self.buckets.as_ref().unwrap_or(&self.buckets_na)
    }

    pub fn number_of_buckets(&self) -> usize {
        self.buckets_limits.len()
    }
}
