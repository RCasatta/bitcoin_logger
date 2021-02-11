use crate::buckets::create_buckets_limits;
use crate::store::Transactions;
use bitcoin::Block;
use std::collections::{HashMap, VecDeque};
use std::num::NonZeroU32;

#[derive(Debug)]
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

pub struct BlocksTimes(HashMap<u32, Option<NonZeroU32>>); //height, block time if non empty

impl BlocksTimes {
    pub fn new() -> Self {
        Self(HashMap::new())
    }
    pub fn add(&mut self, height: u32, block: &Block) {
        let val = if block.txdata.len() > 1 {
            NonZeroU32::new(block.header.time)
        } else {
            None
        };
        self.0.insert(height, val);
    }

    /// returns block time if available, if not check previous height up to 10
    pub fn time(&self, height: u32) -> u32 {
        let min = height.saturating_sub(10);
        for i in (min..=height).rev() {
            if let Some(Some(val)) = self.0.get(&i) {
                return val.get();
            }
        }
        0
    }
}

#[cfg(test)]
mod tests {
    use crate::buckets::blocks::BlocksTimes;
    use bitcoin::blockdata::constants::genesis_block;
    use bitcoin::Network;

    #[test]
    fn test_block_times() {
        let mut bt = BlocksTimes::new();
        assert_eq!(bt.time(0), 0, "giving time even if there aren't");
        let b1 = genesis_block(Network::Bitcoin);
        bt.add(1, &b1);
        assert_eq!(bt.time(1), 0, "considering empty block");
        let mut b2 = b1.clone();
        b2.txdata.push(b1.txdata[0].clone());
        b2.header.time = 2;
        bt.add(2, &b2);
        assert_eq!(bt.time(2), 2, "getting wrong time");
        let mut b3 = b1.clone();
        b3.header.time = 3;
        bt.add(3, &b3);
        assert_eq!(bt.time(3), 2, "getting wrong time because last is empty");
        let mut b4 = b2.clone();
        b4.header.time = 4;
        bt.add(4, &b4);
        assert_eq!(bt.time(4), 4, "getting non last time");
    }
}
