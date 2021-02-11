use crate::buckets::create_buckets_limits;
use crate::store::Transactions;
use bitcoin::Block;
use std::collections::{HashMap, VecDeque};

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

    pub fn last_non_empty_ts(&self) -> Option<u32> {
        for b in self.last_blocks.iter().rev() {
            if b.txdata.len() > 1 {
                return Some(b.header.time);
            }
        }
        return None;
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


#[cfg(test)]
mod tests {
    use bitcoin::blockdata::constants::genesis_block;
    use bitcoin::{Network, Transaction};
    use crate::buckets::blocks::BlocksBuckets;
    use bitcoin::consensus::deserialize;
    use bitcoin::hashes::hex::FromHex;
    use crate::store::now;

    #[test]
    fn test_block_buckets() {
        let b1 = genesis_block(Network::Bitcoin);
        let mut b2 = b1.clone();

        assert_eq!(1231006505, b1.header.time);
        let mut bb = BlocksBuckets::new(50,100.0,1);
        bb.add(b1);
        assert_eq!(bb.last_non_empty_ts(), None, "empty blocks is being considered");

        let bytes = Vec::<u8>::from_hex("0200000001aad73931018bd25f84ae400b68848be09db706eac2ac18298babee71ab656f8b0000000048473044022058f6fc7c6a33e1b31548d481c826c015bd30135aad42cd67790dab66d2ad243b02204a1ced2604c6735b6393e5b41691dd78b00f0c5942fb9f751856faa938157dba01feffffff0280f0fa020000000017a9140fb9463421696b82c833af241c78c17ddbde493487d0f20a270100000017a91429ca74f8a08f81999428185c97b5d852e4063f618765000000").unwrap();
        let tx1: Transaction = deserialize(&bytes).unwrap();
        b2.txdata.push(tx1);
        let mut b3 = b2.clone();
        bb.add(b2);
        assert_eq!(bb.last_non_empty_ts(), Some(1231006505000));
        let now = now() as u32 / 1000;
        b3.header.time= now;
        bb.add(b3);
        assert_eq!(bb.last_non_empty_ts(), Some(now as u64 * 1000u64));
    }
}