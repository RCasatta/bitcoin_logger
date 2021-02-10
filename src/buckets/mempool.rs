use std::collections::{HashMap, HashSet};
use bitcoin::Txid;
use std::iter::FromIterator;
use crate::buckets::create_buckets_limits;

pub struct MempoolWeightBuckets {
    /// contain the total weight for this bucket
    buckets_weights: Vec<u64>,
    /// contain the fee rate limits for every bucket ith
    buckets_limits: Vec<f64>,
    /// in which bucket the Txid is in
    tx_bucket: HashMap<Txid, TxBucket>,
}

pub struct TxBucket {
    index: usize,
    weight: u64,
}

pub struct MempoolBuckets {
    /// contain the number of elements for bucket ith
    buckets: Vec<u64>,
    /// contain the fee rate limits for every bucket ith
    buckets_limits: Vec<f64>,
    /// in which bucket the Txid is in
    tx_bucket: HashMap<Txid, usize>,
}

impl MempoolBuckets {
    pub fn new(increment_percent: u32, upper_limit: f64) -> Self {
        let (buckets_limits, buckets) = create_buckets(increment_percent, upper_limit);

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

impl MempoolWeightBuckets {
    pub fn new(increment_percent: u32, upper_limit: f64) -> Self {
        let (buckets_limits, buckets_weights) = create_buckets(increment_percent, upper_limit);

        MempoolWeightBuckets {
            buckets_weights,
            buckets_limits,
            tx_bucket: HashMap::new(),
        }
    }

    pub fn clear(&mut self) {
        self.tx_bucket.clear();
        for el in self.buckets_weights.iter_mut() {
            *el = 0;
        }
    }

    pub fn add(&mut self, txid: Txid, rate: f64, weight: u64) {
        if rate > 1.0 && self.tx_bucket.get(&txid).is_none() {
            // TODO MempoolBuckets use array of indexes to avoid many comparisons?
            let index = self
                .buckets_limits
                .iter()
                .position(|e| e > &rate)
                .unwrap_or(self.buckets_limits.len() - 1);
            self.buckets_weights[index] += weight;
            self.tx_bucket.insert(txid, TxBucket { index, weight });
        }
    }

    pub fn remove(&mut self, txid: &Txid) {
        if let Some(bucket) = self.tx_bucket.remove(txid) {
            self.buckets_weights[bucket.index] -= bucket.weight;
        }
    }

    pub fn number_of_buckets(&self) -> usize {
        self.buckets_weights.len()
    }

    /// returns vbytes
    pub fn buckets_str(&self) -> String {
        self.buckets_weights
            .iter()
            .map(|e| (e / 4).to_string())
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
fn create_buckets(increment_percent: u32, upper_limit: f64) -> (Vec<f64>,Vec<u64>) {
    let buckets_limits = create_buckets_limits(increment_percent, upper_limit);
    let buckets = vec![0u64; buckets_limits.len()];
    (buckets_limits, buckets)
}


#[cfg(test)]
mod tests {
    use bitcoin::Txid;
    use crate::buckets::mempool::MempoolBuckets;

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

}