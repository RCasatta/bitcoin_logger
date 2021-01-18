use bitcoincore_rpc::{Auth, Client};
use std::path::PathBuf;
use structopt::StructOpt;

pub mod flush;
pub mod rpc;
pub mod store;
pub mod zmq;

// Graphviz high-level scheme
// https://dreampuf.github.io/GraphvizOnline/#digraph%20G%20%7B%0A%20%20subgraph%20cluster_logger%20%7B%0A%20%20%20%20label%20%3D%20%22bitcoin-logger%22%3B%0A%20%20%20%20style%3Dfilled%3B%0A%20%20%20%20color%3Dlightgrey%3B%0A%20%20%20%20node%20%5Bstyle%3Dfilled%2Ccolor%3Dwhite%5D%3B%0A%20%20%20%20%0A%20%20%20%20store%0A%20%20%20%20zmq_reader%0A%20%20%20%20flush%0A%20%20%20%20rpc_call%0A%20%20%20%20zmq_reader%20-%3E%20store%0A%20%20%20%20rpc_call%20-%3E%20store%0A%20%20%20%20store%20-%3E%20flush%0A%20%20%7D%0A%20%20%0A%20%20subgraph%20cluster_bitcoind%20%7B%0A%20%20%20%20label%20%3D%20%22bitcoind%22%3B%0A%20%20%20%20style%3Dfilled%3B%0A%20%20%20%20color%3Dlightgrey%3B%0A%20%20%20%20node%20%5Bstyle%3Dfilled%2Ccolor%3Dwhite%5D%3B%0A%20%20%20%20zmq%3B%0A%20%20%20%20rpc%3B%0A%20%20%7D%0A%0A%20%20subgraph%20cluster_csv%20%7B%0A%20%20%20%20label%20%3D%20%22bitcoin-csv%22%3B%0A%20%20%20%20style%3Dfilled%3B%0A%20%20%20%20color%3Dlightgrey%3B%0A%20%20%20%20node%20%5Bstyle%3Dfilled%2Ccolor%3Dwhite%5D%3B%0A%20%20%20%20raw_logs_reader%0A%20%20%20%20csv_writer%0A%20%20%7D%0A%20%20%0A%20%20raw_logs%20%5Bshape%3DSquare%5D%3B%0A%20%20csv%20%5Bshape%3DSquare%5D%3B%0A%20%20zmq%20-%3E%20zmq_reader%20%5B%20label%3D%22rawtx%2C%20rawblock%2C%20sequence%22%20%5D%0A%20%20store%20-%3E%20rpc%20%5B%20label%3D%22missing%20data%22%20%5D%0A%20%20rpc_call%20-%3E%20rpc%20%5B%20label%3D%22estimatesmartfee%22%20%5D%0A%20%20flush%20-%3E%20raw_logs%0A%20%20raw_logs%20-%3E%20raw_logs_reader%0A%20%20csv_writer%20-%3E%20csv%0A%7D

pub type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;
pub const NAME: &str = "BitcoinLog";

#[derive(StructOpt, Debug)]
pub struct LoggerOptions {
    /// ZMQ address eg. tcp://127.0.0.1:28332
    #[structopt(long)]
    pub zmq_address: String,

    #[structopt(flatten)]
    pub node_config: NodeConfig,

    /// Maximum number of elements per flushed file
    #[structopt(long, default_value = "200000")]
    pub elements: usize,

    /// Directory where files are saved
    #[structopt(long)]
    pub save_path: PathBuf,

    /// Save estimatesmartfee from the node every this seconds
    #[structopt(long, default_value = "10")]
    pub estimatesmartfee_every: u64,

    /// Save rawmempool from the node every this seconds (11 hours)
    #[structopt(long, default_value = "39600")]
    pub getrawmempool_every: u64,
}

#[derive(StructOpt, Debug)]
pub struct NodeConfig {
    /// Rpc address eg. "http://127.0.0.1:18332"
    #[structopt(long)]
    pub rpc_address: String,

    /// Path of the bitcoin cookie file
    #[structopt(long)]
    pub cookie_path: String,
}

#[derive(StructOpt, Debug)]
pub struct CsvOptions {
    /// Directory where logger files are stored
    #[structopt(long)]
    pub load_path: PathBuf,

    /// File name of the output dataset
    #[structopt(long)]
    pub dataset_file: PathBuf,

    /// File name of the file where to print block fee rates
    #[structopt(long)]
    pub fee_file: Option<PathBuf>,

    /// Raw log files are compressed and serialized as cbor, this parameter controls how many
    /// files are processed in parallel
    #[structopt(long, default_value = "4")]
    pub concurrency: u8,

    /// A mempool bucket count how many tx in the mempool are between a lower and upper limits `[sat/vb]`
    /// Lower limit start with 1.0 and the upper limit incremented by this percentage.{n}
    /// Eg. with default 50% first buckets are:{n}
    ///  a0: 1.0-1.5{n}
    ///  a1: 1.5-2.25{n}
    ///  a2: 2.25-3.375{n}
    #[structopt(long, default_value = "50")]
    pub buckets_increment: u8,

    /// Buckets are calculated until lower limit is under this value.
    /// With defaults `buckets_increment=50` and `buckets_limit=500`, 16 buckets are created and the last is
    ///  a15: 437.89-inf
    #[structopt(long, default_value = "500")]
    pub buckets_limit: u16,

    /// A Bitcoin transaction doesn't contain its fee, to compute the fee rate tx referenced in its
    /// input are required. To build mempool buckets calculating the fee rate is required but a
    /// light client can't access any tx, so it's considering only mempool txs whose inputs are in
    /// the last `block_to_consider` blocks, which, due to temporal locality are an unexpected high number
    #[structopt(long, default_value = "6")]
    pub blocks_to_consider: u32,

    /// Percentile fee_rate of the block where tx considered is confirmed
    /// eg q01 for block x contains a fee rate such as:
    /// for every tx in block x:
    ///  if fee_rate(tx) < q01 # matches the 1% txs of the block with lower fee rate
    #[structopt(long, default_value = "1,30,45,55,70,99")]
    pub percentiles: String,
}

#[derive(StructOpt, Debug)]
pub struct ConvertOptions {
    /// File name of the `bitcoin_log` file
    #[structopt(long)]
    pub bitcoin_log: PathBuf,

    /// Directory where logger files converted are written
    #[structopt(long)]
    pub converted_path: PathBuf,

    #[structopt(flatten)]
    pub node_config: NodeConfig,
}

impl NodeConfig {
    pub fn make_rpc_client(&self) -> Result<Client> {
        let auth = Auth::CookieFile((&self.cookie_path).into());
        let url = self.rpc_address.to_string();
        Ok(Client::new(url, auth)?)
    }
}
