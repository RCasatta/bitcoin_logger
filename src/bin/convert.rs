use bitcoin::consensus::{deserialize, serialize};
use bitcoin::Block;
use bitcoin_logger::flush::{parse_sequence, write_log, Sequence};
use bitcoin_logger::store::{read_log, EventType};
use bitcoin_logger::ConvertOptions;
use bitcoin_logger::Result;
use bitcoincore_rpc::RpcApi;
use log::{error, info, warn};
use std::collections::HashMap;
use structopt::StructOpt;

fn main() -> Result<()> {
    env_logger::init_from_env(
        env_logger::Env::default().filter_or(env_logger::DEFAULT_FILTER_ENV, "info"),
    );
    let options: ConvertOptions = ConvertOptions::from_args();
    info!("{:?}", options);

    if !options.converted_path.is_dir() || !options.converted_path.exists() {
        warn!("--converted-path must exist and be a directory");
        return Ok(());
    }

    let mut path = options.converted_path.clone();
    path.push(&options.bitcoin_log.file_name().unwrap());
    if path.exists() {
        error!("Destination file already exist {:?}", path);
        return Ok(());
    }
    let client = options.node_config.make_rpc_client()?;
    info!(
        "connected to bitcoind node version: {}",
        client.get_network_info()?.subversion
    );

    let mut log = read_log(&options.bitcoin_log)?;
    let blocks: HashMap<_, _> = log
        .data
        .blocks
        .iter()
        .map(|b| deserialize::<Block>(b).unwrap())
        .map(|b| (b.block_hash(), b))
        .collect();
    let mut changed = false;
    for event in log.events.iter() {
        if event.1 == EventType::Sequence {
            if let Ok(Sequence::BlockConnected(hash)) = parse_sequence(&event.2) {
                if !blocks.contains_key(&hash) {
                    info!("Doesn't contain {}", hash);
                    let block = client.get_block(&hash)?;
                    let block_bytes = serialize(&block);
                    log.data.blocks.push(block_bytes);
                    changed = true;
                }
            }
        }
    }
    if changed {
        write_log(&log, &path)?;
        info!("written {:?}", &path);
    } else {
        info!("not changed");
    }

    Ok(())
}
