use bitcoin_logger::store::{self, EventOrData, EventType};
use bitcoin_logger::{flush, rpc, zmq, LoggerOptions};
use bitcoincore_rpc::RpcApi;
use log::{info, warn};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::channel;
use std::sync::Arc;
use std::thread;
use structopt::StructOpt;

fn main() {
    env_logger::init_from_env(
        env_logger::Env::default().filter_or(env_logger::DEFAULT_FILTER_ENV, "info"),
    );
    info!("start");
    let options = Arc::new(LoggerOptions::from_args());

    let (to_flush, flush_receiver) = channel();
    let (to_store, store_receiver) = channel();
    let running = Arc::new(AtomicBool::new(true));

    let mut initial_flush = vec![];
    initial_flush.push(EventOrData::event(EventType::Start, vec![]));
    let client = options.node_config.make_rpc_client().unwrap();
    info!("asking initial raw mempool");
    let txs = client.get_raw_mempool().unwrap();
    let event = EventOrData::event(
        EventType::InitialRawMempool,
        serde_cbor::to_vec(&txs).unwrap(),
    );
    initial_flush.push(event);
    to_flush.send(Some(initial_flush)).unwrap();
    info!("initial raw mempool sent to flush");

    let options_clone = options.clone();
    let store_handle =
        thread::spawn(
            move || match store::start(&options_clone, store_receiver, to_flush) {
                Ok(_) => info!("store::start returned Ok"),
                Err(e) => panic!("store::start returned Err({})", e),
            },
        );

    let options_clone = options.clone();
    let flush_handle = thread::spawn(move || match flush::start(&options_clone, flush_receiver) {
        Ok(_) => info!("flush::start returned Ok"),
        Err(e) => panic!("flush::start returned Err({})", e),
    });

    let to_store_clone = to_store.clone();
    let running_clone = running.clone();
    let options_clone = options.clone();
    let zmq_handle =
        thread::spawn(
            move || match zmq::start(&options_clone, to_store_clone, running_clone) {
                Ok(_) => info!("zmq::start returned Ok"),
                Err(e) => panic!("zmq::start returned Err({})", e),
            },
        );

    let running_clone = running.clone();
    let to_store_clone = to_store.clone();
    let rpc_handle =
        thread::spawn(
            move || match rpc::start(&options, to_store_clone, running_clone) {
                Ok(_) => info!("rpc::start returned Ok"),
                Err(e) => panic!("rpc::start returned Err({})", e),
            },
        );

    ctrlc::set_handler(move || {
        warn!("ctrl-c hit, ending threads");
        running.store(false, Ordering::SeqCst);
    })
    .expect("Error setting Ctrl-C handler");

    zmq_handle.join().expect("cannot join zmq");
    rpc_handle.join().expect("cannot join rpc");
    to_store
        .send(Some(EventOrData::event(EventType::End, vec![])))
        .expect("receiver closed");
    to_store.send(None).expect("receiver closed");
    store_handle.join().expect("cannot join store");
    flush_handle.join().expect("cannot join flush");
}
