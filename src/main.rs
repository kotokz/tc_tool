#![feature(slice_patterns, test, plugin)]
#![plugin(clippy)]

extern crate regex;
extern crate glob;
extern crate time;
#[macro_use]
extern crate lazy_static;
extern crate test;

mod tradecache;
mod tcresult;
mod tclogparser;
mod tcerror;
mod tcstat;

use std::thread;
use tradecache::*;

fn main() {

    println!("Name, lastSampleTime, Total(Batch size), Done, lastMsgTimeStamp, Efficiency(per \
              min), Delay");

    let monitors: Vec<_> = vec![
        TcTool::new_ng_publisher(6, false),
        TcTool::new_ng_consumer(6, false),
        TcTool::new_ng_trimmer(6, false),
        TcTool::new_ng_trimmer_batch(1, false),
        TcTool::new_v1_trimmer(1, false),
        TcTool::new_v1_publisher(6, false),
    ];

    let handlers: Vec<_> = monitors.into_iter()
                                   .map(|mut tc| {
                                       thread::spawn(move || {
                                           tc.process_directory();
                                           tc.print_result();
                                       })
                                   })
                                   .collect();

    for h in handlers {
        h.join().unwrap();
    }

}
