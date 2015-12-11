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

use std::thread;
use tradecache::*;

fn main() {

    println!("Name, duration, lastSampleTime, Done, lastMsgTimeStamp, Efficiency(per min), Delay");

    let monitors: Vec<_> = vec![
        TcTool::new_ng_publisher(),
        TcTool::new_ng_consumer(),
        TcTool::new_v1_publisher(),
        TcTool::new_ng_trimmer(),
    ];

    let handlers: Vec<_> = monitors.into_iter()
                                   .map(|mut tc| {
                                       thread::spawn(move || {
                                           tc.process_directory(6);
                                           tc.print_result();
                                       })
                                   })
                                   .collect();

    for h in handlers {
        h.join().unwrap();
    }

}
