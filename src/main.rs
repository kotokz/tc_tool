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
mod tests;

use std::thread;
use tclogparser::*;
use tradecache::*;
use tcresult::*;

fn main() {

    println!("Name, duration, lastSampleTime, Done, lastMsgTimeStamp, Efficiency(per min), Delay");

    let monitors: Vec<_> = vec![
        new_ng_publisher(),
        new_ng_consumer(),
        new_v1_publisher(),
        new_ng_trimmer(),
    ];

    // let mut v: Vec<Box<TcProcesser>>= Vec::new();
    // v.push(Box::new(new_ng_publisher()));

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