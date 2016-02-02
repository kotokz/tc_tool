#![feature(slice_patterns, test, plugin)]
#![plugin(clippy)]

extern crate regex;
extern crate glob;
extern crate time;
extern crate clap;
extern crate fnv;
extern crate test;

mod tc;
mod logresult;
mod logparser;
mod error;
mod logstat;

use clap::{Arg, App};
use std::thread;
use tc::*;

fn main() {

    let matches = App::new("Tc Stat tool")
                      .version("1.0")
                      .author("tz")
                      .about("Collect hour/batch statitic base on log files")
                      .arg(Arg::with_name("CONFIG")
                               .short("t")
                               .long("tc")
                               .help("Sets a log type")
                               .takes_value(true))
                      .arg(Arg::with_name("debug")
                               .short("d")
                               .long("debug")
                               .help("Sets debug mode"))
                      .get_matches();


    let prod = match matches.occurrences_of("debug") {
        0 => true,
        _ => false,
    };

    println!("Name, lastSampleTime, Total(Batch size), Done, lastMsgTimeStamp, Efficiency(per \
              min), Delay");

    let monitors: Vec<_> = match matches.value_of("CONFIG").unwrap_or("hour") {
        "hour" => vec![TcTool::new_hour(6, prod), TcTool::new_batch(6, prod)],
        _ => panic!("Missing config"),
    };

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
