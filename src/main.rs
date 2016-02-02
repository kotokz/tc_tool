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
                      .about("Collect hour/batch statitic base on TC log files")
                      .arg(Arg::with_name("CONFIG")
                               .short("t")
                               .long("tc")
                               .help("Sets a TC instance, can only be V1, NG or Summit")
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

    let monitors: Vec<_> = match matches.value_of("CONFIG").unwrap_or("NG") {
        "V1" => {
            vec![TcTool::new_v1_trimmer(1, prod),
                 TcTool::new_v1_publisher(6, prod),
                 TcTool::new_v1_tradecache(6, prod)]
        }
        "NG" => {
            vec![TcTool::new_ng_publisher(6, prod),
                 TcTool::new_ng_consumer(6, prod),
                 TcTool::new_ng_trimmer(6, prod),
                 TcTool::new_ng_trimmer_batch(1, prod)]
        }
        "XDS" => vec![TcTool::new_xds(6, prod)],
        "Summit" => {
            vec![TcTool::new_summit_consumer(6, prod),
                 TcTool::new_summit_trimmer(6, prod),
                 TcTool::new_summit_trimmer_batch(2, prod),
                 TcTool::new_summit_publisher(6, prod)]
        }
        _ => panic!("TC config can only be V1, NG or Summit"),
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
