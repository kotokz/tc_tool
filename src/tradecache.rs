use glob::glob;
use std::io::{BufReader, BufRead};
use std::path::PathBuf;
use std::fs::File;

use tclogparser::*;
use tcresult::*;
use regex::Regex;
use tcerror::TcError;

pub fn new_ng_publisher() -> TcTool {
    TcTool {
        name: "NG_Publisher".to_owned(),
        path: "C:/working/projects/nimproj/logs/ng/publisher/publish.log*".to_owned(),
        pattern: LogParserEnum::Regex(RegexParser(Some(Regex::new(r"docWriteTime=([^}]+?)}")
                                                           .unwrap()))),
        result: TcResultEnum::HourResult(TcHourResult::new()),
    }
}
pub fn new_ng_consumer() -> TcTool {
    TcTool {
        name: "NG_Consumer".to_owned(),
        path: "C:/working/projects/nimproj/logs/ng/consumer/consumer.log*".to_owned(),
        // path: "E:/TradeCache/SophisConsumer-release/logs/prod/consumer.log*".to_owned(),
        pattern: LogParserEnum::Regex(RegexParser(Some(Regex::new(r"timestamp=(.{28})eve")
                                                           .unwrap()))),
        result: TcResultEnum::HourResult(TcHourResult::new()),
    }
}

pub fn new_ng_trimmer() -> TcTool {
    TcTool {
        name: "NG_Trimmer".to_owned(),
        path: "C:/working/projects/nimproj/logs/ng/tc/tradecache.log*".to_owned(),
        pattern: LogParserEnum::Pattern(PatternParser("committed".to_owned())),
        result: TcResultEnum::HourResult(TcHourResult::new()),
    }
}

pub fn new_v1_publisher() -> TcTool {
    TcTool {
        name: "V1_Publisher".to_owned(),
        path: "C:/working/projects/nimproj/logs/v1/publisher/publish.log*".to_owned(),
        pattern: LogParserEnum::Regex(RegexParser(Some(Regex::new(r"DocWriteTime=([^,]+?),")
                                                           .unwrap()))),
        result: TcResultEnum::BatchResult(TcHourResult::new()),
    }
}

pub struct TcTool {
    name: String,
    path: String,
    pattern: LogParserEnum,
    result: TcResultEnum,
}

impl TcTool {
    fn increase_result(&mut self, time: &str, watermark: &str) -> usize {
        match self.result {
            TcResultEnum::HourResult(ref mut h) => h.increase_count(time, watermark),
            TcResultEnum::BatchResult(ref mut h) => h.increase_count(time, watermark),
        }
    }

    fn get_result(&self) -> Vec<usize> {
        match self.result {
            TcResultEnum::HourResult(ref h) => h.keys_skip_first(),
            TcResultEnum::BatchResult(ref h) => h.keys_skip_first(),
        }
    }

    fn get_value(&self, key: usize) -> Option<&TcStat> {
        match self.result {
            TcResultEnum::HourResult(ref h) => h.get_value(key),
            TcResultEnum::BatchResult(ref h) => h.get_value(key),
        }
    }

    /// sort the path base on extension. if no extension then assume it as 0
    /// for example, make sure the file follow below order
    /// tradecache.log
    /// tradecache.log.1
    /// tradecache.log.2
    /// ...
    /// tradecache.log.10
    fn sorted_path(paths: &[PathBuf]) -> Vec<PathBuf> {
        let mut paths_new: Vec<_> = Vec::new();
        for name in paths {
            let ext: usize = match name.extension() {
                Some(ex) => ex.to_str().unwrap_or("0").parse::<usize>().unwrap_or(0),
                None => 0,
            };
            paths_new.push((name, ext));
        }
        paths_new.sort_by(|a, b| a.1.cmp(&b.1));
        paths_new.iter().map(|a| a.0).cloned().collect()
    }
}

impl TcLogParser for TcTool {
    fn match_line<'a, 'b>(&'a self, line: &'b str) -> Result<Option<&'b str>, TcError> {
        match self.pattern {
            LogParserEnum::Pattern(ref p) => p.match_line(line),
            LogParserEnum::Regex(ref r) => r.match_line(line),
        }
    }
}

impl TcProcesser for TcTool {
    /// Process files which matched the path pattern. for example: directory/file*
    fn process_directory(&mut self, count: usize) {
        let results: Vec<_> = glob(&self.path).unwrap().filter_map(|r| r.ok()).collect();
        let results = Self::sorted_path(&results);

        for name in results {
            let file = File::open(&name).expect("Failed to open log file.");
            let mut c_count = 0;
            for line in BufReader::new(file).lines().filter_map(|line| line.ok()) {
                c_count = match self.process_line(&line) {
                    (Some(pub_time), Some(watermark)) => self.increase_result(pub_time, watermark),
                    (Some(pub_time), None) => self.increase_result(pub_time, ""),
                    _ => continue,
                };

            }
            // we have enough samples, stop!
            if c_count > count {
                return;
            }
        }
    }

    fn print_result(&self) {
        // skip the first value, normally the record too old so likely to be incomplete.
        for (count, key) in self.get_result().iter().rev().enumerate() {
            match self.get_value(*key) {
                Some(val) if count == 0 => {
                    println!("{}-{},{},{}", self.name, count, val, val.delay_time());
                }
                Some(val) => println!("{}-{},{},", self.name, count, val),
                None => println!("{}-{},{}", self.name, count, "missing value"),
            };
        }
    }
}
