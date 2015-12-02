// use std::collections::HashMap;
use std::collections::BTreeMap;
use std::fmt;
use time::*;
#[derive(Debug)]
pub struct TcStat {
    pub duration: usize, // for hour data, this should be the minutes for the hour
    pub last_sample_time: String, // should be the last msg time for this hour
    pub total: usize, // batch size, for hour stat this should be 0
    pub done: usize, // how many works done for this hour
    pub last_time_stamp: String, // should be the last msg DB write time for this hour
}

/// HasDelay provides interface for owner to get delay time.
/// This is to let the owner can cutomize the output, for example only display delay info for latest record.
/// This can help the latest reocrd more noticeable from the output table.
pub trait HasDelay {
    fn delay_time(&self) -> String;
    fn parse_time(time: &str) -> Result<Tm, String>;
    fn time_to_string(time: &str) -> String;
}

/// Implement the Display trait to transfer the struct to output string
/// format: // "duration, last sample time stamp, total, done, last msg time stamp, eff"
impl fmt::Display for TcStat {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let duration = match self.duration {
            0 => 1,
            n => n,
        };

        let last_time = TcStat::time_to_string(&self.last_time_stamp);

        // "duration, last sample time stamp, total, done, last msg time stamp, eff"
        write!(f,
               "{}, {}, {}, {}, {:.2}",
               self.duration,
               self.last_sample_time,
               self.done,
               last_time,
               (self.done as f32 / duration as f32))
        // self.delay_time())
    }
}

impl HasDelay for TcStat {
    /// 3 kind of watermark timestamp:
    /// a) "2015-09-08 23:41:28"   same as last sample time  length = 19
    /// "%Y-%m-%d %H:%M:%S"
    /// b) "Fri Sep 11 07:59:55 BST 2015"  length = 28
    ///    "%a %b %d %T %Z %Y"
    /// c) "20150918 02:55:33"  length = 17
    ///    "%Y%m%d %H:%M:%S"
    /// d) ""  length = 0
    fn parse_time(time: &str) -> Result<Tm, String> {
        match time.len() {
            19 => strptime(time, "%Y-%m-%d %H:%M:%S").map_err(|e| e.to_string()),
            28 => strptime(time, "%a %b %d %T %Z %Y").map_err(|e| e.to_string()),
            17 => strptime(time, "%Y%m%d %H:%M:%S").map_err(|e| e.to_string()),
            _ => Err("Not Available".to_owned()),
        }
    }

    fn time_to_string(time: &str) -> String {
        match TcStat::parse_time(time)
                  .as_ref()
                  .map(|time| time.strftime("%Y-%m-%d %H:%M:%S")) {
            Ok(Ok(t)) => t.to_string(),
            Ok(Err(e)) => e.to_string(),
            Err(e) => e.clone(),
        }
    }

    /// delay_time calculates the delay from sample time and watermark.
    /// the display format is "HH:MM:SS"
    /// shows 0 if missing information, for example missing watermark for pattern match result
    fn delay_time(&self) -> String {
        let sample_time = Self::parse_time(&self.last_sample_time);
        let time_stamp = Self::parse_time(&self.last_time_stamp);

        match (sample_time, time_stamp) {
            (Ok(s), Ok(t)) => {
                let delay = s - t;
                format!("{:02}:{:02}:{:02}",
                        delay.num_hours(),
                        delay.num_minutes() % 60,
                        delay.num_seconds() % 60)
            }
            _ => "0".to_owned(),
        }
    }
}

pub enum TcResultEnum {
    HourResult(TcHourResult),
    BatchResult(TcHourResult),
}

pub trait TcResult {
    type Result;

    /// Increase hour result
    ///
    /// ** Parameters **
    /// time: The timestamp of the log line
    /// watermark: the timestamp of the trade DB write time.
    ///
    /// Returns the current count of TcResult, for early exit purpose
    fn increase_count(&mut self, time: &str, watermark: &str) -> usize;
    fn new() -> Self;

    /// Returns the keys without the oldest record
    fn keys_skip_first(&self) -> Vec<usize>;

    /// Return TcStat value base on key,
    /// Return None if the key not exist.
    fn get_value(&self, key: usize) -> Option<&Self::Result>;

    fn trim_index(index: &str) -> usize {
        String::from_utf8(index.bytes().filter(|c| *c >= b'0' && *c <= b'9').collect::<Vec<_>>())
            .unwrap_or("0".to_owned())
            .parse::<usize>()
            .unwrap_or(0)
    }
}

pub struct TcHourResult(pub BTreeMap<usize, TcStat>);

impl TcResult for TcHourResult {
    type Result = TcStat;

    fn new() -> Self {
        TcHourResult(BTreeMap::<usize, TcStat>::new())
    }

    fn increase_count(&mut self, time: &str, watermark: &str) -> usize {
        let split: Vec<_> = time.split(':').collect();
        let (hour, min): (usize, usize) = match &split[..] {
            // todo: better error handling required
            [ref hour, ref min, _] => (Self::trim_index(hour), min.parse().unwrap()),
            [ref hour, ref min] => (Self::trim_index(hour), min.parse().unwrap()),
            _ => (0, 0),
        };
        {
            let mut result = self.0
                                 .entry(hour)
                                 .or_insert(TcStat {
                                     duration: min,
                                     last_sample_time: time.to_owned(),
                                     total: 0,
                                     done: 0,
                                     last_time_stamp: watermark.to_owned(),
                                 });

            result.done += 1;
            if result.duration <= min {
                result.duration = min;
                result.last_sample_time = time.to_owned();
                result.last_time_stamp = watermark.to_owned();
            }
        }
        self.0.len() as usize
    }

    fn keys_skip_first(&self) -> Vec<usize> {
        // self.sorted_keys().into_iter().skip(1).collect()
        self.0.keys().cloned().skip(1).collect()
    }

    fn get_value(&self, key: usize) -> Option<&Self::Result> {
        self.0.get(&key)
    }
}