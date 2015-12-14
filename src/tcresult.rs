// use std::collections::HashMap;
use std::collections::BTreeMap;
use std::fmt;
use time::*;
use tcerror::*;

#[derive(Debug,Clone)]
pub struct TcStat {
    /// for hour data, this should be the minutes for the hour
    pub duration: usize,
    /// should be the last msg time for this hour
    pub last_sample_time: String,
    /// batch size, for hour stat this should be 0
    pub total: usize,
    /// how many works done for this hour
    pub done: usize,
    /// should be the last msg DB write time for this hour
    pub last_time_stamp: String,
}

impl ::std::ops::Add for TcStat {
    type Output = TcStat;

    fn add(self, rhs: TcStat) -> Self::Output {
        TcStat {
            duration: self.duration + rhs.duration,
            last_sample_time: self.last_sample_time.clone(),
            total: self.total + self.total,
            done: self.done,
            last_time_stamp: self.last_sample_time.clone(),
        }
    }
}

impl TcStat {
    pub fn new() -> TcStat {
        TcStat {
            duration: 0,
            last_sample_time: "".to_owned(),
            total: 0,
            done: 0,
            last_time_stamp: "".to_owned(),
        }
    }
    /// delay_time calculates the delay from sample time and watermark.
    /// the display format is "HH:MM:SS"
    /// shows 0 if missing information, for example missing watermark for pattern match result
    pub fn delay_time(&self) -> String {
        let sample_time = self.last_sample_time.parse::<TcTime>();
        let time_stamp = self.last_time_stamp.parse::<TcTime>();

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
    /// to_str is a helper function to convert TcStat into String.
    /// follow the format "duration, last sample time stamp, total, done, last msg time stamp, eff, delay"
    /// *** Paramter ***
    /// delay: bool   whether display delay value. we don't want to show delay for every row.
    /// otherwise use is very hard to notice the first line, which is normally the latest
    /// information
    pub fn to_str(&self, delay: bool) -> String {

        let duration = match self.duration {
            0 => 1,
            n => n,
        };

        // "duration, last sample time stamp, total, done, last msg time stamp, eff, delay"
        format!("{}, {}, {}, {}, {:.2}, {}",
                self.duration,
                self.last_sample_time,
                self.done,
                match self.last_time_stamp.parse::<TcTime>() {
                    Ok(e) => e.to_string(),
                    Err(e) => e.to_string(),
                },
                (self.done as f32 / duration as f32),
                if delay {
                    self.delay_time()
                } else {
                    "".to_owned()
                })
    }
}

/// TcTime is for date time format conversion and help to calculates delta, for example to calculate
/// delay value.
pub struct TcTime(Tm);

impl ::std::str::FromStr for TcTime {
    type Err = TcError;

    /// 3 kind of watermark timestamp:
    /// a) "2015-09-08 23:41:28"   same as last sample time  length = 19
    /// "%Y-%m-%d %H:%M:%S"
    /// b) "Fri Sep 11 07:59:55 BST 2015"  length = 28
    ///    "%a %b %d %T %Z %Y"
    /// c) "20150918 02:55:33"  length = 17
    ///    "%Y%m%d %H:%M:%S"
    /// d) ""  length = 0
    fn from_str(s: &str) -> Result<TcTime> {
        match s.len() {
            19 => {
                strptime(s, "%Y-%m-%d %H:%M:%S")
                    .map_err(|_| TcError::InvalidTimeFormat)
                    .map(TcTime)
            }
            28 => {
                strptime(s, "%a %b %d %T %Z %Y")
                    .map_err(|_| TcError::InvalidTimeFormat)
                    .map(TcTime)
            }
            17 => {
                strptime(s, "%Y%m%d %H:%M:%S")
                    .map_err(|_| TcError::InvalidTimeFormat)
                    .map(TcTime)
            }
            0 => Err(TcError::MissingWaterMark),
            _ => Err(TcError::InvalidTimeFormat),
        }
    }
}

impl fmt::Display for TcTime {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self.0.strftime("%Y-%m-%d %H:%M:%S") {
            Ok(t) => write!(f, "{}", t),
            Err(e) => write!(f, "{}", e),
        }
    }
}

/// Implemnts Sub trait for calculate TcTime subtraction
/// TcTime - TcTime = Duration
impl ::std::ops::Sub for TcTime {
    type Output = Duration;

    fn sub(self, rhs: TcTime) -> Self::Output {
        self.0 - rhs.0
    }
}
fn trim_index(index: &str) -> usize {
    String::from_utf8(index.bytes().filter(|c| *c >= b'0' && *c <= b'9').collect::<Vec<_>>())
        .ok()
        .and_then(|m| m.parse::<usize>().ok())
        .unwrap_or(0)
}

pub enum TcResultEnum {
    HourResult(TcHourResult),
    BatchResult(TcBatchResult),
}

impl TcResultEnum {
    pub fn increase_count(&mut self, time: &str, watermark: &str) -> Option<usize> {
        match *self {
            TcResultEnum::HourResult(ref mut h) => h.increase_count(time, watermark),
            TcResultEnum::BatchResult(ref mut h) => h.increase_count(time, watermark),
        }
    }

    pub fn get_result(&self) -> Vec<usize> {
        match *self {
            TcResultEnum::HourResult(ref h) => h.keys_skip_first(),
            TcResultEnum::BatchResult(ref h) => h.keys_skip_first(),
        }
    }

    pub fn get_value(&self, key: usize) -> Option<&TcStat> {
        match *self {
            TcResultEnum::HourResult(ref h) => h.get_value(key),
            TcResultEnum::BatchResult(ref h) => h.get_value(key),
        }
    }

    pub fn wrap_up_file(&mut self) -> usize {
        match *self {
            TcResultEnum::HourResult(ref h) => h.0.len() as usize,
            TcResultEnum::BatchResult(ref mut h) => h.wrap_up_file(),
        }
    }

    pub fn process_batch(&mut self, index: &str, total: usize) {
        if let TcResultEnum::BatchResult(ref mut h) = *self {
            h.process_batch(index, total);
        }
    }
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
    fn increase_count(&mut self, time: &str, watermark: &str) -> Option<usize>;

    /// Returns the keys without the oldest record
    fn keys_skip_first(&self) -> Vec<usize>;

    /// Return TcStat value base on key,
    /// Return None if the key not exist.
    fn get_value(&self, key: usize) -> Option<&Self::Result>;

    fn wrap_up_file(&mut self) -> usize;
}

/// TcHourResult is simply just a BTreeMap, using the log hour (usize, for example "2015 09") as 
/// index and TcStat as content.
/// Chose BTreeMap is for TcStat order. new hour is the largest record in the map. so we can use 
/// reverse print to print from latest to oldest.
/// The record just less than 10 records, so BTreemap performance is very fast.
pub struct TcHourResult(pub BTreeMap<usize, TcStat>);

impl TcHourResult {
    pub fn new() -> TcHourResult {
        TcHourResult(BTreeMap::<usize, TcStat>::new())
    }
}

/// Implements TcResult trait for TcHourResult.
/// This Struct is for hour statistic collection.
impl TcResult for TcHourResult {
    type Result = TcStat;

    fn increase_count(&mut self, time: &str, watermark: &str) -> Option<usize> {
        let split: Vec<_> = time.split(':').collect();
        let (hour, min): (usize, usize) = match &split[..] {
            // [TODO]: Better error handling required - 2015-12-07 10:07P
            [ref hour, ref min, _] => (trim_index(hour), min.parse().unwrap()),
            [ref hour, ref min] => (trim_index(hour), min.parse().unwrap()),
            _ => return None,
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
        Some(self.0.len() as usize)
    }

    fn keys_skip_first(&self) -> Vec<usize> {
        // self.sorted_keys().into_iter().skip(1).collect()
        self.0.keys().cloned().skip(1).collect()
    }

    fn get_value(&self, key: usize) -> Option<&Self::Result> {
        self.0.get(&key)
    }

    fn wrap_up_file(&mut self) -> usize {
        self.0.len() as usize
    }
}

pub struct TcBatchResult {
    /// BTreeMap for the batch, reuse TcStat to hold the statistic for each batch
    /// usize is the batch start time, is only for batch order
    map: BTreeMap<usize, TcStat>,

    /// temp_count should be always zero when start processing a new file. Untill we meet a batch
    /// indicator, the count should be used in 'increase_count' method. Once we meet a batch
    /// indicator, we should switch to use the batch stat in map. temp_count should be
    /// remained unchange untill the file finished process. then we should either add it into
    /// leftover_count (if we don't have batch indicator line in this file) or replace left_over
    /// count with temp_count value (the left over count should be added into the last batch of this file)
    temp_count: TcStat,

    /// leftover_count means the counts which cannot be recognized as which batch after processed a
    /// file. if the next file has batch, this number should be added into the last batch of the
    /// next file.
    leftover_count: TcStat,

    /// current_batch is the current batch index. We need to keep this for quick reference.
    /// When the current_batch is Some, it means we are in the known batch scope, all the counts
    /// will be go into the batch statistic.
    /// When the current_batch is None, it means we don't know these counts in which batch scope,
    /// likely we are in a the begining of a new file, so keep the counts in temp_count.
    current_batch: Option<usize>,
}

impl TcBatchResult {
    pub fn new() -> TcBatchResult {
        TcBatchResult {
            map: BTreeMap::<usize, TcStat>::new(),
            temp_count: TcStat::new(),
            leftover_count: TcStat::new(),
            current_batch: None,
        }
    }
    fn process_batch(&mut self, index: &str, total: usize) {
        self.current_batch = Some(trim_index(index));
        let mut result = self.map
                             .entry(self.current_batch.unwrap())
                             .or_insert(TcStat::new());
        result.total = total;
    }
}

impl TcResult for TcBatchResult {
    type Result = TcStat;

    fn increase_count(&mut self, time: &str, watermark: &str) -> Option<usize> {
        let split: Vec<_> = time.split(':').collect();
        let (hour, min): (usize, usize) = match &split[..] {
            // [TODO]: Better error handling required - 2015-12-07 10:07P
            [ref hour, ref min, _] => (trim_index(hour), min.parse().unwrap()),
            [ref hour, ref min] => (trim_index(hour), min.parse().unwrap()),
            _ => return None,
        };
        Some(self.map.len() as usize);
        unimplemented!()
    }

    fn keys_skip_first(&self) -> Vec<usize> {
        // self.sorted_keys().into_iter().skip(1).collect()
        self.map.keys().cloned().skip(1).collect()
    }

    fn get_value(&self, key: usize) -> Option<&Self::Result> {
        self.map.get(&key)
    }

    /// wrap_up_file will perform post-file processing for batch result.
    /// like reset current_batch, recalculate temp_count and leftover_count.
    fn wrap_up_file(&mut self) -> usize {
        if let Some(batch) = self.current_batch {
            // the leftover_count from previous should be part of the last batch of this file
            // if batch is some, then add the count into batch.
            let mut result = self.map
                                 .entry(batch)
                                 .or_insert(TcStat::new());

            result.done += self.leftover_count.done;
            result.duration = self.leftover_count.duration;
            result.last_sample_time = self.leftover_count.last_sample_time.clone();
            result.last_time_stamp = self.leftover_count.last_time_stamp.clone();
            self.leftover_count = self.temp_count.clone();
        } else {
            self.leftover_count = self.leftover_count.clone() + self.temp_count.clone();
        }

        self.current_batch = None;
        self.map.len() as usize
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn can_increase_hour_count() {
        let mut result = TcHourResult::new();
        result.increase_count("2015-11-09 02:01:03", "2015-11-09 01:29:32");
        result.increase_count("2015-11-09 02:02:03", "2015-11-09 01:19:32");
        result.increase_count("2015-11-09 02:03:03", "2015-11-09 01:09:32");
        result.increase_count("2015-11-09 01:04", "2015-11-09 01:09:32");
        result.increase_count("2015-11-09 01:05", "2015-11-09 01:09:32");
        result.increase_count("nothing here", "test test");
        result.increase_count("nothing here", "");
        result.increase_count("", "");
        let c = result.increase_count("2015-11-09 01:06", "2015-11-09 01:09:32");

        // return value equals to the map length
        assert_eq!(c.unwrap(), result.0.len() as usize);

        verify_result_set(&result);

        assert_eq!(result.0.get(&2015110902).unwrap().to_str(false),
                   "3, 2015-11-09 02:03:03, 3, 2015-11-09 01:09:32, 1.00, ");

        assert_eq!(result.0.get(&2015110902).unwrap().to_str(true),
                   "3, 2015-11-09 02:03:03, 3, 2015-11-09 01:09:32, 1.00, 00:53:31");

        assert_eq!(result.0.get(&2015110901).unwrap().to_str(false),
                   "6, 2015-11-09 01:06, 3, 2015-11-09 01:09:32, 0.50, ");

        assert_eq!(result.0.get(&2015110901).unwrap().to_str(true),
                   "6, 2015-11-09 01:06, 3, 2015-11-09 01:09:32, 0.50, 0");
    }

    #[test]
    fn can_increase_trimmer_hour_count() {
        let mut result = TcHourResult::new();
        result.increase_count("2015-11-09 02:01:03", "");
        result.increase_count("2015-11-09 02:02:03", "");
        result.increase_count("2015-11-09 02:03:03", "");
        result.increase_count("2015-11-09 01:04", "");
        result.increase_count("2015-11-09 01:05", "");
        let c = result.increase_count("2015-11-09 01:06", "");

        // return value equals to the map length
        assert_eq!(c.unwrap(), result.0.len() as usize);

        verify_result_set(&result);

        assert_eq!(result.0.get(&2015110902).unwrap().to_str(false),
                   "3, 2015-11-09 02:03:03, 3, Not Available, 1.00, ");

        assert_eq!(result.0.get(&2015110902).unwrap().to_str(true),
                   "3, 2015-11-09 02:03:03, 3, Not Available, 1.00, 0");

        assert_eq!(result.0.get(&2015110901).unwrap().to_str(false),
                   "6, 2015-11-09 01:06, 3, Not Available, 0.50, ");

        assert_eq!(result.0.get(&2015110901).unwrap().to_str(true),
                   "6, 2015-11-09 01:06, 3, Not Available, 0.50, 0");
    }

    fn verify_result_set(result: &TcHourResult) {

        for (_, val) in &result.0 {
            // logs can be porperly categoried in map
            assert_eq!(3, val.done);
        }

        let keys: Vec<_> = result.0.keys().into_iter().cloned().collect();

        // keys are in order
        assert_eq!(keys, [2015110901, 2015110902]);


        let keys_2 = result.keys_skip_first();
        let ordered_keys_2 = [2015110902];
        // The old key can be removed correctly
        assert_eq!(keys_2, ordered_keys_2);

    }

    #[test]
    fn can_parse_to_tctime() {
        let t = "2015-09-08 23:41:28".parse::<TcTime>().unwrap();
        assert_eq!(t.to_string(), "2015-09-08 23:41:28");

        let t = "Fri Sep 11 07:59:55 BST 2015".parse::<TcTime>().unwrap();
        assert_eq!(t.to_string(), "2015-09-11 07:59:55");

        let t = "20150918 02:55:33".parse::<TcTime>().unwrap();
        assert_eq!(t.to_string(), "2015-09-18 02:55:33");

        match "".parse::<TcTime>() {
            Ok(_) => panic!("Can not be ok"),
            Err(e) => assert_eq!(e.to_string(), "Not Available"),
        }
    }
}
