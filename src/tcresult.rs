
use std::collections::HashMap;
use std::collections::hash_state::DefaultState;
use std::collections::BTreeMap;
use fnv::FnvHasher;
use tcstat::TcStat;

pub fn trim_index(index: &str) -> usize {
    String::from_utf8(index.bytes().filter(|c| *c >= b'0' && *c <= b'9').collect::<Vec<_>>())
        .ok()
        .and_then(|m| m.parse::<usize>().ok())
        .unwrap_or(0)
}

pub trait ResultTrait {
    fn increase_count(&mut self, time: &str, watermark: &str, count: usize) -> Option<usize>;
    fn wrap_up_file(&mut self) -> usize;
    fn process_batch(&mut self, _: &str, _: &str, _: &str) {}
    fn print_result(&self, name: &str);
}

/// TcHourResult is simply just a HashMap, using the log hour (usize, for example "2015 09") as 
/// index and TcStat as content.
pub struct TcHourResult(pub HashMap<usize, TcStat, DefaultState<FnvHasher>>);

impl TcHourResult {
    pub fn new() -> TcHourResult {
        TcHourResult(Default::default())
    }
    /// Returns the keys without the oldest record
    fn get_result(&self) -> Vec<usize> {
        // self.0.keys().cloned().skip(1).collect()
        let mut keys: Vec<_> = self.0.keys().cloned().collect();
        keys.sort();
        keys.iter().skip(1).cloned().collect()
    }
}


impl ResultTrait for TcHourResult {
    /// Increase hour result
    ///
    /// ** Parameters **
    /// time: The timestamp of the log line
    /// watermark: the timestamp of the trade DB write time.
    ///
    /// Returns the current count of TcResult, for early exit purpose
    fn increase_count(&mut self, time: &str, watermark: &str, _: usize) -> Option<usize> {
        let split: Vec<_> = time.split(':').collect();
        let (hour, min): (usize, u32) = match &split[..] {
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


    fn wrap_up_file(&mut self) -> usize {
        self.0.len() as usize
    }

    fn print_result(&self, name: &str) {
        // skip the first value, normally the record too old so likely to be incomplete.
        for (count, key) in self.get_result().iter().rev().enumerate() {
            match self.0.get(&key) {
                Some(val) if count == 0 => {
                    println!("{}-{},{}", name, count, val.to_str(true));
                }
                Some(val) => {
                    println!("{}-{},{}", name, count, val.to_str(false));
                }
                None => println!("{}-{},{}", name, count, "missing value"),
            };
        }
    }
}

pub struct TcBatchResult {
    /// BTreeMap for the batch, reuse TcStat to hold the statistic for each batch
    /// usize is the batch start time, is only for batch order
    map: BTreeMap<usize, TcStat>,

    /// temp_count should be always zero when start processing a new file. 
    temp_count: TcStat,

    /// leftover_count means the counts which cannot be recognized as which batch after processed a
    /// file.
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
}

impl ResultTrait for TcBatchResult {
    fn process_batch(&mut self, index: &str, _: &str, total: &str) {
        let total = total.parse::<u32>().unwrap_or(0);
        self.current_batch = Some(trim_index(index));
        let mut result = self.map
                             .entry(self.current_batch.unwrap())
                             .or_insert(TcStat::new());
        result.total = total;
        result.last_sample_time = index.to_owned();
    }

    fn increase_count(&mut self, time: &str, _: &str, _: usize) -> Option<usize> {
        match self.current_batch {
            Some(c) => {
                let mut result = self.map.entry(c).or_insert(TcStat::new());
                result.done += 1;
                result.last_time_stamp = time.to_owned();
            }
            None => {
                self.temp_count.done += 1;
                self.temp_count.last_time_stamp = time.to_owned();
            }
        };
        Some(self.map.len())
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
            if self.leftover_count.last_time_stamp != "" {
                result.last_time_stamp = self.leftover_count.last_time_stamp.clone();
            }
            // self.leftover_count = self.temp_count.clone();
            ::std::mem::swap(&mut self.leftover_count, &mut self.temp_count);
        } else {
            self.leftover_count.done += self.temp_count.done;
            if self.leftover_count.last_time_stamp == "" {
                self.leftover_count.last_time_stamp = self.temp_count.last_time_stamp.clone();
            }
        }
        self.temp_count = TcStat::new();

        self.current_batch = None;
        self.map.len() + 1
    }
    fn print_result(&self, name: &str) {
        // skip the first value, normally the record too old so likely to be incomplete.
        for (count, key) in self.map.keys().rev().enumerate() {
            match self.map.get(&key) {
                Some(val) => {
                    println!("{}-{},{}", name, count, val.batch_to_str());
                }
                None => println!("{}-{},{}", name, count, "missing value"),
            };
        }
    }
}

#[derive(Debug, PartialEq)]
pub struct XdsStat {
    pub period: String,
    pub count: usize,
    pub spent: usize,
}

pub struct XdsResult(pub BTreeMap<usize, XdsStat>);

impl XdsResult {
    pub fn new() -> XdsResult {
        XdsResult(BTreeMap::<usize, XdsStat>::new())
    }
}

impl ResultTrait for XdsResult {
    fn wrap_up_file(&mut self) -> usize {
        self.0.len() as usize
    }

    fn increase_count(&mut self, time: &str, spent: &str, count: usize) -> Option<usize> {
        let split: Vec<_> = time.split(':').collect();
        let hour: usize = match &split[..] {
            // [TODO]: Better error handling required - 2015-12-07 10:07P
            [ref hour, _, _] => trim_index(hour),
            [ref hour, _] => trim_index(hour), 
            _ => return None,
        };
        {
            let mut result = self.0
                                 .entry(hour)
                                 .or_insert(XdsStat {
                                     period: "".into(),
                                     count: 0,
                                     spent: 0,
                                 });
            result.period = time.to_owned();
            result.count += count;
            result.spent += spent.parse::<usize>().unwrap_or(0);
        }
        Some(self.0.len() as usize)
    }

    fn print_result(&self, name: &str) {
        // skip the first value, normally the record too old so likely to be incomplete.
        for (count, key) in self.0.keys().rev().enumerate() {
            match self.0.get(&key) {
                Some(val) => {
                    println!("{}-{},{:?}", name, count, val);
                }
                None => println!("{}-{},{}", name, count, "missing value"),
            };
        }
    }
}


#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn can_increase_xds_count() {
        let mut result = XdsResult::new();
        result.increase_count("2015-11-09 02:01:03", "2092", 100);
        result.increase_count("2015-11-09 02:01:04", "2092", 10);
        result.increase_count("2015-11-09 02:01:05", "2092", 100);
        let c = result.increase_count("2015-11-09 02:06", "2092", 10);

        assert_eq!(*result.0.get(&2015110902).unwrap(),
                   XdsStat {
                       period: "2015-11-09 02:06".to_owned(),
                       count: 220,
                       spent: 8368,
                   });
        assert_eq!(c.unwrap(), result.0.len() as usize);

    }

    #[test]
    fn can_increase_hour_count() {
        let mut result = TcHourResult::new();
        result.increase_count("2015-11-09 02:01:03", "2015-11-09 01:29:32", 1);
        result.increase_count("2015-11-09 02:02:03", "2015-11-09 01:19:32", 1);
        result.increase_count("2015-11-09 02:03:03", "2015-11-09 01:09:32", 1);
        result.increase_count("2015-11-09 01:04", "2015-11-09 01:09:32", 1);
        result.increase_count("2015-11-09 01:05", "2015-11-09 01:09:32", 1);
        result.increase_count("nothing here", "test test", 1);
        result.increase_count("nothing here", "", 1);
        result.increase_count("", "", 1);
        let c = result.increase_count("2015-11-09 01:06", "2015-11-09 01:09:32", 1);

        // return value equals to the map length
        assert_eq!(c.unwrap(), result.0.len() as usize);

        verify_result_set(&result);

        assert_eq!(result.0.get(&2015110902).unwrap().to_str(false),
                   "2015-11-09 02:03:03, 0, 3, 2015-11-09 01:09:32, 1.00, ");

        assert_eq!(result.0.get(&2015110902).unwrap().to_str(true),
                   "2015-11-09 02:03:03, 0, 3, 2015-11-09 01:09:32, 1.00, 00:53:31");

        assert_eq!(result.0.get(&2015110901).unwrap().to_str(false),
                   "2015-11-09 01:06, 0, 3, 2015-11-09 01:09:32, 0.50, ");

        assert_eq!(result.0.get(&2015110901).unwrap().to_str(true),
                   "2015-11-09 01:06, 0, 3, 2015-11-09 01:09:32, 0.50, 0");
    }

    #[test]
    fn can_increase_trimmer_hour_count() {
        let mut result = TcHourResult::new();
        result.increase_count("2015-11-09 02:01:03", "", 1);
        result.increase_count("2015-11-09 02:02:03", "", 1);
        result.increase_count("2015-11-09 02:03:03", "", 1);
        result.increase_count("2015-11-09 01:04", "", 1);
        result.increase_count("2015-11-09 01:05", "", 1);
        let c = result.increase_count("2015-11-09 01:06", "", 1);

        // return value equals to the map length
        assert_eq!(c.unwrap(), result.0.len() as usize);

        verify_result_set(&result);

        assert_eq!(result.0.get(&2015110902).unwrap().to_str(false),
                   "2015-11-09 02:03:03, 0, 3, Not Available, 1.00, ");

        assert_eq!(result.0.get(&2015110902).unwrap().to_str(true),
                   "2015-11-09 02:03:03, 0, 3, Not Available, 1.00, 0");

        assert_eq!(result.0.get(&2015110901).unwrap().to_str(false),
                   "2015-11-09 01:06, 0, 3, Not Available, 0.50, ");

        assert_eq!(result.0.get(&2015110901).unwrap().to_str(true),
                   "2015-11-09 01:06, 0, 3, Not Available, 0.50, 0");
    }

    fn verify_result_set(result: &TcHourResult) {

        for (_, val) in &result.0 {
            // logs can be porperly categoried in map
            assert_eq!(3, val.done);
        }

        //let keys: Vec<_> = result.0.keys().into_iter().cloned().collect();

        // keys are in order
        // assert_eq!(keys, [2015110901, 2015110902]);


        let keys_2 = result.get_result();
        let ordered_keys_2 = [2015110902];
        // The old key can be removed correctly
        assert_eq!(keys_2, ordered_keys_2);

    }
}