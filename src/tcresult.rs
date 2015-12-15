// use std::collections::HashMap;
use std::collections::BTreeMap;
use tcstat::*;

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
            TcResultEnum::BatchResult(ref mut h) => h.increase_count(time),
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

    pub fn process_batch(&mut self, index: &str, watermark: &str, total: &str) {
        if let TcResultEnum::BatchResult(ref mut h) = *self {
            h.process_batch(index, watermark, total.parse::<usize>().unwrap_or(0));
        }
    }
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


    /// Increase hour result
    ///
    /// ** Parameters **
    /// time: The timestamp of the log line
    /// watermark: the timestamp of the trade DB write time.
    ///
    /// Returns the current count of TcResult, for early exit purpose
    pub fn increase_count(&mut self, time: &str, watermark: &str) -> Option<usize> {
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

    /// Returns the keys without the oldest record
    fn keys_skip_first(&self) -> Vec<usize> {
        // self.sorted_keys().into_iter().skip(1).collect()
        self.0.keys().cloned().skip(1).collect()
    }

    /// Return TcStat value base on key,
    /// Return None if the key not exist.
    fn get_value(&self, key: usize) -> Option<&TcStat> {
        self.0.get(&key)
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
    fn process_batch(&mut self, index: &str, _: &str, total: usize) {
        self.current_batch = Some(trim_index(index));
        let mut result = self.map
                             .entry(self.current_batch.unwrap())
                             .or_insert(TcStat::new());
        result.total = total;
        result.last_sample_time = index.to_owned();
    }

    pub fn increase_count(&mut self, time: &str) -> Option<usize> {
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
        Some(self.map.len() as usize)
    }

    fn keys_skip_first(&self) -> Vec<usize> {
        // self.sorted_keys().into_iter().skip(1).collect()
        self.map.keys().cloned().collect()
    }

    fn get_value(&self, key: usize) -> Option<&TcStat> {
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
            if self.leftover_count.last_time_stamp != "" {
                result.last_time_stamp = self.leftover_count.last_time_stamp.clone();
            }
            self.leftover_count = self.temp_count.clone();
        } else {
            self.leftover_count.done += self.temp_count.done;
            if self.leftover_count.last_time_stamp == "" {
                self.leftover_count.last_time_stamp = self.temp_count.last_time_stamp.clone();
            }
        }
        self.temp_count = TcStat::new();

        self.current_batch = None;
        self.map.len() + 1 as usize
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
