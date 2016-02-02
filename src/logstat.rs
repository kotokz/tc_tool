use std::fmt;
use time::{Duration, strptime, Tm};
use error::*;

#[derive(Debug,Clone, Default)]
pub struct Stat {
    /// for hour data, this should be the minutes for the hour
    pub duration: u32,
    /// for hour stat, should be the last msg time for this hour
    /// for batch stat, should be the batch start time
    pub last_sample_time: String,
    /// batch size, for hour stat this should be 0
    pub total: u32,
    /// how many works done for this hour
    pub done: u32,
    /// should be the last msg DB write time for this hour
    pub last_time_stamp: String,
}

impl Stat {
    pub fn new() -> Stat {
        Stat::default()
    }
    /// delay_time calculates the delay from sample time and watermark.
    /// the display format is "HH:MM:SS"
    /// shows 0 if missing information, for example missing watermark for pattern match result
    pub fn delay_time(&self) -> String {
        let sample_time = self.last_sample_time.parse::<LogTime>();
        let time_stamp = self.last_time_stamp.parse::<LogTime>();

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
    /// to_str is a helper function to convert Stat into String.
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
                self.last_sample_time,
                self.total,
                self.done,
                match self.last_time_stamp.parse::<LogTime>() {
                    Ok(e) => e.to_string(),
                    Err(e) => e.to_string(),
                },
                (self.done as f32 / duration as f32),
                if delay {
                    self.delay_time()
                } else {
                    String::default()
                })
    }

    fn cal_batch_eff(&self) -> f32 {
        let sample_time = self.last_sample_time.parse::<LogTime>();
        let time_stamp = self.last_time_stamp.parse::<LogTime>();

        match (sample_time, time_stamp) {
            (Ok(s), Ok(t)) => {
                let time = (t - s).num_seconds() as f32 / 60.0;
                (self.done as f32 / time)
            }
            _ => 0.0,
        }
    }

    pub fn batch_to_str(&self) -> String {

        // "duration, last sample time stamp, total, done, last msg time stamp, eff, delay"
        format!("{}, {}, {}, {}, {:.2}, {}",
                self.last_sample_time,
                self.total,
                self.done,
                match self.last_time_stamp.parse::<LogTime>() {
                    Ok(e) => e.to_string(),
                    Err(e) => e.to_string(),
                },
                self.cal_batch_eff(),
                "")
    }
}

/// LogTime is for date time format conversion and help to calculates delta, for example to calculate
/// delay value.
pub struct LogTime(Tm);

impl ::std::str::FromStr for LogTime {
    type Err = LogError;

    /// 3 kind of watermark timestamp:
    /// a) "2015-09-08 23:41:28"   same as last sample time  length = 19
    /// "%Y-%m-%d %H:%M:%S"
    /// b) "Fri Sep 11 07:59:55 BST 2015"  length = 28
    ///    "%a %b %d %T %Z %Y"
    /// c) "20150918 02:55:33"  length = 17
    ///    "%Y%m%d %H:%M:%S"
    /// d) ""  length = 0
    /// e) "04/09/15 22:28:10" length = 17
    fn from_str(s: &str) -> Result<LogTime> {
        match s.len() {
            19 => {
                strptime(s, "%Y-%m-%d %H:%M:%S")
                    .map_err(|_| LogError::InvalidTimeFormat)
                    .map(LogTime)
            }
            28 => {
                strptime(s, "%a %b %d %T %Z %Y")
                    .map_err(|_| LogError::InvalidTimeFormat)
                    .map(LogTime)
            }
            17 => {
                if s.contains("/") {
                    strptime(s, "%d/%m/%y %H:%M:%S")
                        .map_err(|_| LogError::InvalidTimeFormat)
                        .map(|mut t| {
                            t.tm_year += 100;           // default is 19xx, change it to 20xx
                            let d = Duration::hours(1); // 1 hour timezone difference
                            LogTime(t - d)
                        })
                } else {
                    strptime(s, "%Y%m%d %H:%M:%S")
                        .map_err(|_| LogError::InvalidTimeFormat)
                        .map(LogTime)
                }
            }
            0 => Err(LogError::MissingWaterMark),
            _ => Err(LogError::InvalidTimeFormat),
        }
    }
}

impl fmt::Display for LogTime {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self.0.strftime("%Y-%m-%d %H:%M:%S") {
            Ok(t) => write!(f, "{}", t),
            Err(e) => write!(f, "{}", e),
        }
    }
}

/// Implemnts Sub trait for calculate LogTime subtraction
/// LogTime - LogTime = Duration
impl ::std::ops::Sub for LogTime {
    type Output = Duration;

    fn sub(self, rhs: LogTime) -> Self::Output {
        self.0 - rhs.0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn can_parse_to_time() {
        let t = "2015-09-08 23:41:28".parse::<LogTime>().unwrap();
        assert_eq!(t.to_string(), "2015-09-08 23:41:28");

        let t = "Fri Sep 11 07:59:55 BST 2015".parse::<LogTime>().unwrap();
        assert_eq!(t.to_string(), "2015-09-11 07:59:55");

        let t = "20150918 02:55:33".parse::<LogTime>().unwrap();
        assert_eq!(t.to_string(), "2015-09-18 02:55:33");

        match "".parse::<LogTime>() {
            Ok(_) => panic!("Can not be ok"),
            Err(e) => assert_eq!(e.to_string(), "Not Available"),
        }
    }
}