use std::fmt;
use time::*;
use tcerror::*;

#[derive(Debug,Clone)]
pub struct TcStat {
    /// for hour data, this should be the minutes for the hour
    pub duration: usize,
    /// for hour stat, should be the last msg time for this hour
    /// for batch stat, should be the batch start time
    pub last_sample_time: String,
    /// batch size, for hour stat this should be 0
    pub total: usize,
    /// how many works done for this hour
    pub done: usize,
    /// should be the last msg DB write time for this hour
    pub last_time_stamp: String,
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
                self.last_sample_time,
                self.total,
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

    fn cal_batch_eff(&self) -> f32 {
        let sample_time = self.last_sample_time.parse::<TcTime>();
        let time_stamp = self.last_time_stamp.parse::<TcTime>();

        match (sample_time, time_stamp) {
            (Ok(s), Ok(t)) => {
                let time = (t - s).num_minutes() as usize;
                (self.done as f32 / time as f32)
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
                match self.last_time_stamp.parse::<TcTime>() {
                    Ok(e) => e.to_string(),
                    Err(e) => e.to_string(),
                },
                self.cal_batch_eff(),
                "")
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
