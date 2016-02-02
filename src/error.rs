use std::fmt;
use std::error::Error;

#[derive(Debug)]
pub enum LogError {
    MisMatch,
    InvalidTimeFormat,
    MissingWaterMark,
}

pub type Result<T> = ::std::result::Result<T, LogError>;

impl fmt::Display for LogError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str(self.description())
    }
}

impl Error for LogError {
    fn description(&self) -> &str {
        match *self {
            LogError::MisMatch => "MisMatch",
            LogError::InvalidTimeFormat => "Invalid Time Format",
            LogError::MissingWaterMark => "Not Available",
        }
    }

    fn cause(&self) -> Option<&Error> {
        None
    }
}
