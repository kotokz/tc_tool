use std::fmt;
use std::error::Error;

#[derive(Debug)]
pub enum TcError {
    MisMatch,
    InvalidTimeFormat,
    MissingWaterMark,
}

pub type Result<T> = ::std::result::Result<T, TcError>;

impl fmt::Display for TcError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str(self.description())
    }
}

impl Error for TcError {
    fn description(&self) -> &str {
        match *self {
            TcError::MisMatch => "MisMatch",
            TcError::InvalidTimeFormat => "Invalid Time Format",
            TcError::MissingWaterMark => "Not Available",
        }
    }

    fn cause(&self) -> Option<&Error> {
        None
    }
}
