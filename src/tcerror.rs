#[derive(Debug)]
pub enum TcError {
    MisMatch,
    InvalidTimeFormat,
    MissingWaterMark,
    Invalid,
}

impl ::std::fmt::Display for TcError {
    fn fmt(&self, f: &mut ::std::fmt::Formatter) -> ::std::fmt::Result {
        match *self {
            TcError::MisMatch => f.write_str("MisMatch"),
            TcError::InvalidTimeFormat => f.write_str("Invalid Time Format"),
            TcError::MissingWaterMark => f.write_str("Not Available"),
            TcError::Invalid => f.write_str("Invalid"), 
        }
    }
}
