use regex::Regex;
use tcresult::*;
use tcerror::TcError;

lazy_static! {
    static ref TIMESTAMP_PATTERN: Regex = Regex::new(r"^([^,]+?),").unwrap();
}

pub trait TcLogParser {
    /// process_line use match_line to verify the line and extract the watermark from it.
    /// If the input line is the expected line, then also call get_timestamp to extract the
    /// time stamp.We need both timestamp and watermark to update the result set.
    fn extract_times<'a>(&mut self, line: &'a str) -> (Option<&'a str>, Option<&'a str>) {
        match self.match_line(line) {
            Ok(r) => {
                let t = self.get_timestamp(line);
                (t, r)
            }
            _ => (None, None),
        }
    }

    /// match_line checks whether the input line is matched by a specific pattern.
    /// if matched,  return Ok with optional watermak string.
    /// if not matched, return Err
    fn match_line<'a>(&self, line: &'a str) -> Result<Option<&'a str>, TcError>;

    /// get_timestamp extract the time stamp from the beigining of the matched line.
    /// The time format is known in this content so hardcoded in the function as default
    /// implementation.
    fn get_timestamp<'a>(&self, line: &'a str) -> Option<&'a str> {
        match TIMESTAMP_PATTERN.captures(line) {
            Some(t) => t.at(1),
            None => None,
        }
    }

    fn increase_result(&mut self, time: &str, watermark: &str) -> Option<usize>;


    fn process_line(&mut self, line: &str) -> Option<usize>;

    fn print_result(&self, name: &str);
}

pub enum TcParser {
    Regex(RegexParser, TcResultEnum),
    Pattern(PatternParser, TcResultEnum),
}

impl TcLogParser for TcParser {
    fn match_line<'a>(&self, line: &'a str) -> Result<Option<&'a str>, TcError> {
        match *self {
            TcParser::Regex(ref r, _) => r.match_line(line),
            TcParser::Pattern(ref r, _) => r.match_line(line),
        }
    }

    fn increase_result(&mut self, time: &str, watermark: &str) -> Option<usize> {
        match *self {
            TcParser::Regex(_, ref mut r) => r.increase_result(time, watermark),
            TcParser::Pattern(_, ref mut r) => r.increase_result(time, watermark),
        }
    }
    fn process_line(&mut self, line: &str) -> Option<usize> {
        match self.extract_times(&line) {
            (Some(pub_time), Some(watermark)) => self.increase_result(pub_time, watermark),
            (Some(pub_time), None) => self.increase_result(pub_time, ""),
            _ => None,
        }
    }

    fn print_result(&self, name: &str) {
        let result = match *self {
            TcParser::Regex(_, ref r) => r,
            TcParser::Pattern(_, ref r) => r,
        };
        // skip the first value, normally the record too old so likely to be incomplete.
        for (count, key) in result.get_result().iter().rev().enumerate() {
            match result.get_value(*key) {
                Some(val) if count == 0 => {
                    println!("{}-{},{}", name, count, val.to_str(true));
                }
                Some(val) => println!("{}-{},{}", name, count, val.to_str(false)),
                None => println!("{}-{},{}", name, count, "missing value"),
            };
        }
    }
}

/// Regex parser to use regex to match line and extract watermark.
pub struct RegexParser(pub Regex);
impl RegexParser {
    pub fn match_line<'a>(&self, line: &'a str) -> Result<Option<&'a str>, TcError> {
        match self.0.captures(line) {
            Some(c) => Ok(c.at(1)),
            None => Err(TcError::MisMatch),
        }
    }
}

pub struct PatternParser(pub String);

impl PatternParser {
    pub fn match_line<'a>(&self, line: &'a str) -> Result<Option<&'a str>, TcError> {
        if line.contains(&self.0) {
            Ok(None)
        } else {
            Err(TcError::MisMatch)
        }
    }
}
