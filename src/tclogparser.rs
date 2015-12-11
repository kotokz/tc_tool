use regex::Regex;
use tcresult::TcResult;
use tcerror::TcError;

lazy_static! {
    static ref TIMESTAMP_PATTERN: Regex = Regex::new(r"^([^,]+?),").unwrap();
}

pub trait TcLogParser {
    /// process_line use match_line to verify the line and extract the watermark from it.
    /// If the input line is the expected line, then also call get_timestamp to extract the time stamp.
    /// We need both timestamp and watermark to update the result set.
    fn process_line<'b>(&mut self, line: &'b str) -> (Option<&'b str>, Option<&'b str>) {
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
    fn match_line<'b>(&self, line: &'b str) -> Result<Option<&'b str>, TcError>;

    /// get_timestamp extract the time stamp from the beigining of the matched line.
    /// The time format is known in this content so hardcoded in the function as default
    /// implementation.
    fn get_timestamp<'b>(&self, line: &'b str) -> Option<&'b str> {
        match TIMESTAMP_PATTERN.captures(line) {
            Some(t) => t.at(1),
            None => None,
        }
    }
}

pub enum TcParser {
    Regex(RegexParser),
    Pattern(PatternParser),
}

impl TcParser {
    pub fn match_line<'b>(&self, line: &'b str) -> Result<Option<&'b str>, TcError> {
        match *self {
            TcParser::Regex(ref r) => r.match_line(line),
            TcParser::Pattern(ref r) => r.match_line(line),
        }
    }
}

/// Regex parser to use regex to match line and extract watermark.
pub struct RegexParser(pub Regex);
impl RegexParser {
    pub fn match_line<'b>(&self, line: &'b str) -> Result<Option<&'b str>, TcError> {
        match self.0.captures(line) {
            Some(c) => Ok(c.at(1)),
            None => Err(TcError::MisMatch),
        }
    }
}

pub struct PatternParser(pub String);

impl PatternParser {
    pub fn match_line<'b>(&self, line: &'b str) -> Result<Option<&'b str>, TcError> {
        if line.contains(&self.0) {
            Ok(None)
        } else {
            Err(TcError::MisMatch)
        }
    }
}
