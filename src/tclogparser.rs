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
    fn process_line<'a, 'b>(&'a mut self, line: &'b str) -> (Option<&'b str>, Option<&'b str>) {
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
    fn match_line<'a, 'b>(&'a self, line: &'b str) -> Result<Option<&'b str>, TcError>;
    fn get_timestamp<'a, 'b>(&'a self, line: &'b str) -> Option<&'b str> {
        match TIMESTAMP_PATTERN.captures(line) {
            Some(t) => t.at(1),
            None => None,
        }
    }
}

/// Regex parser to use regex to match line and extract watermark.
pub struct RegexParser(pub Option<Regex>);
impl RegexParser {
    pub fn match_line<'a, 'b>(&'a self, line: &'b str) -> Result<Option<&'b str>, TcError> {
        match self.0 {
            Some(ref r) => {
                match r.captures(line) {
                    Some(c) => Ok(c.at(1)),
                    None => Err(TcError::MisMatch),
                }
            }
            None => Err(TcError::Invalid),
        }
    }
}

/// PatternParser using string pattern to match the line only. watermark not available for this case.
/// Return Ok(None) for successful match.
/// Retrun Err(TcError::MisMatch) for mismatch.
pub struct PatternParser(pub String);
impl PatternParser {
    pub fn match_line<'a, 'b>(&'a self, line: &'b str) -> Result<Option<&'b str>, TcError> {
        if line.contains(&self.0) {
            return Ok(None);
        }
        Err(TcError::MisMatch)
    }
}

pub enum LogParserEnum {
    Pattern(PatternParser),
    Regex(RegexParser),
}

pub trait TcProcesser {
    fn process_directory(&mut self, count: usize);
    fn print_result(&self);
}
                                  