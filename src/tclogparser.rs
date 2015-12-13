use regex::Regex;
use tcresult::*;
use tcerror::TcError;

lazy_static! {
    static ref TIMESTAMP_PATTERN: Regex = Regex::new(r"^([^,]+?),").unwrap();
}

pub struct TcParser {
    matcher: MatcherEnum,
    result: TcResultEnum,
    batchMatcher: Option<MatcherEnum>,
}


impl TcParser {
    pub fn new(regex: Option<&str>, pattern: Option<&str>, batch: Option<&str>) -> TcParser {
        TcParser {
            matcher: MatcherEnum::new(regex, pattern).unwrap(),
            result: match pattern {
                None => TcResultEnum::HourResult(TcHourResult::new()),
                Some(_) => TcResultEnum::BatchResult(TcBatchResult::new()),
            },
            batchMatcher: MatcherEnum::new(batch, None),
        }
    }
    /// extract_times use match_line to verify the line and extract the watermark from it.
    /// If the input line is the expected line, then also call get_timestamp to extract the
    /// time stamp.We need both timestamp and watermark to update the result set.
    pub fn extract_times<'a>(&mut self, line: &'a str) -> (Option<&'a str>, Option<&'a str>) {
        match self.matcher.match_line(line) {
            Ok(r) => {
                let t = self.get_timestamp(line);
                (t, r)
            }
            _ => (None, None),
        }
    }

    /// process_line consumes a single
    /// it will extract the information from input and save into result.
    /// it will return None if the line doesn't match any pattern.
    pub fn process_line(&mut self, line: &str) -> Option<usize> {
        match self.extract_times(&line) {
            (Some(pub_time), Some(watermark)) => self.result.increase_result(pub_time, watermark),
            (Some(pub_time), None) => self.result.increase_result(pub_time, ""),
            _ => None,
        }
    }

    /// get_timestamp extract the time stamp from the beigining of the matched line.
    /// The time format is known in this content so hardcoded in the function as default
    /// implementation.
    fn get_timestamp<'a>(&self, line: &'a str) -> Option<&'a str> {
        match TIMESTAMP_PATTERN.captures(line) {
            Some(t) => t.at(1),
            None => None,
        }
    }

    pub fn wrap_up_file(&mut self) -> usize {
        self.result.wrap_up_file()
    }

    pub fn print_result(&self, name: &str) {
        // skip the first value, normally the record too old so likely to be incomplete.
        for (count, key) in self.result.get_result().iter().rev().enumerate() {
            match self.result.get_value(*key) {
                Some(val) if count == 0 => {
                    println!("{}-{},{}", name, count, val.to_str(true));
                }
                Some(val) => println!("{}-{},{}", name, count, val.to_str(false)),
                None => println!("{}-{},{}", name, count, "missing value"),
            };
        }
    }
}

pub enum MatcherEnum {
    Regex(RegexParser),
    Pattern(PatternParser),
}

impl MatcherEnum {
    /// match_line checks whether the input line is matched by a specific pattern.
    /// if matched,  return Ok with optional watermak string.
    /// if not matched, return Err
    fn match_line<'a>(&self, line: &'a str) -> Result<Option<&'a str>, TcError> {
        match *self {
            MatcherEnum::Regex(ref r) => r.match_line(line),
            MatcherEnum::Pattern(ref r) => r.match_line(line),
        }
    }

    fn new(regex: Option<&str>, pattern: Option<&str>) -> Option<MatcherEnum> {
        if let Some(r) = regex {
            Some(MatcherEnum::Regex(RegexParser(Regex::new(r).unwrap())))
        } else if let Some(p) = pattern {
            Some(MatcherEnum::Pattern(PatternParser(p.to_owned())))
        } else {
            None
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
