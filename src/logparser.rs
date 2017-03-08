use regex::{Regex, Match};
use logresult::*;
use error::LogError;


pub struct LogParser<'tc> {
    matcher: MatcherEnum,
    result: Box<ResultTrait + Send + 'tc>,
    batch_matcher: Option<MatcherEnum>,
    time_regex: Regex,
}

impl<'tc> LogParser<'tc> {
    pub fn new<T: ToMatcher>(pattern: T) -> LogParser<'tc> {
        LogParser {
            matcher: pattern.to_matcher(),
            result: Box::new(HourResult::new()),
            batch_matcher: None,
            time_regex: Regex::new(r"^([^,]+),").unwrap(),
        }
    }

    pub fn new_batch<T: ToMatcher, P: ToMatcher>(pattern: T, batch: Option<P>) -> LogParser<'tc> {
        LogParser {
            matcher: pattern.to_matcher(),
            result: match batch {
                None => Box::new(HourResult::new()),
                Some(_) => Box::new(BatchResult::new()),
            },
            batch_matcher: batch.map(|t| t.to_matcher()),
            time_regex: Regex::new(r"^([^,]+),").unwrap(),
        }
    }

    /// extract_times use match_line to verify the line and extract the watermark from it.
    /// If the input line is the expected line, then also call get_timestamp to extract the
    /// time stamp.We need both timestamp and watermark to update the result set.
    pub fn extract_info<'a>(&mut self,
                            line: &'a str)
                            -> (Option<&'a str>, Option<Match<'a>>, Option<Match<'a>>) {
        match self.matcher.match_line(line) {
            Ok((r, c)) => {
                let t = self.get_timestamp(line);
                (Some(t), r, c)
            }
            _ => (None, None, None),
        }
    }

    /// process_line consumes a single
    /// it will extract the information from input and save into result.
    /// it will return None if the line doesn't match any pattern.
    pub fn process_line(&mut self, line: &str) -> Option<usize> {
        match self.extract_info(line) {
            (Some(time), Some(count), Some(spent)) => {
                self.result.increase_count(time, spent.as_str(), count.as_str().parse::<usize>().unwrap_or(1))
            }
            (Some(time), Some(watermark), None) => self.result.increase_count(time, watermark.as_str(), 1),
            (Some(time), None, None) => self.result.increase_count(time, "", 1),
            _ => {
                self.check_batch(line);
                None
            }
        }
    }

    fn check_batch(&mut self, line: &str) {
        if let Some(ref p) = self.batch_matcher {

            match p.match_batch(line) {
                Ok((Some(r), Some(c))) => {
                    let t = self.get_timestamp(line);
                    self.result.process_batch(t, r.as_str(), c.as_str())
                }
                Ok((Some(c), None)) if c.as_str().parse::<usize>().unwrap_or(0) > 0 => {
                    let t = self.get_timestamp(line);
                    self.result.process_batch(t, "", c.as_str())
                }
                _ => return,
            }
        }
    }

    /// get_timestamp extract the time stamp from the beigining of the matched line.
    /// The time format is known in this content so hardcoded in the function as default
    /// implementation.
    fn get_timestamp<'a>(&self, line: &'a str) -> &'a str {
        match self.time_regex.captures(line) {
            Some(t) => t.get(1).map_or("", |m| m.as_str()),
            None => "",
        }
    }

    pub fn wrap_up_file(&mut self) -> usize {
        self.result.wrap_up_file()
    }

    pub fn print_result(&self, name: &str) {
        self.result.print_result(name);
    }
}

pub enum MatcherEnum {
    Regex(Regex),
    Pattern(String),
}

pub trait ToMatcher {
    fn to_matcher(self) -> MatcherEnum;
}

impl ToMatcher for Regex {
    fn to_matcher(self) -> MatcherEnum {
        MatcherEnum::Regex(self)
    }
}

impl ToMatcher for String {
    fn to_matcher(self) -> MatcherEnum {
        MatcherEnum::Pattern(self)
    }
}

impl<'a> ToMatcher for &'a str {
    fn to_matcher(self) -> MatcherEnum {
        MatcherEnum::Pattern(self.to_owned())
    }
}

impl MatcherEnum {
    pub fn match_line<'a>(&self,
                          line: &'a str)
                          -> Result<(Option<Match<'a>>, Option<Match<'a>>), LogError> {
        match *self {
            MatcherEnum::Regex(ref r) => {
                match r.captures(line) {
                    Some(c) => Ok((c.get(1), c.get(2))),
                    None => Err(LogError::MisMatch),
                }
            }
            MatcherEnum::Pattern(ref r) => {
                if line.contains(r) {
                    Ok((None, None))
                } else {
                    Err(LogError::MisMatch)
                }
            }
        }
    }

    pub fn match_batch<'a>(&self,
                           line: &'a str)
                           -> Result<(Option<Match<'a>>, Option<Match<'a>>), LogError> {
        match *self {
            MatcherEnum::Regex(ref r) => {
                match r.captures(line) {
                    Some(c) => Ok((c.get(1), c.get(2))),
                    None => Err(LogError::MisMatch),
                }
            }
            _ => Err(LogError::MisMatch),
        }
    }
}
