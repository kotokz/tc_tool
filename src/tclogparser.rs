use regex::Regex;
use tcresult::*;
use tcerror::TcError;

lazy_static! {
    static ref TIMESTAMP_PATTERN: Regex = Regex::new(r"^([^,]+?),").unwrap();
}

pub struct TcParser {
    // matcher: MatcherEnum,
    matcher: Box<Matcher + Send + 'static>,
    // result: TcResultEnum,
    result: Box<TcResult<Result = TcStat> + Send + 'static>, 
    batch_matcher: Option<RegMatcher>,
}


impl TcParser {

    pub fn new(regex: Option<&str>, pattern: Option<&str>, batch: Option<&str>) -> TcParser {
        TcParser {
            // matcher: MatcherEnum::new(regex, pattern).unwrap(),
            matcher: match (regex, pattern) {
                (Some(r), _) => Box::new(RegMatcher::new(r)),
                (_, Some(p)) => Box::new(PatternMatcher::new(p)),
                _ => panic!("Please provide at least one matcher"),
            },
            // result: match pattern {
            //     None => TcResultEnum::HourResult(TcHourResult::new()),
            //     Some(_) => TcResultEnum::BatchResult(TcBatchResult::new()),
            // },
            result: match batch {
                None => Box::new(TcHourResult::new()),
                Some(_) => Box::new(TcBatchResult::new()),
            }, 
            batch_matcher: match batch {
                None => None,
                Some(r) => Some(RegMatcher::new(r)),
            },            
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
            (Some(pub_time), Some(watermark)) => self.result.increase_count(pub_time, watermark),
            (Some(pub_time), None) => self.result.increase_count(pub_time, ""),
            _ => self.check_batch(line),
        }        
    }
    
    fn check_batch(&mut self, line: &str) -> Option<usize> {
       if let Some(ref r) = self.batch_matcher {
           match r.match_line(line) {
               Ok(r) => {},
               Err(_) => {},
           }
       }
       None
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
        for (count, key) in self.result.keys_skip_first().iter().rev().enumerate() {
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

trait Matcher {
    fn match_line<'a>(&self, line: &'a str) -> Result<Option<&'a str>, TcError>;
}

struct RegMatcher(pub Regex);
impl Matcher for RegMatcher {
    fn match_line<'a>(&self, line: &'a str) -> Result<Option<&'a str>, TcError> {
        match self.0.captures(line) {
            Some(c) => Ok(c.at(1)),
            None => Err(TcError::MisMatch),
        }
    }
}

impl RegMatcher {
    pub fn new(regex: &str) -> RegMatcher {
        RegMatcher(Regex::new(regex).unwrap())
    }
}

pub struct PatternMatcher(pub String);

impl Matcher for PatternMatcher {
    fn match_line<'a>(&self, line: &'a str) -> Result<Option<&'a str>, TcError> {
        // if line.contains(&self.0) {
        //     Ok(None)
        // } else {
        //     Err(TcError::MisMatch)
        // }
        match line.find(&self.0) {
            Some(_) => Ok(None),
            None => Err(TcError::MisMatch), 
        }
    }
}

impl PatternMatcher {
    pub fn new(pattern: &str) -> PatternMatcher {
        PatternMatcher(pattern.to_owned())
    }
}
