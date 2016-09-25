use glob::glob;
use regex::Regex;
use std::io::{BufReader, BufRead};
use std::path::PathBuf;
use std::fs::File;

use logparser::*;

pub struct TcTool<'a> {
    name: &'a str,
    path: &'a str,
    pattern: LogParser<'a>,
    count: usize,
}

impl<'a> TcTool<'a> {
    pub fn new_hour(count: usize, prod: bool) -> TcTool<'a> {
        TcTool {
            name: "Hour",
            path: if prod {
                "xxxx/hour_prod.log*"
            } else {
                "xxxx/hour.log*"
            },
            pattern: LogParser::new(Regex::new(r"docWriteTime=([^}]+)}").unwrap()),

            count: count,
        }
    }


    pub fn new_batch(count: usize, prod: bool) -> TcTool<'a> {
        TcTool {
            name: "Batch",
            path: if prod {
                "xxx/prod/logs/batch.log*"
            } else {
                "xxx/test/logs/batch.log*"
            },
            pattern: LogParser::new_batch("committed", Regex::new(r"Context contains (\d+)").ok()),
            count: count,
        }
    }

    /// sort the path base on extension. if no extension then assume it as 0
    /// for example, make sure the file follow below order
    /// tc.log
    /// tc.log.1
    /// tc.log.2
    /// ...
    /// tc.log.10
    fn sorted_path(paths: &[PathBuf]) -> Vec<PathBuf> {
        let mut paths_new: Vec<_> = paths.iter()
                                         .map(|name| {
                                             let ext: usize = match name.extension() {
                                                 Some(ex) => {
                                                     ex.to_str()
                                                       .and_then(|m| m.parse::<usize>().ok())
                                                       .unwrap_or(0)
                                                 }
                                                 None => 0,
                                             };
                                             (name, ext)
                                         })
                                         .collect();
        paths_new.sort_by(|a, b| a.1.cmp(&b.1));
        paths_new.iter().map(|a| a.0).cloned().collect()
    }
    /// Process files which matched the path pattern. for example: directory/file*
    pub fn process_directory(&mut self) {
        let files: Vec<_> = glob(self.path).unwrap().filter_map(|r| r.ok()).collect();
        let files = Self::sorted_path(&files);

        for name in files {
            let file = File::open(&name).expect("Failed to open log file.");
            for line in BufReader::new(file).lines().filter_map(|line| line.ok()) {
                self.pattern.process_line(&line);
            }
            // we have enough samples, stop!
            if self.pattern.wrap_up_file() > self.count {
                return;
            }
        }
    }

    pub fn print_result(&self) {
        self.pattern.print_result(self.name);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use test::Bencher;
    use std::thread;

    fn process_line_incorrect_tester(tc: &mut TcTool) {
        let lines: Vec<_> = vec![
                "incorrect line",
                "",
                "2015-09-11 09:28:49,842 aaaaaaaaaaaaaaaaaa timestamp=aaaa",
                "2015-09-11 09:28:49,842 aaaaaaaaaaaaaaaaaa docWriteTime=aaaa",
                "2015-09-11 09:28:49,842 aaaaaaaaaaaaaaaaaa DocWriteTime=aaaa",
            ];

        for line in lines {
            let (pub_time, watermark, _) = tc.pattern.extract_info(&line);
            assert_eq!(pub_time, None);
            assert_eq!(watermark, None);
        }
    }

    #[bench]
    fn bench_process_line_v1_publisher(b: &mut Bencher) {
        let mut tc = TcTool::new_v1_publisher(6, false);

        let line = "2015-11-08 09:07:54,679 JMS DocWriteTime=20151028 07:17:17,";

        b.iter(|| {
            let (pub_time, watermark, _) = tc.pattern.extract_info(&line);
            assert_eq!(pub_time, Some("2015-11-08 09:07:54"));
            assert_eq!(watermark, Some("20151028 07:17:17"));

            process_line_incorrect_tester(&mut tc);
        });
    }

    #[bench]
    fn bench_process_line_ng_consumer(b: &mut Bencher) {
        let mut tc = TcTool::new_ng_consumer(6, false);

        let line = "2015-09-11 09:28:49,842 INFO timestamp=Fri Sep 11 09:28:49 BST \
                    2015eventId=45139252}";
        b.iter(|| {
            let (pub_time, watermark, _) = tc.pattern.extract_info(&line);
            assert_eq!(pub_time, Some("2015-09-11 09:28:49"));
            assert_eq!(watermark, Some("Fri Sep 11 09:28:49 BST 2015"));

            process_line_incorrect_tester(&mut tc);
        });
    }

    #[bench]
    fn bench_process_line_ng_publisher(b: &mut Bencher) {
        let mut tc = TcTool::new_ng_publisher(6, false);

        let line = "2015-09-09 02:35:01,024 =, docWriteTime=2015-09-09 01:35:03}, ";
        b.iter(|| {
            let (pub_time, watermark, _) = tc.pattern.extract_info(&line);
            assert_eq!(pub_time, Some("2015-09-09 02:35:01"));
            assert_eq!(watermark, Some("2015-09-09 01:35:03"));

            process_line_incorrect_tester(&mut tc);
        });
    }

    #[bench]
    fn bench_process_line_ng_trimmer(b: &mut Bencher) {
        let mut tc = TcTool::new_ng_trimmer(6, false);

        let line = "2015-09-10 21:06:34,594 INFO    - committed deletes to disk cache";
        b.iter(|| {
            let (pub_time, watermark, _) = tc.pattern.extract_info(&line);
            assert_eq!(pub_time, Some("2015-09-10 21:06:34"));
            assert_eq!(watermark, None);

            process_line_incorrect_tester(&mut tc);
        });
    }


    #[bench]
    #[ignore]
    fn bench_tc_v1_process(b: &mut Bencher) {
        b.iter(|| {
            let mut publisher = TcTool::new_v1_publisher(6, false);
            publisher.process_directory();
        });
    }

    #[bench]
    #[ignore]
    fn bench_tc_ng_process(b: &mut Bencher) {
        b.iter(|| {
            let mut consumer = TcTool::new_ng_consumer(6, false);
            consumer.process_directory();
        });
    }

    #[bench]
    #[ignore]
    fn bench_tc_ng_trimmer(b: &mut Bencher) {
        b.iter(|| {
            let mut trimmer = TcTool::new_ng_trimmer(6, false);
            trimmer.process_directory();
        });
    }


    #[bench]
    #[ignore]
    fn bench_process_two(b: &mut Bencher) {
        b.iter(|| {
            let mut publisher = TcTool::new_v1_publisher(6, false);
            let mut ng_consumer = TcTool::new_ng_consumer(6, false);

            let handle_pub = thread::spawn(move || {
                publisher.process_directory();
            });

            let handle_consumer = thread::spawn(move || {
                ng_consumer.process_directory();
            });

            handle_pub.join().unwrap();
            handle_consumer.join().unwrap();
        });
    }

    #[bench]
    #[ignore]
    fn bench_process_three(b: &mut Bencher) {
        b.iter(|| {
            let mut ng_pub = TcTool::new_ng_publisher(6, false);
            let mut ng_con = TcTool::new_ng_consumer(6, false);
            let mut v1_pub = TcTool::new_v1_publisher(6, false);

            let h_pub = thread::spawn(move || {
                ng_pub.process_directory();
            });

            let h_con = thread::spawn(move || {
                ng_con.process_directory();
            });

            let h_v1_pub = thread::spawn(move || {
                v1_pub.process_directory();
            });

            h_pub.join().unwrap();
            h_con.join().unwrap();
            h_v1_pub.join().unwrap();
        });
    }
}
