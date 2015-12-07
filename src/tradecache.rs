use glob::glob;
use std::io::{BufReader, BufRead};
use std::path::PathBuf;
use std::fs::File;

use tclogparser::*;
use tcresult::*;
use regex::Regex;
use tcerror::TcError;

pub fn new_ng_publisher() -> TcTool {
    TcTool {
        name: "NG_Publisher".to_owned(),
        path: "C:/working/projects/nimproj/logs/ng/publisher/publish.log*".to_owned(),
        pattern: RegexParser(Some(Regex::new(r"docWriteTime=([^}]+?)}").unwrap())),
        result: TcResultEnum::HourResult(TcHourResult::new()),
    }
}
pub fn new_ng_consumer() -> TcTool {
    TcTool {
        name: "NG_Consumer".to_owned(),
        path: "C:/working/projects/nimproj/logs/ng/consumer/consumer.log*".to_owned(),
        // path: "E:/TradeCache/SophisConsumer-release/logs/prod/consumer.log*".to_owned(),
        pattern: RegexParser(Some(Regex::new(r"timestamp=(.{28})eve").unwrap())),
        result: TcResultEnum::HourResult(TcHourResult::new()),
    }
}

pub fn new_ng_trimmer() -> TcTool {
    TcTool {
        name: "NG_Trimmer".to_owned(),
        path: "C:/working/projects/nimproj/logs/ng/tc/tradecache.log*".to_owned(),
        pattern: RegexParser(Some(Regex::new(r"committed").unwrap())),
        result: TcResultEnum::HourResult(TcHourResult::new()),
    }
}

pub fn new_v1_publisher() -> TcTool {
    TcTool {
        name: "V1_Publisher".to_owned(),
        path: "C:/working/projects/nimproj/logs/v1/publisher/publish.log*".to_owned(),
        pattern: RegexParser(Some(Regex::new(r"DocWriteTime=([^,]+?),").unwrap())),
        result: TcResultEnum::BatchResult(TcHourResult::new()),
    }
}

pub struct TcTool {
    name: String,
    path: String,
    pattern: RegexParser,
    result: TcResultEnum,
}

impl TcTool {
    fn increase_result(&mut self, time: &str, watermark: &str) -> usize {
        match self.result {
            TcResultEnum::HourResult(ref mut h) => h.increase_count(time, watermark),
            TcResultEnum::BatchResult(ref mut h) => h.increase_count(time, watermark),
        }
    }

    fn get_result(&self) -> Vec<usize> {
        match self.result {
            TcResultEnum::HourResult(ref h) => h.keys_skip_first(),
            TcResultEnum::BatchResult(ref h) => h.keys_skip_first(),
        }
    }

    fn get_value(&self, key: usize) -> Option<&TcStat> {
        match self.result {
            TcResultEnum::HourResult(ref h) => h.get_value(key),
            TcResultEnum::BatchResult(ref h) => h.get_value(key),
        }
    }

    /// sort the path base on extension. if no extension then assume it as 0
    /// for example, make sure the file follow below order
    /// tradecache.log
    /// tradecache.log.1
    /// tradecache.log.2
    /// ...
    /// tradecache.log.10
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
    pub fn process_directory(&mut self, count: usize) {
        let results: Vec<_> = glob(&self.path).unwrap().filter_map(|r| r.ok()).collect();
        let results = Self::sorted_path(&results);

        for name in results {
            let file = File::open(&name).expect("Failed to open log file.");
            let mut c_count = 0;
            for line in BufReader::new(file).lines().filter_map(|line| line.ok()) {
                c_count = match self.process_line(&line) {
                    (Some(pub_time), Some(watermark)) => self.increase_result(pub_time, watermark),
                    (Some(pub_time), None) => self.increase_result(pub_time, ""),
                    _ => continue,
                };

            }
            // we have enough samples, stop!
            if c_count > count {
                return;
            }
        }
    }

    pub fn print_result(&self) {
        // skip the first value, normally the record too old so likely to be incomplete.
        for (count, key) in self.get_result().iter().rev().enumerate() {
            match self.get_value(*key) {
                Some(val) if count == 0 => {
                    println!("{}-{},{}", self.name, count, val.to_str(true));
                }
                Some(val) => println!("{}-{},{}", self.name, count, val.to_str(false)),
                None => println!("{}-{},{}", self.name, count, "missing value"),
            };
        }
    }
}

impl TcLogParser for TcTool {
    fn match_line<'a, 'b>(&'a self, line: &'b str) -> Result<Option<&'b str>, TcError> {
        self.pattern.match_line(line)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tclogparser::*;
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
            let (pub_time, watermark) = tc.process_line(&line);
            assert_eq!(pub_time, None);
            assert_eq!(watermark, None);
        }
    }

    #[bench]
    fn bench_process_line_v1_publisher(b: &mut Bencher) {
        let mut tc = new_v1_publisher();

        let line = "2015-11-08 09:07:54,679 JMS publish : \
                    70330098:400884:10003236:10001030/288641449 msg_len=40208 meta: \
                    {OutOfSequencePublish=false, LastMovementStatus=VerifiedBO, \
                    EventId=288641449, LinkedPositionId=, InstrumentType=D, \
                    AuditDateTime=20151028 06:46:06, SophisSystemId=PARIS_SOPHIS, \
                    LastAmendedMovement=422345385, tradeEvent=Update, Allotment=Conv. Bond, \
                    MovementCount=20, FeedName=sophis, LegalEntity=10001030, FolioId=400884, \
                    DocWriteTime=20151028 07:17:17, PositionId=70330098:400884:10003236:10001030, \
                    SummitId=6996768, Counterparty=10003236, MovementStatus=VerifiedBO}, \
                    Destination name: \
                    queue://PGBLHDEIS1/GB_DEIS.GB_TRDC.GB_TRDC.TRD_SOPHML?persistence=-1";

        b.iter(|| {
            let (pub_time, watermark) = tc.process_line(&line);
            assert_eq!(pub_time, Some("2015-11-08 09:07:54"));
            assert_eq!(watermark, Some("20151028 07:17:17"));

            process_line_incorrect_tester(&mut tc);
        });
    }

    #[bench]
    fn bench_process_line_ng_consumer(b: &mut Bencher) {
        let mut tc = new_ng_consumer();

        let line = "2015-09-11 09:28:49,842 INFO \
                    [org.springframework.jms.listener.DefaultMessageListenerContainer#0-1] \
                    com.hsbc.cibm.tradecache.sophisconsumer.CachedObjectUpdater - Updating \
                    InstrumentGenericUpdateEvent \
                    {id=I|77787473;type=InstrumentGenericUpdate;version=45139252;timestamp=Fri \
                    Sep 11 09:28:49 BST 2015eventId=45139252}";
        b.iter(|| {
            let (pub_time, watermark) = tc.process_line(&line);
            assert_eq!(pub_time, Some("2015-09-11 09:28:49"));
            assert_eq!(watermark, Some("Fri Sep 11 09:28:49 BST 2015"));

            process_line_incorrect_tester(&mut tc);
        });
    }

    #[bench]
    fn bench_process_line_ng_publisher(b: &mut Bencher) {
        let mut tc = new_ng_publisher();

        let line = "2015-09-09 02:35:01,024 JMS publish : I|75442050/45035056 msg_len=197258 \
                    meta: {tradeType=PositionEvent, OutOfSequencePublish=false, packageModel=CCF \
                    Package, legalEntity=, tradeId=I|75442050, feedName=SophisFeed, transition=, \
                    eventType=, allotmentName=, cptyTreatsId=, \
                    tradeEvent=InstrumentVersionUpdate, positionStatus=, auditDateTime=2015-09-09 \
                    00:13:46, movementCount=0.0, bsmTrade=, sourceSystemTradeId=75442050, \
                    eventId=45035056, movementStatus=, tradeVersion=45035056, folioPath=, \
                    systemId=PARIS_SOPHIS, instrumentType=, docWriteTime=2015-09-09 01:35:03}, \
                    Destination name: \
                    topic://PRV_TCACHE/SED/SOPHISML/PS_FOLIO/EVENT?brokerDurSubQueue=SYSTEM.JMS.D.\
                    GB_TRDC&persistence=-1&brokerVersion=1&XMSC_WMQ_BROKER_PUBQ_QMGR=PGBLHDEIS1&br\
                    okerCCDurSubQueue=SYSTEM.JMS.D.CC.GB_TRDC";
        b.iter(|| {
            let (pub_time, watermark) = tc.process_line(&line);
            assert_eq!(pub_time, Some("2015-09-09 02:35:01"));
            assert_eq!(watermark, Some("2015-09-09 01:35:03"));

            process_line_incorrect_tester(&mut tc);
        });
    }

    #[bench]
    fn bench_process_line_ng_trimmer(b: &mut Bencher) {
        let mut tc = new_ng_trimmer();

        let line = "2015-09-10 21:06:34,594 INFO  [schedulerFactoryBean_Worker-4] \
                    cachemaint.CacheMaintainerImpl (CacheMaintainerImpl.java:146)     - committed \
                    deletes to disk cache";
        b.iter(|| {
            let (pub_time, watermark) = tc.process_line(&line);
            assert_eq!(pub_time, Some("2015-09-10 21:06:34"));
            assert_eq!(watermark, None);

            process_line_incorrect_tester(&mut tc);
        });
    }


    #[bench]
    #[ignore]
    fn bench_tc_v1_process(b: &mut Bencher) {
        b.iter(|| {
            let mut publisher = new_v1_publisher();
            publisher.process_directory(6);
        });
    }

    #[bench]
    #[ignore]
    fn bench_tc_ng_process(b: &mut Bencher) {
        b.iter(|| {
            let mut consumer = new_ng_consumer();
            consumer.process_directory(6);
        });
    }

    #[bench]
    #[ignore]
    fn bench_tc_ng_trimmer(b: &mut Bencher) {
        b.iter(|| {
            let mut trimmer = new_ng_trimmer();
            trimmer.process_directory(6);
        });
    }


    #[bench]
    #[ignore]
    fn bench_process_two(b: &mut Bencher) {
        b.iter(|| {
            let mut publisher = new_v1_publisher();
            let mut ng_consumer = new_ng_consumer();

            let handle_pub = thread::spawn(move || {
                publisher.process_directory(6);
            });

            let handle_consumer = thread::spawn(move || {
                ng_consumer.process_directory(6);
            });

            handle_pub.join().unwrap();
            handle_consumer.join().unwrap();
        });
    }

    #[bench]
    #[ignore]
    fn bench_process_three(b: &mut Bencher) {
        b.iter(|| {
            let mut ng_pub = new_ng_publisher();
            let mut ng_con = new_ng_consumer();
            let mut v1_pub = new_v1_publisher();

            let h_pub = thread::spawn(move || {
                ng_pub.process_directory(6);
            });

            let h_con = thread::spawn(move || {
                ng_con.process_directory(6);
            });

            let h_v1_pub = thread::spawn(move || {
                v1_pub.process_directory(6);
            });

            h_pub.join().unwrap();
            h_con.join().unwrap();
            h_v1_pub.join().unwrap();
        });
    }
}
