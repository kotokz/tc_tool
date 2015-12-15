use glob::glob;
use std::io::{BufReader, BufRead};
use std::path::PathBuf;
use std::fs::File;

use tclogparser::*;

pub struct TcTool {
    name: String,
    path: String,
    pattern: TcParser,
    count: usize,
}

impl TcTool {
    pub fn new_ng_publisher(count: usize, prod: bool) -> TcTool {
        Self::with_regex("NG_Publisher",
                         if prod {
                             "E:/Publisher/sophis2/prod/logs/publish.log*"
                         } else {
                             "C:/working/projects/nimproj/logs/ng/publisher/publish.log*"
                         },
                         r"docWriteTime=([^}]+?)}",
                         None,
                         count)
    }
    pub fn new_ng_consumer(count: usize, prod: bool) -> TcTool {
        Self::with_regex("NG_Consumer",
                         if prod {
                             "E:/TradeCache/SophisConsumer-release/logs/prod/consumer.log*"
                         } else {
                             "C:/working/projects/nimproj/logs/ng/consumer/consumer.log*"
                         },
                         r"timestamp=(.{28})eve",
                         None,
                         count)
    }

    pub fn new_ng_trimmer(count: usize, prod: bool) -> TcTool {
        Self::with_pattern("NG_Trimmer",
                           if prod {
                               "E:/TradeCache/sophis2/prod/logs/tradecache.log*"

                           } else {
                               "C:/working/projects/nimproj/logs/ng/tc/tradecache.log*"
                           },
                           "committed",
                           None,
                           count)
    }

    pub fn new_v1_publisher(count: usize, prod: bool) -> TcTool {
        Self::with_regex("V1_Publisher",
                         if prod {
                             "E:/Publisher/sophis_aggr/prod/logs/publish.log*"
                         } else {
                             "C:/working/projects/nimproj/logs/v1/publisher/publish.log*"
                         },
                         r"DocWriteTime=([^,]+?),",
                         None,
                         count)
    }

    pub fn new_v1_trimmer(count: usize, prod: bool) -> TcTool {
        Self::with_pattern("V1_Trimmer_Batch",
                           if prod {
                               "E:/Tradecache/sophis_aggr/prod/logs/tradecache.log*"
                           } else {
                               "C:/working/projects/nimproj/logs/v1/tcaggr/tradecache.log*"
                           },
                           "committed",
                           Some(r"Context contains (\d+)"),
                           count)
    }

    pub fn new_ng_trimmer_batch(count: usize, prod: bool) -> TcTool {
        Self::with_pattern("NG_Trimmer_Batch",
                           if prod {
                               "E:/TradeCache/sophis2/prod/logs/tradecache.log*"
                           } else {
                               "C:/working/projects/nimproj/logs/ng/tc/tradecache.log*"
                           },
                           "committed",
                           Some(r"Context contains (\d+)"),
                           count)
    }

    pub fn with_regex(name: &str,
                      path: &str,
                      pattern: &str,
                      batch: Option<&str>,
                      count: usize)
                      -> TcTool {
        TcTool {
            name: name.to_owned(),
            path: path.to_owned(),
            pattern: TcParser::new(Some(pattern), None, batch),
            count: count,
        }
    }

    pub fn with_pattern(name: &str,
                        path: &str,
                        pattern: &str,
                        batch: Option<&str>,
                        count: usize)
                        -> TcTool {
        TcTool {
            name: name.to_owned(),
            path: path.to_owned(),
            pattern: TcParser::new(None, Some(pattern), batch),
            count: count,
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
    pub fn process_directory(&mut self) {
        let files: Vec<_> = glob(&self.path).unwrap().filter_map(|r| r.ok()).collect();
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
        self.pattern.print_result(&self.name);
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
            let (pub_time, watermark) = tc.pattern.extract_times(&line);
            assert_eq!(pub_time, None);
            assert_eq!(watermark, None);
        }
    }

    #[bench]
    fn bench_process_line_v1_publisher(b: &mut Bencher) {
        let mut tc = TcTool::new_v1_publisher(6, false);

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
            let (pub_time, watermark) = tc.pattern.extract_times(&line);
            assert_eq!(pub_time, Some("2015-11-08 09:07:54"));
            assert_eq!(watermark, Some("20151028 07:17:17"));

            process_line_incorrect_tester(&mut tc);
        });
    }

    #[bench]
    fn bench_process_line_ng_consumer(b: &mut Bencher) {
        let mut tc = TcTool::new_ng_consumer(6, false);

        let line = "2015-09-11 09:28:49,842 INFO \
                    [org.springframework.jms.listener.DefaultMessageListenerContainer#0-1] \
                    com.hsbc.cibm.tradecache.sophisconsumer.CachedObjectUpdater - Updating \
                    InstrumentGenericUpdateEvent \
                    {id=I|77787473;type=InstrumentGenericUpdate;version=45139252;timestamp=Fri \
                    Sep 11 09:28:49 BST 2015eventId=45139252}";
        b.iter(|| {
            let (pub_time, watermark) = tc.pattern.extract_times(&line);
            assert_eq!(pub_time, Some("2015-09-11 09:28:49"));
            assert_eq!(watermark, Some("Fri Sep 11 09:28:49 BST 2015"));

            process_line_incorrect_tester(&mut tc);
        });
    }

    #[bench]
    fn bench_process_line_ng_publisher(b: &mut Bencher) {
        let mut tc = TcTool::new_ng_publisher(6, false);

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
            let (pub_time, watermark) = tc.pattern.extract_times(&line);
            assert_eq!(pub_time, Some("2015-09-09 02:35:01"));
            assert_eq!(watermark, Some("2015-09-09 01:35:03"));

            process_line_incorrect_tester(&mut tc);
        });
    }

    #[bench]
    fn bench_process_line_ng_trimmer(b: &mut Bencher) {
        let mut tc = TcTool::new_ng_trimmer(6, false);

        let line = "2015-09-10 21:06:34,594 INFO  [schedulerFactoryBean_Worker-4] \
                    cachemaint.CacheMaintainerImpl (CacheMaintainerImpl.java:146)     - committed \
                    deletes to disk cache";
        b.iter(|| {
            let (pub_time, watermark) = tc.pattern.extract_times(&line);
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
