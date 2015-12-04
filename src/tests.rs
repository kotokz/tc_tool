#[cfg(test)]
mod tests {

    use tclogparser::*;
    use tradecache::*;
    use tcresult::*;
    use test::Bencher;


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
            // let (pub_time, watermark) = match tc.process_line(&line) {
            //     (Some(pub_time), Some(watermark)) => (pub_time, watermark),
            //     _ => panic!("Regex failure!"),
            // };
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

    #[test]
    fn can_increase_hour_count() {
        let mut result = TcHourResult::new();
        result.increase_count("2015-11-09 02:01:03", "2015-11-09 01:29:32");
        result.increase_count("2015-11-09 02:02:03", "2015-11-09 01:19:32");
        result.increase_count("2015-11-09 02:03:03", "2015-11-09 01:09:32");
        result.increase_count("2015-11-09 01:04", "2015-11-09 01:09:32");
        result.increase_count("2015-11-09 01:05", "2015-11-09 01:09:32");
        result.increase_count("nothing here", "test test");
        result.increase_count("nothing here", "");
        result.increase_count("", "");
        let c = result.increase_count("2015-11-09 01:06", "2015-11-09 01:09:32");

        // return value equals to the map length
        assert_eq!(c, result.0.len() as usize);

        verify_result_set(&result);
    }

    #[test]
    fn can_increase_trimmer_hour_count() {
        let mut result = TcHourResult::new();
        result.increase_count("2015-11-09 02:01:03", "");
        result.increase_count("2015-11-09 02:02:03", "");
        result.increase_count("2015-11-09 02:03", "");
        result.increase_count("2015-11-09 01:04", "");
        result.increase_count("2015-11-09 01:05", "");
        let c = result.increase_count("2015-11-09 01:06", "");

        // return value equals to the map length
        assert_eq!(c, result.0.len() as usize);

        verify_result_set(&result);
    }

    fn verify_result_set(result: &TcHourResult) {

        for (_, val) in &result.0 {
            // logs can be porperly categoried in map
            assert_eq!(3, val.done);
        }

        let keys: Vec<_> = result.0.keys().into_iter().cloned().collect();

        // keys are in order
        assert_eq!(keys, [2015110901, 2015110902]);


        let keys_2 = result.keys_skip_first();
        let ordered_keys_2 = [2015110902];
        // The old key can be removed correctly
        assert_eq!(keys_2, ordered_keys_2);

    }

    // #[bench]
    // fn bench_tc_v1_process(b: &mut Bencher) {
    //     b.iter(|| {
    //         let mut publisher = new_v1_publisher();
    //         publisher.process_directory(6);
    //     });
    // }

    // #[bench]
    // fn bench_tc_ng_process(b: &mut Bencher) {
    //     b.iter(|| {
    //         let mut consumer = new_ng_consumer();
    //         consumer.process_directory(6);
    //     });
    // }

    // #[bench]
    // fn bench_tc_ng_trimmer(b: &mut Bencher) {
    //     b.iter(|| {
    //         let mut trimmer = new_ng_trimmer();
    //         trimmer.process_directory(6);
    //     });
    // }


    // #[bench]
    // fn bench_process_two(b: &mut Bencher) {
    //     b.iter(|| {
    //         let mut publisher = new_v1_publisher();
    //         let mut ng_consumer = new_ng_consumer();

    //         let handle_pub = thread::spawn(move || {
    //             publisher.process_directory(6);
    //         });

    //         let handle_consumer = thread::spawn(move || {
    //             ng_consumer.process_directory(6);
    //         });

    //         handle_pub.join().unwrap();
    //         handle_consumer.join().unwrap();
    //     });
    // }

    // #[bench]
    // fn bench_process_three(b: &mut Bencher) {
    //     b.iter(|| {
    //         let mut ng_pub = new_ng_publisher();
    //         let mut ng_con = new_ng_consumer();
    //         let mut v1_pub = new_v1_publisher();

    //         let h_pub = thread::spawn(move || {
    //             ng_pub.process_directory(6);
    //         });

    //         let h_con = thread::spawn(move || {
    //             ng_con.process_directory(6);
    //         });

    //         let h_v1_pub = thread::spawn(move || {
    //             v1_pub.process_directory(6);
    //         });

    //         h_pub.join().unwrap();
    //         h_con.join().unwrap();
    //         h_v1_pub.join().unwrap();
    //     });
    // }
}