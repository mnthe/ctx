use super::*;
use ctx_daemon_runtime::NativeWatchResult;

struct MetadataFixture {
    _temp: tempfile::TempDir,
    home: PathBuf,
    data_root: PathBuf,
    source_root: PathBuf,
    source: PathBuf,
    nested: PathBuf,
    route: SourceRouteIdentity,
    authority: RwLock<WatchAuthority>,
    counters: Mutex<WatchCounters>,
    wakeup: DaemonWakeup,
    observed: Arc<Mutex<SourceWatchBatch>>,
}

impl MetadataFixture {
    fn new() -> Self {
        let temp = tempfile::tempdir().unwrap();
        let home = temp.path().join("home");
        let data_root = home.join(".ctx");
        let source_root = home.join(".codex/sessions");
        let nested = source_root.join("nested");
        let source = nested.join("session.jsonl");
        let healthy_root = home.join(".claude/projects");
        fs::create_dir_all(&data_root).unwrap();
        fs::create_dir_all(&nested).unwrap();
        fs::create_dir_all(&healthy_root).unwrap();
        fs::write(
            data_root.join(CONFIG_FILE),
            b"[indexing]\nmode = \"auto\"\n",
        )
        .unwrap();
        fs::write(&source, b"{\"event\":1}\n").unwrap();
        let catalog = watch_catalog([
            catalog_route(
                CaptureProvider::Codex,
                source_root.clone(),
                "codex_session_jsonl_tree",
            ),
            catalog_route(
                CaptureProvider::Claude,
                healthy_root,
                "claude_projects_jsonl_tree",
            ),
            catalog_route(
                CaptureProvider::OpenCode,
                home.join(".opencode/history.db"),
                "opencode_sqlite",
            ),
        ]);
        let route = catalog
            .routes_overlapping_path(&source)
            .into_iter()
            .next()
            .unwrap();
        assert_eq!(catalog.routes_overlapping_path(&home).len(), 3);
        let wakeup = DaemonWakeup::default();
        let observed = Arc::new(Mutex::new(SourceWatchBatch::default()));
        let sink = Arc::clone(&observed);
        wakeup.install_source_watch_sink(Arc::new(move |batch| {
            sink.lock().unwrap().merge(batch.clone());
        }));
        Self {
            _temp: temp,
            home,
            data_root: data_root.clone(),
            source_root,
            source,
            nested,
            route,
            authority: RwLock::new(watch_authority(&data_root, catalog)),
            counters: Mutex::new(WatchCounters::default()),
            wakeup,
            observed,
        }
    }

    fn observe(&self, event: NativeWatchResult, sequence: u64) -> SourceWatchBatch {
        record_and_observe_watch_event(
            &self.authority,
            &self.counters,
            &self.wakeup,
            &self.data_root,
            &self.data_root.join("daemon"),
            event,
            EventWatermark::new(23, sequence),
        )
    }
}

#[test]
fn ancestor_and_sibling_write_times_do_not_invalidate_sources_or_config() {
    let fixture = MetadataFixture::new();
    for (sequence, path) in [
        fixture.home.clone(),
        fixture.home.join(".claude.json"),
        fixture.home.join(".claude.json.lock"),
        fixture.home.join(".claude.json.tmp"),
    ]
    .into_iter()
    .enumerate()
    {
        let batch = fixture.observe(
            Ok(NativeWatchEvent::metadata(vec![path])),
            sequence as u64 + 1,
        );
        assert!(batch.is_empty(), "{batch:?}");
    }
    assert!(fixture.observed.lock().unwrap().is_empty());
    assert!(fixture.wakeup.pending_source_watch().is_empty());
    assert_eq!(fixture.counters.lock().unwrap().raw_events, 0);
}

#[test]
fn exact_source_write_time_and_content_still_record_the_changed_member() {
    let fixture = MetadataFixture::new();
    for (sequence, event) in [
        NativeWatchEvent::metadata(vec![fixture.source.clone()]),
        NativeWatchEvent::content(vec![fixture.source.clone()]),
    ]
    .into_iter()
    .enumerate()
    {
        let batch = fixture.observe(Ok(event), sequence as u64 + 1);
        assert_eq!(batch.routes.len(), 1);
        assert!(batch.routes.contains_key(&fixture.route));
        assert_eq!(
            batch.members.get(&fixture.route),
            Some(&BTreeSet::from([fixture.source.clone()]))
        );
        assert!(batch.reconcile.is_none());
        assert!(!batch.rearm);
    }
}

#[test]
fn mixed_write_time_paths_keep_the_actual_source_change() {
    let fixture = MetadataFixture::new();
    let batch = fixture.observe(
        Ok(NativeWatchEvent::metadata(vec![
            fixture.home.clone(),
            fixture.source.clone(),
        ])),
        1,
    );
    assert_eq!(batch.routes.len(), 1);
    assert_eq!(
        batch.members.get(&fixture.route),
        Some(&BTreeSet::from([fixture.source.clone()]))
    );
    assert!(batch.reconcile.is_none());
    assert!(!batch.rearm);
}

#[test]
fn creating_a_missing_source_ancestor_keeps_discovery_route_local() {
    let fixture = MetadataFixture::new();
    let parent = fixture.home.join(".opencode");
    let missing_source = parent.join("history.db");
    let expected = fixture
        .authority
        .read()
        .unwrap()
        .catalog
        .snapshot()
        .unwrap()
        .routes_overlapping_path(&missing_source);
    assert_eq!(expected.len(), 1);
    fs::create_dir(&parent).unwrap();
    let batch = fixture.observe(Ok(NativeWatchEvent::requiring_rearm(vec![parent])), 1);
    assert_eq!(
        batch.routes.keys().cloned().collect::<BTreeSet<_>>(),
        expected
    );
    assert!(batch.members.is_empty());
    assert!(batch.reconcile.is_none());
    assert!(batch.rearm);
}

#[test]
fn exact_config_write_time_still_requests_catalog_reconciliation() {
    let fixture = MetadataFixture::new();
    let batch = fixture.observe(
        Ok(NativeWatchEvent::metadata(vec![fixture
            .data_root
            .join(CONFIG_FILE)])),
        1,
    );
    assert_eq!(batch.reconcile, Some(EventWatermark::new(23, 1)));
    assert_eq!(fixture.observed.lock().unwrap().reconcile, batch.reconcile);
}

#[test]
fn write_time_on_source_root_or_inner_directory_remains_route_local() {
    let fixture = MetadataFixture::new();
    for (sequence, path) in [fixture.source_root.clone(), fixture.nested.clone()]
        .into_iter()
        .enumerate()
    {
        let batch = fixture.observe(
            Ok(NativeWatchEvent::metadata(vec![path])),
            sequence as u64 + 1,
        );
        assert_eq!(batch.routes.len(), 1);
        assert!(batch.routes.contains_key(&fixture.route));
        assert!(batch.members.is_empty());
        assert!(batch.reconcile.is_none());
        assert!(!batch.rearm);
    }
}

#[test]
fn structural_and_unknown_ancestor_events_keep_conservative_reconciliation() {
    for requires_rearm in [false, true] {
        let fixture = MetadataFixture::new();
        let event = if requires_rearm {
            NativeWatchEvent::requiring_rearm(vec![fixture.home.clone()])
        } else {
            NativeWatchEvent::ordinary(vec![fixture.home.clone()])
        };
        let batch = fixture.observe(Ok(event), 1);
        assert_eq!(batch.reconcile, Some(EventWatermark::new(23, 1)));
        assert_eq!(batch.rearm, requires_rearm);
    }
}

#[test]
fn unknown_ancestor_metadata_keeps_descendant_access_recovery_route_local() {
    let fixture = MetadataFixture::new();
    // Permissions and ambiguous metadata normalize to Unknown because they
    // can change access to sources below the reported directory.
    let batch = fixture.observe(
        Ok(NativeWatchEvent::ordinary(vec![fixture
            .home
            .join(".codex")])),
        1,
    );
    assert_eq!(batch.routes.len(), 1);
    assert!(batch.routes.contains_key(&fixture.route));
    assert!(batch.members.is_empty());
    assert!(batch.reconcile.is_none());
    assert!(!batch.rearm);
}

#[test]
fn write_time_does_not_downgrade_coalesced_structural_uncertainty() {
    let fixture = MetadataFixture::new();
    fixture.observe(
        Ok(NativeWatchEvent::requiring_rearm(vec![fixture
            .home
            .clone()])),
        1,
    );
    fixture.observe(
        Ok(NativeWatchEvent::metadata(vec![fixture.home.clone()])),
        2,
    );
    fixture.observe(
        Ok(NativeWatchEvent::metadata(vec![fixture.source.clone()])),
        3,
    );
    let observed = fixture.observed.lock().unwrap();
    assert_eq!(observed.reconcile, Some(EventWatermark::new(23, 1)));
    assert!(observed.rearm);
    assert!(observed.routes.is_empty());
    assert!(observed.members.is_empty());
}

#[test]
fn write_time_filter_does_not_suppress_rescan_or_backend_errors() {
    let fixture = MetadataFixture::new();
    fixture.observe(
        Ok(NativeWatchEvent::metadata(vec![fixture.home.clone()])),
        1,
    );
    let rescan = fixture.observe(Ok(NativeWatchEvent::rescan(vec![fixture.home.clone()])), 2);
    assert_eq!(rescan.reconcile, Some(EventWatermark::new(23, 2)));
    assert!(rescan.rearm);
    let error = fixture.observe(Err(NativeWatchError), 3);
    assert_eq!(error.reconcile, Some(EventWatermark::new(23, 3)));
    assert!(error.rearm);
    let counters = fixture.counters.lock().unwrap();
    assert_eq!(counters.rescan_notifications, 1);
    assert_eq!(counters.backend_errors, 1);
}
