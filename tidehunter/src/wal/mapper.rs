use super::layout::WalLayout;
use super::position::{MapId, WalFileId};
use super::tracking_mmap::TrackingMMapMut;
use super::{Map, Wal, WalFiles};
use crate::metrics::{MetricIntGauge, Metrics};
use crate::wal::syncer::WalSyncer;
use arc_swap::ArcSwap;
use std::collections::{BTreeMap, HashSet};
use std::fs::File;
use std::path::PathBuf;
use std::sync::{Arc, mpsc};
use std::thread;
use std::thread::JoinHandle;
use std::time::Instant;

pub(crate) enum WalMapperMessage {
    MapFinalized(MapId),
    MinWalPositionUpdated(u64, mpsc::SyncSender<()>),
    DeleteFiles(Vec<WalFileId>, mpsc::SyncSender<()>),
}

pub(crate) struct WalMapper {
    jh: Option<JoinHandle<()>>,
    sender: Option<mpsc::SyncSender<WalMapperMessage>>,
}

struct WalMapperThread {
    receiver: mpsc::Receiver<WalMapperMessage>,
    maps_arc: Arc<ArcSwap<WalMaps>>,
    maps: WalMaps,
    files: Arc<ArcSwap<WalFiles>>,
    layout: WalLayout,
    syncer: WalSyncer,
    metrics: Arc<Metrics>,
    unlinker: UnlinkWorker,
}
const INITIAL_MAPS_BUFFER: usize = 2;

/// Background worker that performs `remove_file` calls off the mapper
/// thread. Sparse GC can produce large delete batches; running the syscalls
/// inline would block `MapFinalized` processing long enough for the writer
/// to time out in `get_writeable_map`.
struct UnlinkWorker {
    sender: Option<mpsc::Sender<PathBuf>>,
    jh: Option<JoinHandle<()>>,
}

impl UnlinkWorker {
    fn start() -> Self {
        let (sender, receiver) = mpsc::channel::<PathBuf>();
        let jh = thread::Builder::new()
            .name("wal-unlinker".to_string())
            .spawn(move || {
                for path in receiver {
                    if path.exists() {
                        std::fs::remove_file(&path).expect("Failed to remove wal file");
                    }
                }
            })
            .expect("failed to start wal-unlinker thread");
        Self {
            sender: Some(sender),
            jh: Some(jh),
        }
    }

    fn unlink(&self, path: PathBuf) {
        // The receiver is only dropped when the worker thread exits, which
        // only happens after this struct is dropped. `send` cannot fail in
        // normal operation.
        let _: Result<(), mpsc::SendError<PathBuf>> = self
            .sender
            .as_ref()
            .expect("UnlinkWorker dropped")
            .send(path);
    }
}

impl Drop for UnlinkWorker {
    fn drop(&mut self) {
        // Closing the sender lets the worker drain its queue and exit.
        self.sender.take();
        if let Some(jh) = self.jh.take() {
            crate::thread_util::join_thread_with_timeout(jh, "wal-unlinker", 10);
        }
    }
}

#[derive(Clone, Default)]
pub struct WalMaps {
    // todo make it vec + min map id
    maps: BTreeMap<MapId, Map>,
}

impl WalMapper {
    pub fn start(
        maps_arc: Arc<ArcSwap<WalMaps>>,
        files: Arc<ArcSwap<WalFiles>>,
        layout: WalLayout,
        syncer: WalSyncer,
        metrics: Arc<Metrics>,
    ) -> Self {
        let maps = maps_arc.load();
        assert!(
            !maps.maps.is_empty(),
            "Should not pass empty maps to WalMapper::start"
        );
        let min_max_maps = INITIAL_MAPS_BUFFER + 1;
        if layout.max_maps < min_max_maps {
            panic!(
                "Provided layout with max_maps {}, minimum allowed {min_max_maps}",
                layout.max_maps
            );
        }
        let maps = WalMaps::clone(&maps);
        let (sender, receiver) = mpsc::sync_channel(5);
        let this = WalMapperThread {
            maps,
            maps_arc,
            files,
            layout,
            receiver,
            metrics,
            syncer,
            unlinker: UnlinkWorker::start(),
        };
        let jh = thread::Builder::new()
            .name("wal-mapper".to_string())
            .spawn(move || this.run())
            .expect("failed to start wal-mapper thread");
        let sender = Some(sender);
        let jh = Some(jh);
        Self { jh, sender }
    }

    #[cfg(test)]
    pub fn new_unstarted() -> (Self, mpsc::Receiver<WalMapperMessage>) {
        let (sender, receiver) = mpsc::sync_channel(1024);
        (
            Self {
                jh: None,
                sender: Some(sender),
            },
            receiver,
        )
    }

    pub fn map_finalized(&self, map_id: MapId) {
        self.sender
            .as_ref()
            .expect("Wal mapper dropped")
            .send(WalMapperMessage::MapFinalized(map_id))
            .ok();
    }

    pub fn min_wal_position_updated(&self, watermark: u64) {
        let (tx, rx) = mpsc::sync_channel(1);
        self.sender
            .as_ref()
            .expect("Wal mapper dropped")
            .send(WalMapperMessage::MinWalPositionUpdated(watermark, tx))
            .ok();
        // Block until the mapper thread finishes deleting files and updating
        // the file set. This prevents a race where a subsequent operation
        // (e.g. the next relocation run, or another `delete_files` call)
        // iterates or rewrites WAL files that are concurrently being deleted.
        rx.recv().ok();
    }

    /// Delete a specific set of WAL files. Files that still have any map in
    /// `WalMaps` (writeable or finalized) are skipped by the mapper thread
    /// and will be reconsidered by the next snapshot once their map has
    /// been evicted. Blocks until the mapper has removed the deletable
    /// entries from the file map; the actual `remove_file` syscalls run on
    /// a background unlink worker, so subsequent `file_ids()` calls see the
    /// deletion immediately even though the dirents may not be gone yet.
    pub fn delete_files(&self, files: Vec<WalFileId>) {
        let (tx, rx) = mpsc::sync_channel(1);
        self.sender
            .as_ref()
            .expect("Wal mapper dropped")
            .send(WalMapperMessage::DeleteFiles(files, tx))
            .ok();
        rx.recv().ok();
    }
}

impl Drop for WalMapper {
    fn drop(&mut self) {
        self.sender.take();
        if let Some(jh) = self.jh.take() {
            crate::thread_util::join_thread_with_timeout(jh, "wal-mapper", 10);
        }
    }
}

impl WalMapperThread {
    pub fn run(mut self) {
        let mut map_id = *self
            .maps
            .maps
            .last_key_value()
            .expect("Can't start wal mapper with empty maps")
            .0;
        for _ in 0..INITIAL_MAPS_BUFFER {
            map_id = map_id.next_map();
            self.make_map(map_id);
        }
        while let Ok(message) = self.receiver.recv() {
            let timer = Instant::now();
            match message {
                WalMapperMessage::MapFinalized(map_to_sync_id) => {
                    let map_to_sync = self.maps.maps.get_mut(&map_to_sync_id);
                    let Some(map_to_sync) = map_to_sync else {
                        // It is possible (mostly in tests) that map is removed
                        // via min_wal_position_updated before it is finalized.
                        // In this case we simply drop the map since underlining file
                        // is already deleted.
                        continue;
                    };
                    map_to_sync.writeable = false;
                    self.syncer.send(
                        map_to_sync.clone(),
                        self.layout.map_range(map_to_sync_id).end,
                    );
                    map_id = map_id.next_map();
                    self.make_map(map_id);
                }
                WalMapperMessage::MinWalPositionUpdated(watermark, _cb) => {
                    self.min_wal_position_updated(watermark);
                    // dropping _cb to release receiver
                }
                WalMapperMessage::DeleteFiles(to_delete, _cb) => {
                    self.delete_files(&to_delete);
                    // dropping _cb to release receiver
                }
            }
            self.metrics
                .wal_mapper_time_mcs
                .inc_by(timer.elapsed().as_micros() as u64);
        }
    }

    /// Delete files forming a prefix below `watermark`.
    fn min_wal_position_updated(&mut self, watermark: u64) {
        let wal_files = self.files.load();
        let mut to_delete = Vec::new();
        for &file_id in wal_files.files.keys() {
            if self.layout.wal_file_range(file_id).end > watermark {
                break;
            }
            to_delete.push(file_id);
        }
        drop(wal_files);
        if !to_delete.is_empty() {
            self.delete_files(&to_delete);
        }
    }

    /// Delete the specific set of files (gaps allowed). Files that still
    /// have a map in `WalMaps` are skipped — that covers the writer's
    /// current file and its lookahead (writeable maps) as well as any
    /// finalized maps still in the LRU. Skipping is safe: the snapshot
    /// path recomputes the deletable set each time it runs, so a file that
    /// is reclaimable but currently mapped will be picked up by the next
    /// snapshot once its map has been evicted by `WalMaps::pop_first`.
    fn delete_files(&mut self, to_delete: &[WalFileId]) {
        let wal_files = self.files.load();
        let mapped_files: HashSet<WalFileId> = self
            .maps
            .maps
            .keys()
            .map(|map_id| self.layout.file_for_map(*map_id))
            .collect();
        let mut actual: Vec<WalFileId> = to_delete
            .iter()
            .copied()
            .filter(|id| !mapped_files.contains(id) && wal_files.files.contains_key(id))
            .collect();
        if actual.is_empty() {
            return;
        }
        actual.sort();
        actual.dedup();
        // Drop the entries from the file map first — subsequent `file_ids()`
        // and `get_checked()` see the deletion immediately. The actual
        // `remove_file` syscalls are dispatched to the unlink worker so a
        // large batch doesn't keep this thread out of `MapFinalized` for
        // long. In-flight readers retain their `Arc<File>` clones, and Unix
        // unlink keeps the inode alive until the last reference drops.
        let new_files = wal_files.without_files(&actual);
        self.files.store(Arc::new(new_files));
        for id in &actual {
            self.unlinker
                .unlink(self.layout.wal_file_name(&wal_files.base_path, *id));
        }
        // No `WalMaps` update needed: the `mapped_files` filter above already
        // guarantees every entry in `actual` is unmapped, so there is nothing
        // in `self.maps` referencing the just-deleted files.
    }

    fn make_map(&mut self, map_id: MapId) {
        let file_id = self.layout.file_for_map(map_id);
        let mut files = self.files.load();
        if file_id > files.current_file_id() {
            assert_eq!(file_id, WalFileId(files.current_file_id().0 + 1));
            let new_file_path = self.layout.wal_file_name(&files.base_path, file_id);
            let new_file = Wal::open_file(&new_file_path, &self.layout)
                .expect("Failed to create new wal file");
            let new_wal_files = files.with_file(file_id, Arc::new(new_file));
            self.files.store(Arc::new(new_wal_files));
            files = self.files.load();
        }
        let file = files.get(file_id);
        Wal::extend_to_map_id(&self.layout, file, map_id).expect("Failed to extend wal file");
        self.maps.map(
            file,
            &self.layout,
            map_id,
            self.metrics.wal_mmap_bytes.clone(),
        );

        self.publish_maps();
    }

    fn publish_maps(&self) {
        let new_maps = self.maps.clone();
        self.maps_arc.store(Arc::new(new_maps));
    }
}

impl WalMaps {
    pub fn get(&self, map_id: MapId) -> Option<&Map> {
        self.maps.get(&map_id)
    }

    pub fn map(
        &mut self,
        file: &File,
        layout: &WalLayout,
        map_id: MapId,
        wal_mmap_bytes: MetricIntGauge,
    ) -> &Map {
        let range = layout.map_range(map_id);
        let data = unsafe {
            let mut options = memmap2::MmapOptions::new();
            options
                .offset(layout.offset_in_wal_file(range.start))
                .len(layout.frag_size as usize);
            let mmap = options
                .populate()
                .map_mut(file)
                .expect("Failed to mmap on wal file");
            TrackingMMapMut::new(mmap, wal_mmap_bytes).into()
        };
        let map = Map {
            id: map_id,
            writeable: true,
            data,
        };
        assert!(self.maps.insert(map_id, map).is_none());
        if self.maps.len() > layout.max_maps {
            self.maps.pop_first();
        }
        self.maps.get(&map_id).unwrap()
    }
}
