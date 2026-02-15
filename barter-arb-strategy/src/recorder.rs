//! Orderbook snapshot recorder for debugging and replay.
//!
//! Periodically writes orderbook state to JSON files after every N updates
//! per instrument. Enable via `RECORD_SNAPSHOTS=true` environment variable.

use barter_data::books::OrderBook;
use chrono::Utc;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use tracing::{debug, error, info};

/// Records orderbook snapshots to disk for debugging and replay.
pub struct OrderbookRecorder {
    /// Number of updates between snapshots per instrument.
    snapshot_interval: u32,
    /// Per-instrument update counter.
    delta_counts: HashMap<String, u32>,
    /// Directory to write snapshot files.
    output_dir: PathBuf,
    /// Total snapshots written (for logging).
    total_written: u64,
}

impl OrderbookRecorder {
    /// Create a new recorder.
    ///
    /// # Arguments
    /// * `snapshot_interval` - Write a snapshot every N orderbook updates per instrument.
    /// * `output_dir` - Directory to store JSON snapshot files.
    pub fn new(snapshot_interval: u32, output_dir: impl Into<PathBuf>) -> Self {
        let output_dir = output_dir.into();
        if let Err(e) = std::fs::create_dir_all(&output_dir) {
            error!(?output_dir, %e, "Failed to create snapshot output directory");
        } else {
            info!(?output_dir, snapshot_interval, "OrderbookRecorder initialized");
        }
        Self {
            snapshot_interval,
            delta_counts: HashMap::new(),
            output_dir,
            total_written: 0,
        }
    }

    /// Create from environment variables.
    ///
    /// Returns `Some` if `RECORD_SNAPSHOTS=true`, using:
    /// - `SNAPSHOT_INTERVAL` (default: 100)
    /// - `SNAPSHOT_DIR` (default: `./snapshots`)
    pub fn from_env() -> Option<Self> {
        let enabled = std::env::var("RECORD_SNAPSHOTS")
            .map(|v| v == "true" || v == "1")
            .unwrap_or(false);

        if !enabled {
            return None;
        }

        let interval = std::env::var("SNAPSHOT_INTERVAL")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(100);

        let dir = std::env::var("SNAPSHOT_DIR").unwrap_or_else(|_| "./snapshots".to_string());

        Some(Self::new(interval, dir))
    }

    /// Called on every orderbook update. Writes a snapshot if the update count
    /// for this instrument has reached the configured interval.
    pub fn on_orderbook_update(&mut self, instrument: &str, book: &OrderBook) {
        let count = self.delta_counts.entry(instrument.to_string()).or_insert(0);
        *count += 1;

        let should_write = *count >= self.snapshot_interval;
        if should_write {
            *count = 0;
            self.write_snapshot(instrument, book);
        }
    }

    /// Write a snapshot to disk as a JSON file.
    fn write_snapshot(&mut self, instrument: &str, book: &OrderBook) {
        let timestamp = Utc::now().format("%Y%m%dT%H%M%S%.3f");
        // Sanitize instrument name for filesystem
        let safe_name: String = instrument
            .chars()
            .map(|c| if c.is_alphanumeric() || c == '-' || c == '_' { c } else { '_' })
            .collect();
        let filename = format!("{}_{}.json", safe_name, timestamp);
        let path = self.output_dir.join(&filename);

        match serde_json::to_string_pretty(book) {
            Ok(json) => match std::fs::write(&path, json) {
                Ok(()) => {
                    self.total_written += 1;
                    debug!(%instrument, ?path, total = self.total_written, "Snapshot written");
                }
                Err(e) => error!(?path, %e, "Failed to write snapshot"),
            },
            Err(e) => error!(%instrument, %e, "Failed to serialize orderbook"),
        }
    }

    /// Get the output directory.
    pub fn output_dir(&self) -> &Path {
        &self.output_dir
    }

    /// Get total snapshots written.
    pub fn total_written(&self) -> u64 {
        self.total_written
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use barter_data::books::Level;
    use rust_decimal_macros::dec;
    use std::fs;

    #[test]
    fn test_recorder_writes_at_interval() {
        let dir = std::env::temp_dir().join(format!("barter_test_snapshots_{}", std::process::id()));
        let _ = fs::remove_dir_all(&dir);

        let mut recorder = OrderbookRecorder::new(3, &dir);
        let book = OrderBook::new(
            1,
            None,
            vec![Level::new(dec!(0.45), dec!(100))],
            vec![Level::new(dec!(0.46), dec!(50))],
        );

        // First 2 updates: no snapshot written
        recorder.on_orderbook_update("TEST_MARKET", &book);
        recorder.on_orderbook_update("TEST_MARKET", &book);
        assert_eq!(recorder.total_written(), 0);

        // 3rd update: snapshot written
        recorder.on_orderbook_update("TEST_MARKET", &book);
        assert_eq!(recorder.total_written(), 1);

        // Verify file exists
        let files: Vec<_> = fs::read_dir(&dir)
            .unwrap()
            .filter_map(|e| e.ok())
            .collect();
        assert_eq!(files.len(), 1);

        // Verify content is valid JSON
        let content = fs::read_to_string(files[0].path()).unwrap();
        let _: serde_json::Value = serde_json::from_str(&content).unwrap();

        // Cleanup
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_recorder_per_instrument_counters() {
        let dir = std::env::temp_dir().join(format!("barter_test_snap2_{}", std::process::id()));
        let _ = fs::remove_dir_all(&dir);

        let mut recorder = OrderbookRecorder::new(2, &dir);
        let book = OrderBook::new(1, None, vec![Level::new(dec!(0.50), dec!(10))], vec![]);

        // Instrument A: 1 update
        recorder.on_orderbook_update("A", &book);
        // Instrument B: 1 update
        recorder.on_orderbook_update("B", &book);
        assert_eq!(recorder.total_written(), 0);

        // Instrument A: 2nd update → snapshot
        recorder.on_orderbook_update("A", &book);
        assert_eq!(recorder.total_written(), 1);

        // Instrument B: 2nd update → snapshot
        recorder.on_orderbook_update("B", &book);
        assert_eq!(recorder.total_written(), 2);

        let _ = fs::remove_dir_all(&dir);
    }
}
