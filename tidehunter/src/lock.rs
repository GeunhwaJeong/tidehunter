use parking_lot::Mutex;
use std::collections::HashSet;
use std::fs::{File, OpenOptions};
use std::io;
use std::os::unix::io::AsRawFd;
use std::path::{Path, PathBuf};
use std::sync::OnceLock;
use std::time::Duration;

static OPEN_DBS: OnceLock<Mutex<HashSet<PathBuf>>> = OnceLock::new();

fn open_dbs() -> &'static Mutex<HashSet<PathBuf>> {
    OPEN_DBS.get_or_init(|| Mutex::new(HashSet::new()))
}

/// An exclusive lock on a database directory, held for the lifetime of the value.
///
/// Uses two layers of protection to prevent concurrent access:
///
/// 1. **In-process registry**: a global `HashSet` of canonical paths tracks all currently-open
///    databases within the process. This is necessary because POSIX `fcntl` locks are
///    per-process, not per-fd — a second `F_SETLK` call from the same process would silently
///    succeed and replace the first lock rather than blocking.
///
/// 2. **`fcntl(F_SETLK)` advisory lock** on a `LOCK` file in the database directory: catches
///    concurrent access from other processes. The OS releases this lock automatically on process
///    exit or fd close, so no cleanup is needed after a crash.
///
/// The registry check must happen before opening any fd. Opening then closing a competing fd
/// would silently release all `fcntl` locks held by this process on that inode — including the
/// one the first opener thinks it still holds (see [RocksDB issue #1780]).
///
/// [RocksDB issue #1780]: https://github.com/facebook/rocksdb/issues/1780
#[derive(Debug)]
pub struct DbLock {
    _file: File,
    canonical_path: PathBuf,
}

impl DbLock {
    /// Default timeout used when no config is available (e.g. in `Db::drop_db`).
    /// Acquires an exclusive lock on `db_path`.
    ///
    /// Retries for up to `retry_timeout` awhen the in-process registry shows the database is
    /// held — background threads may briefly extend the lifetime of a dropped `Arc<Db>`,
    /// and this gives them time to finish before the open is considered an error.
    ///
    /// Returns `ErrorKind::AlreadyExists` if the database is still open after the timeout,
    /// or if it is locked by another process.
    pub fn acquire(db_path: &Path, retry_timeout: Duration) -> io::Result<Self> {
        let canonical = db_path.canonicalize()?;

        // Insert into the in-process registry before opening any fd.
        // Must happen before open() to avoid the close()-releases-fcntl-locks hazard:
        // closing a competing fd on the same inode would silently drop the first
        // opener's fcntl lock (POSIX fcntl locks are per-process, not per-fd).
        let start = std::time::Instant::now();
        let deadline = start + retry_timeout;
        let mut last_warning = None::<std::time::Instant>;
        loop {
            {
                let mut open = open_dbs().lock();
                if !open.contains(&canonical) {
                    open.insert(canonical.clone());
                    break;
                }
            }
            let now = std::time::Instant::now();
            if now >= deadline {
                return Err(io::Error::new(
                    io::ErrorKind::AlreadyExists,
                    format!(
                        "Database at {} is already open in this process",
                        db_path.display()
                    ),
                ));
            }
            let should_warn =
                last_warning.is_none_or(|t| now.duration_since(t) >= Duration::from_secs(1));
            if should_warn {
                eprintln!(
                    "WARNING: waiting for database at {} to close (waited {:.1}s)",
                    db_path.display(),
                    now.duration_since(start).as_secs_f32()
                );
                last_warning = Some(now);
            }
            std::thread::sleep(Duration::from_millis(10));
        }

        let lock_path = canonical.join("LOCK");
        let file = match OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(false)
            .open(&lock_path)
        {
            Ok(f) => f,
            Err(e) => {
                open_dbs().lock().remove(&canonical);
                return Err(e);
            }
        };

        #[allow(clippy::unnecessary_cast)] // l_type is i16 on some platforms, i32 on others
        let mut flock = libc::flock {
            l_type: libc::F_WRLCK as i16, // Exclusive write lock
            l_whence: libc::SEEK_SET as i16,
            l_start: 0,
            l_len: 0, // Lock the entire file
            l_pid: 0,
        };
        let result = unsafe { libc::fcntl(file.as_raw_fd(), libc::F_SETLK, &mut flock) };
        if result == -1 {
            open_dbs().lock().remove(&canonical);
            let err = io::Error::last_os_error();
            if err.raw_os_error() == Some(libc::EWOULDBLOCK)
                || err.raw_os_error() == Some(libc::EAGAIN)
            {
                return Err(io::Error::new(
                    io::ErrorKind::AlreadyExists,
                    format!(
                        "Database at {} is already locked by another process",
                        db_path.display()
                    ),
                ));
            }
            return Err(err);
        }

        Ok(DbLock {
            _file: file,
            canonical_path: canonical,
        })
    }
}

impl Drop for DbLock {
    fn drop(&mut self) {
        open_dbs().lock().remove(&self.canonical_path);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Read;
    use std::process::{Command, Stdio};
    use tempdir::TempDir;

    #[test]
    fn test_lock_same_process() {
        let dir = TempDir::new("test-lock").unwrap();
        let canonical = dir.path().canonicalize().unwrap();
        let lock = DbLock::acquire(dir.path(), Duration::ZERO).unwrap();

        // Verify the registry shows the DB as held without triggering the retry timeout.
        assert!(open_dbs().lock().contains(&canonical));

        drop(lock);
        // Lock released — should succeed now
        let _lock2 = DbLock::acquire(dir.path(), Duration::ZERO).unwrap();
    }

    #[test]
    fn test_lock_registry_cleaned_up_on_drop() {
        let dir = TempDir::new("test-lock-cleanup").unwrap();
        let canonical = dir.path().canonicalize().unwrap();

        assert!(!open_dbs().lock().contains(&canonical));
        let lock = DbLock::acquire(dir.path(), Duration::ZERO).unwrap();
        assert!(open_dbs().lock().contains(&canonical));
        drop(lock);
        assert!(!open_dbs().lock().contains(&canonical));
    }

    // This "test" is actually a subprocess entrypoint used by test_lock_cross_process.
    // When TIDEHUNTER_TEST_HOLD_LOCK is set, it acquires the lock at that path, creates
    // a "{path}/.ready" file to signal the parent, then waits for stdin to close.
    #[test]
    fn _subprocess_lock_holder() {
        let Ok(path) = std::env::var("TIDEHUNTER_TEST_HOLD_LOCK") else {
            return;
        };
        let path = PathBuf::from(path);
        let _lock =
            DbLock::acquire(&path, Duration::ZERO).expect("subprocess failed to acquire lock");
        // Signal parent via file — avoids libtest stdout capture interference
        std::fs::write(path.join(".ready"), b"").unwrap();
        // Block until parent closes stdin
        let mut buf = [0u8; 1];
        let _ = std::io::stdin().read(&mut buf);
    }

    #[test]
    fn test_lock_cross_process() {
        let dir = TempDir::new("test-lock-cross-process").unwrap();
        let exe = std::env::current_exe().unwrap();
        let ready = dir.path().join(".ready");

        let mut child = Command::new(&exe)
            .arg("_subprocess_lock_holder")
            .env("TIDEHUNTER_TEST_HOLD_LOCK", dir.path())
            .stdin(Stdio::piped())
            .spawn()
            .unwrap();

        // Wait for subprocess to signal it holds the lock (up to 5s)
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
        while !ready.exists() {
            assert!(std::time::Instant::now() < deadline, "subprocess timed out");
            std::thread::sleep(std::time::Duration::from_millis(10));
        }

        // Another process holds the lock — must fail
        let err = DbLock::acquire(dir.path(), Duration::ZERO).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::AlreadyExists);

        // Signal subprocess to exit by closing its stdin
        drop(child.stdin.take());
        child.wait().unwrap();

        // Lock released — must succeed now
        let _lock = DbLock::acquire(dir.path(), Duration::ZERO).unwrap();
    }
}
