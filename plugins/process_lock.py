"""Secure cross-platform single-process lock stored in private runtime state."""

import os
import stat
from pathlib import Path


class ProcessLockError(RuntimeError):
    pass


class AlreadyRunningError(ProcessLockError):
    pass


def _validate_owned_regular_file(path: Path, file_stat=None):
    path_stat = path.lstat()
    if stat.S_ISLNK(path_stat.st_mode) or not stat.S_ISREG(path_stat.st_mode):
        raise ProcessLockError("process lock path is not a regular file")
    if os.name != "nt" and path_stat.st_uid != os.geteuid():
        raise ProcessLockError("process lock is not owned by the bot user")
    if file_stat is not None and (
        path_stat.st_dev != file_stat.st_dev or path_stat.st_ino != file_stat.st_ino
    ):
        raise ProcessLockError("process lock changed while it was being opened")


def prepare_private_runtime_dir(runtime_dir) -> Path:
    path = Path(runtime_dir).expanduser().absolute()
    if path.exists() and path.is_symlink():
        raise ProcessLockError("runtime directory must not be a symbolic link")
    path.mkdir(mode=0o700, parents=True, exist_ok=True)
    directory_stat = path.stat()
    if not stat.S_ISDIR(directory_stat.st_mode):
        raise ProcessLockError("runtime path is not a directory")
    if os.name != "nt":
        if directory_stat.st_uid != os.geteuid():
            raise ProcessLockError("runtime directory is not owned by the bot user")
        path.chmod(0o700)
        if stat.S_IMODE(path.stat().st_mode) != 0o700:
            raise ProcessLockError("runtime directory permissions are not 0700")
    return path.resolve()


def acquire_process_lock(runtime_dir, name="mccxbot.lock"):
    directory = prepare_private_runtime_dir(runtime_dir)
    lock_path = directory / name
    flags = os.O_RDWR | os.O_CREAT
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW

    if not lock_path.exists():
        try:
            descriptor = os.open(lock_path, flags | os.O_EXCL, 0o600)
        except FileExistsError:
            _validate_owned_regular_file(lock_path)
            descriptor = os.open(lock_path, flags, 0o600)
    else:
        _validate_owned_regular_file(lock_path)
        descriptor = os.open(lock_path, flags, 0o600)

    lock_file = None
    try:
        file_stat = os.fstat(descriptor)
        if not stat.S_ISREG(file_stat.st_mode):
            raise ProcessLockError("opened process lock is not a regular file")
        _validate_owned_regular_file(lock_path, file_stat)
        if os.name != "nt":
            os.fchmod(descriptor, 0o600)
        lock_file = os.fdopen(descriptor, "r+", encoding="ascii", closefd=True)
        descriptor = None
        if file_stat.st_size == 0:
            try:
                lock_file.write("0")
                lock_file.flush()
            except PermissionError as exc:
                raise AlreadyRunningError(
                    "Another MCCxBot instance is already running"
                ) from exc
        lock_file.seek(0)
        try:
            if os.name == "nt":
                import msvcrt
                msvcrt.locking(lock_file.fileno(), msvcrt.LK_NBLCK, 1)
            else:
                import fcntl
                fcntl.flock(lock_file.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError as exc:
            lock_file.close()
            lock_file = None
            raise AlreadyRunningError(
                "Another MCCxBot instance is already running"
            ) from exc
        return lock_file
    except Exception:
        if lock_file is not None:
            lock_file.close()
        if descriptor is not None:
            os.close(descriptor)
        raise
