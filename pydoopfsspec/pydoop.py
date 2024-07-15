import errno
import io
import secrets
import shutil
from contextlib import suppress
from functools import cached_property, wraps
from urllib.parse import parse_qs

from fsspec.spec import AbstractFileSystem
from fsspec.utils import (
    infer_storage_options,
    mirror_from,
    tokenize,
)


def wrap_exceptions(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except OSError as exception:
            if not exception.args:
                raise

            message, *args = exception.args
            if isinstance(message, str) and "does not exist" in message:
                raise FileNotFoundError(errno.ENOENT, message) from exception
            else:
                raise

    return wrapper


class PydoopHDFSWrapper(AbstractFileSystem):
    """FSSpec-compatible wrapper of pydoop.hdfs.hdfs.
    
    Parameters
    ----------
    fs : pydoop.hdfs.hdfs
    
    """

    root_marker = "/"

    def __init__(self, fs, **kwargs):
        self.fs = fs
        super().__init__(**kwargs)

    @property
    def protocol(self):
        return 'hdfs'

    @cached_property
    def fsid(self):
        return "hdfs_" + tokenize(self.fs.host, self.fs.port)

    @classmethod
    def _strip_protocol(cls, path):
        ops = infer_storage_options(path)
        path = ops["path"]
        if path.startswith("//"):
            # special case for "hdfs://path" (without the triple slash)
            path = path[1:]
        return path

    def ls(self, path, detail=False, **kwargs):
        path = self._strip_protocol(path)
        entries = [
            self._make_entry(entry)
            for entry in self.fs.list_directory(path)
        ]
        if detail:
            return entries
        else:
            return [entry["name"] for entry in entries]

    def info(self, path, **kwargs):
        path = self._strip_protocol(path)
        info = self.fs.get_path_info(path)
        return self._make_entry(info)

    def exists(self, path):
        path = self._strip_protocol(path)
        return self.fs.exists(path)

    def _make_entry(self, info):
        return {
            "name": info["name"],
            "size": info["size"],
            "type": info["kind"],
            "mtime": info["last_mod"],
        }

    @wrap_exceptions
    def cp_file(self, path1, path2, **kwargs):
        path1 = self._strip_protocol(path1).rstrip("/")
        path2 = self._strip_protocol(path2).rstrip("/")

        with self._open(path1, "rb") as lstream:
            tmp_fname = f"{path2}.tmp.{secrets.token_hex(6)}"
            try:
                with self.open(tmp_fname, "wb") as rstream:
                    shutil.copyfileobj(lstream, rstream)
                self.fs.rename(tmp_fname, path2)
            except BaseException:  # noqa
                with suppress(FileNotFoundError):
                    self.fs.delete(tmp_fname)
                raise

    @wrap_exceptions
    def mv(self, path1, path2, **kwargs):
        path1 = self._strip_protocol(path1).rstrip("/")
        path2 = self._strip_protocol(path2).rstrip("/")
        self.fs.rename(path1, path2) # hdfs.move and hdfs.rename are different method in pydoop 2.0.0

    @wrap_exceptions
    def rm_file(self, path):
        path = self._strip_protocol(path)
        self.fs.delete(path)

    @wrap_exceptions
    def rm(self, path, recursive=False, maxdepth=None): # TODO: maxdepth
        path = self._strip_protocol(path).rstrip("/")
        if self.isdir(path):
            if recursive:
                self.fs.delete(path, recursive=True)
            else:
                raise ValueError("Can't delete directories without recursive=True")
        else:
            self.fs.delete(path)

    @wrap_exceptions
    def _open(self, path, mode="rb", block_size=None, seekable=True, **kwargs): # TODO: seekable: Open an input stream for sequential reading.
        blocksize = 0 if block_size==None else block_size
        if mode == "rb":
            if seekable:
                return self.fs.open_file(path=path, mode="r", blocksize=blocksize)
            else:
                return self.fs.open_file(path=path, mode="r", blocksize=blocksize)
        elif mode == "wb":
            return self.fs.open_file(path=path, mode="w", blocksize=blocksize)
        elif mode == "ab":
            return self.fs.open_file(path=path, mode="a", blocksize=blocksize)
        elif len(mode)==2 and mode[1] == "t":
            return self.fs.open_file(path=path, mode=mode, blocksize=blocksize)
        else:
            raise ValueError(f"unsupported mode for Pydoop hdfs: {mode!r}")

    @wrap_exceptions
    def mkdir(self, path, create_parents=True, **kwargs):
        path = self._strip_protocol(path)
        if create_parents:
            self.makedirs(path, exist_ok=True)
        else:
            self.fs.create_directory(path)

    @wrap_exceptions
    def makedirs(self, path, exist_ok=False):
        path = self._strip_protocol(path)
        self.fs.create_directory(path)

    @wrap_exceptions
    def rmdir(self, path):
        path = self._strip_protocol(path)
        self.fs.delete(path, recursive=True)

    @wrap_exceptions
    def modified(self, path):
        path = self._strip_protocol(path)
        return self.fs.get_path_info(path)["last_mod"]

    def cat_file(self, path, start=None, end=None, **kwargs):
        kwargs["seekable"] = start not in [None, 0]
        return super().cat_file(path, start=start, end=end, **kwargs)

    def get_file(self, rpath, lpath, **kwargs):
        kwargs["seekable"] = False
        super().get_file(rpath, lpath, **kwargs)


@mirror_from(
    "stream",
    [
        "read",
        "seek",
        "tell",
        "write",
        "readable",
        "writable",
        "close",
        "size",
        "seekable",
    ],
)
class HDFSFile(io.IOBase):
    def __init__(self, fs, stream, path, mode, block_size=None, **kwargs):
        self.path = path
        self.mode = mode

        self.fs = fs
        self.stream = stream

        self.blocksize = self.block_size = block_size
        self.kwargs = kwargs

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return self.close()


class HadoopFileSystem(PydoopHDFSWrapper):
    """A wrapper on top of the pydoop.hdfs.hdfs to connect its interface with fsspec"""

    protocol = "pydoop"

    def __init__(
        self,
        host="default",
        port=0,
        user=None,
        **kwargs,
    ):
        """
        Parameters
        ----------
        host: str
            hostname or IP address of the HDFS NameNode. Set to 
            an empty string (and port to 0) to connect to the 
            local file system; set to 'default' (and port to 0) 
            to connect to the default (i.e., the one defined in 
            the Hadoop configuration files) file system.
        port: int
            the port on which the NameNode is listening
        user: str or None
            the Hadoop domain user name. Defaults to the current 
            UNIX user. Note that, in MapReduce applications, since 
            tasks are spawned by the JobTracker, the default user 
            will be the one that started the JobTracker itself.
        """
        from pydoop.hdfs import hdfs

        fs = hdfs(
            host=host,
            port=port,
            user=user,
        )

        super().__init__(fs=fs, **kwargs)

    @staticmethod
    def _get_kwargs_from_urls(path):
        ops = infer_storage_options(path)
        out = {}
        if ops.get("host", None):
            out["host"] = ops["host"]
        if ops.get("username", None):
            out["user"] = ops["username"]
        if ops.get("port", None):
            out["port"] = ops["port"]
        if ops.get("url_query", None):
            queries = parse_qs(ops["url_query"])
            if queries.get("replication", None):
                out["replication"] = int(queries["replication"][0])
        return out
