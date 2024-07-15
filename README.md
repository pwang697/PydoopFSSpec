# Pydoop FSSpec Interface

This is a Pydoop implementation for interacting with the Hadoop File System (HDFS) using a FSSpec Pythonic interface.

## Features

- Compatible with FSSpec and PyArrow
- Provides seamless integration with Pydoop
- Supports common file system operations such as listing directories, reading and writing files, and more

## Installation

1. Build the wheel
```
python3 setup.py sdist bdist_wheel
```

2. Install the package using pip:
```
pip install dist/pydoopfsspec-0.1.0-py3-none-any.whl 
```

## Usage

### FSSpec instance

```
import fsspec
from pydoopfsspec import HadoopFileSystem

fsspec.register_implementation("pydoop", HadoopFileSystem)
hdfs = fsspec.filesystem("pydoop", host="hdfs", port=8020, user="root")
```

### Pyarrow instance
```
import fsspec
import pyarrow.fs
from pydoopfsspec import HadoopFileSystem

fsspec.register_implementation("pydoop", HadoopFileSystem)
hdfs = fsspec.filesystem("pydoop", host="hdfs", port=8020, user="root")
fs = pyarrow.fs.PyFileSystem(pyarrow.fs.FSSpecHandler(hdfs))
```