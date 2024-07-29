import unittest
import fsspec
import pyarrow.fs
from pydoopfsspec import HadoopFileSystem

# Register the pydoop implementation with pyarrow
fsspec.register_implementation("pydoop", HadoopFileSystem)
hdfs = fsspec.filesystem("pydoop", host="hdfs", port=8020, user="root")
fs = pyarrow.fs.PyFileSystem(pyarrow.fs.FSSpecHandler(hdfs))

class TestHadoopFileSystem(unittest.TestCase):

    def test_01create_dir(self):
        fs.create_dir("/tests/pyarrow/one")
        fs.create_dir("/tests/pyarrow/two")
        result = hdfs.ls("/tests/pyarrow")
        print(result)
        self.assertGreaterEqual(len(result), 2)

    def test_02get_file_info(self):
        info = fs.get_file_info("/tests/pyarrow")
        print(info)
        self.assertIsNotNone(info)

    def test_03copy_file(self):
        fs.copy_file("/tests/file.txt", "/tests/pyarrow/file.txt")
        fs.copy_file("/tests/file.txt", "/tests/pyarrow/one/file.txt")
        result = hdfs.ls("/tests/pyarrow")
        print(result)
        self.assertGreaterEqual(len(result), 2)

    def test_04move(self):
        fs.move("/tests/pyarrow/file.txt", "/tests/pyarrow/two/file.txt")
        result = hdfs.ls("/tests/pyarrow/two")
        print(result)
        self.assertGreaterEqual(len(result), 1)

    def test_05delete_file(self):
        fs.copy_file("/tests/file.txt", "/tests/pyarrow/file.txt")
        result = hdfs.ls("/tests/pyarrow")
        print(result)
        self.assertIn("/tests/pyarrow/file.txt", [f.split("hdfs://hdfs:8020")[1] for f in result])
        
        fs.delete_file("/tests/pyarrow/file.txt")
        result = hdfs.ls("/tests/pyarrow")
        print(result)
        self.assertNotIn('/tests/pyarrow/file.txt', [f.split("hdfs://hdfs:8020")[1] for f in result])

    def test_06delete_dir_contents(self):
        result = hdfs.ls("/tests/pyarrow/one")
        print(result)
        self.assertGreaterEqual(len(result), 1)

        fs.delete_dir_contents("/tests/pyarrow/one")
        result = hdfs.ls("/tests/pyarrow/one")
        print(result)
        self.assertEqual(len(result), 0)

    def test_07delete_dir(self):
        result = hdfs.ls("/tests/pyarrow/two")
        print(result)
        self.assertGreaterEqual(len(result), 1)

        fs.delete_dir("/tests/pyarrow/two")
        result = hdfs.ls("/tests/pyarrow")
        print(result)
        self.assertNotIn("/tests/pyarrow/two", [f.split("hdfs://hdfs:8020")[1] for f in result])

if __name__ == "__main__":
    unittest.main()
    fs.delete_dir("/tests/pyarrow")