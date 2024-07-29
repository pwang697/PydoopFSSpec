import unittest
import fsspec
from pydoopfsspec import HadoopFileSystem

# Register the pydoop implementation with fsspec
fsspec.register_implementation("pydoop", HadoopFileSystem)
hdfs = fsspec.filesystem("pydoop", host="hdfs", port=8020, user="root")

class TestHadoopFileSystem(unittest.TestCase):

    def test_01mkdir_ls(self):
        hdfs.mkdir("/tests/fsspec/one")
        hdfs.mkdir("/tests/fsspec/two")
        result = hdfs.ls("/tests/fsspec", detail=True)
        print(result)
        self.assertGreaterEqual(len(result), 2)

    def test_02info(self):
        info = hdfs.info("/tests/fsspec")
        print(info)
        self.assertIsNotNone(info)

    def test_03exists(self):
        exists = hdfs.exists("/tests/fsspec")
        print(exists)
        self.assertTrue(exists)

    def test_04cp_file(self):
        hdfs.cp_file("/tests/file.txt", "/tests/fsspec/file.txt")
        hdfs.cp_file("/tests/file.txt", "/tests/fsspec/one/file.txt")
        result = hdfs.ls("/tests/fsspec")
        print(result)
        self.assertGreaterEqual(len(result), 2)

    def test_05mv(self):
        hdfs.mv("/tests/fsspec/file.txt", "/tests/fsspec/two/file.txt")
        result = hdfs.ls("/tests/fsspec/two")
        print(result)
        self.assertGreaterEqual(len(result), 1)

    def test_06rm_file(self):
        hdfs.cp_file("/tests/file.txt", "/tests/fsspec/file.txt")
        result = hdfs.ls("/tests/fsspec")
        print(result)
        self.assertIn("/tests/fsspec/file.txt", [f.split("hdfs://hdfs:8020")[1] for f in result])
        
        hdfs.rm_file("/tests/fsspec/file.txt")
        result = hdfs.ls("/tests/fsspec")
        print(result)
        self.assertNotIn('/tests/fsspec/file.txt', [f.split("hdfs://hdfs:8020")[1] for f in result])

    def test_07rm_rmdir(self):
        hdfs.rm("/tests/fsspec/one", recursive=True)
        result = hdfs.ls("/tests/fsspec")
        print(result)
        self.assertNotIn('/tests/fsspec/one', [f.split("hdfs://hdfs:8020")[1] for f in result])
        
        hdfs.rm("/tests/fsspec/two/file.txt")
        hdfs.rmdir("/tests/fsspec/two")
        result = hdfs.ls("/tests/fsspec")
        print(result)
        self.assertNotIn('/tests/fsspec/two', [f.split("hdfs://hdfs:8020")[1] for f in result])

    def test_08modified(self):
        info = hdfs.modified("/tests/fsspec")
        print(info)
        self.assertIsNotNone(info)

    def test_09cat_file(self):
        content = hdfs.cat_file("/tests/file.txt")
        print(content)
        self.assertIsNotNone(content)

    def test_10open(self):
        with hdfs.open("/tests/file.txt", 'rb') as f:
            first_line = f.readline()
            print("First line of the file:", first_line)
            self.assertIsNotNone(first_line)

if __name__ == "__main__":
    unittest.main()
    hdfs.rm("/tests/fsspec", recursive=True)
