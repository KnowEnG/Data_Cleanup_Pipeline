import unittest
import data_cleanup_toolbox as data_cln


class TestGet_file_basename(unittest.TestCase):
    def setUp(self):
        self.file_path = "../../../example.tsv"
        self.golden_output = "example"

    def tearDown(self):
        del self.file_path
        del self.golden_output

    def test_get_file_basename(self):
        ret = data_cln.get_file_basename(self.file_path)
        self.assertEqual(ret, self.golden_output)


if __name__ == '__main__':
    unittest.main()
