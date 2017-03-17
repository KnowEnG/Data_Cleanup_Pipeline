import unittest
import os
import pandas as pd
import numpy.testing as npytest
import shutil
import data_cleanup_toolbox as data_cln


class TestLoad_data_file(unittest.TestCase):
    def setUp(self):
        self.run_dir = "./run_file"
        self.user_spreadsheet = "user_spreadsheet.tsv"
        self.spreadsheet_path = self.run_dir + "/" + self.user_spreadsheet
        self.f_context = "\ta\tb\tc\n" + \
                         "ENSG00000000003\t1\t0\t1\n" + \
                         "ENSG00001000205\t0\t0\t1\n" + \
                         "ENSG00000700034\t1\t1\t1\n"
        self.golden_output = pd.DataFrame([
            [1, 0, 1],
            [0, 0, 1],
            [1, 1, 1]],
            index=['ENSG00000000003', "ENSG00001000205", 'ENSG00000700034'],
            columns=['a', 'b', 'c'])

    def tearDown(self):
        del self.user_spreadsheet
        del self.spreadsheet_path
        del self.f_context
        del self.golden_output

    def createFile(self, dir_name, file_name, file_content):
        os.makedirs(dir_name, mode=0o755, exist_ok=True)
        with open(os.path.join(dir_name, file_name), "w") as f:
            f.write(file_content)
        f.close()

    def test_load_data_file(self):
        self.createFile(self.run_dir, self.user_spreadsheet, self.f_context)
        ret_df = data_cln.load_data_file(self.spreadsheet_path)
        npytest.assert_array_equal(self.golden_output, ret_df)
        shutil.rmtree(self.run_dir)

    def test_load_data_file_with_execption(self):
        ret_df = data_cln.load_data_file("./file_not_exist")
        self.assertEqual(None, ret_df)


if __name__ == '__main__':
    unittest.main()
