import unittest
import os
import src.input_data_cleaning as data_cln


class TestParse_config(unittest.TestCase):
    def setUp(self):
        self.config_dir = "./config_tmp"
        self.user_spreadsheet = "user_spreadsheet.csv"
        self.full_file_path = os.path.join(self.config_dir, self.user_spreadsheet)
        self.run_parameter_template = {
            'spreadsheet_name_full_path': 'cc_cluster_nmf',
            'results_directory': self.full_file_path
        }

    def tearDown(self):
        pass

    def createFile(self, dir_name, file_name, file_content):
        os.makedirs(dir_name, mode=0o755, exist_ok=True)
        with open(os.path.join(dir_name, file_name), "w") as f:
            f.write(file_content)

        f.close()

    def test_parse_config(self):
        cur_config = data_cln.parse_config()


if __name__ == '__main__':
    unittest.main()
