import unittest
import os
import yaml
import shutil
import data_cleanup_toolbox as data_cln


class TestParse_config(unittest.TestCase):
    def setUp(self):
        self.run_dir = "./run_dir"
        self.user_spreadsheet = "user_spreadsheet.csv"
        self.full_file_path = os.path.join(self.run_dir, self.user_spreadsheet)

        self.f_context = yaml.dump(
            dict(spreadsheet_name_full_path=self.full_file_path, results_directory=self.run_dir + "/" + "results"),
            default_flow_style=True)
        self.golden_output = {
            "spreadsheet_name_full_path": self.full_file_path,
            "results_directory": self.run_dir + "/" + "results"
        }

    def tearDown(self):
        shutil.rmtree(self.run_dir)
        del self.user_spreadsheet
        del self.full_file_path
        del self.f_context
        del self.golden_output

    def createFile(self, dir_name, file_name, file_content):
        os.makedirs(dir_name, mode=0o755, exist_ok=True)
        with open(os.path.join(dir_name, file_name), "w") as f:
            f.write(file_content)

        f.close()

    def test_parse_config(self):
        self.createFile(self.run_dir, "data_cleanup.yml", self.f_context)
        cur_config = data_cln.parse_config(self.run_dir + "/" + "data_cleanup.yml")
        self.assertDictEqual(cur_config, self.golden_output)


if __name__ == '__main__':
    unittest.main()
