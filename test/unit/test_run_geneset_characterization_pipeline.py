import unittest
import data_cleanup_toolbox as data_cln

class TestRun_geneset_characterization_pipeline(unittest.TestCase):
    def setUp(self):
        self.run_parameters = {
            "spreadsheet_name_full_path": "../../data/spreadsheets/TEST_1_gene_expression.tsv",
            "phenotype_full_path": "../../data/spreadsheets/TEST_1_phenotype.tsv",
            "results_directory": "./",
            "source_hint": "",
            "taxonid": '9606',
            "pipeline_type": "geneset_characterization_pipeline"
        }


    def tearDown(self):
        del self.run_parameters


    def test_run_geneset_characterization_pipeline(self):
        ret_flag, ret_msg = data_cln.run_geneset_characterization_pipeline(self.run_parameters)
        self.assertEqual(False, ret_flag)


if __name__ == '__main__':
    unittest.main()
