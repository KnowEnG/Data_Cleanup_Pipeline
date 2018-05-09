import unittest
import pandas as pd
from utils.common_util import CommonUtil
import utils.log_util as logger


class TestRemove_dataframe_indexer_duplication(unittest.TestCase):
    def setUp(self):
        logger.init()
        self.input_df_good = pd.DataFrame(
            [[1, 0],
             [0, 0],
             [1, 1]],
            index=['ENSG00000000003', "ENSG00000000457", 'ENSG00000000005'],
            columns=['a', 'b']
        )
        self.input_phenotype = pd.DataFrame(
            [[1.1, 2.2, 3.3]],
            index=['drug1'],
            columns=['a', 'b', 'c']
        )

        self.run_parameters = {
            "spreadsheet_name_full_path": "../data/spreadsheets/example.tsv",
            "phenotype_name_full_path": ".. /data/spreadsheets/phenotype.tsv",
            "results_directory": "./",
            "redis_credential": {
                "host": "knowredis.knoweng.org",
                "port": 6379,
                "password": "KnowEnG"
            },
            "source_hint": "",
            "taxonid": '9606',
            "pipeline_type": "gene_priorization_pipeline",
            "input_data_type": ""
        }

    def tearDown(self):
        del self.input_df_good
        del self.run_parameters

    def test_Remove_dataframe_indexer_duplication(self):
        ret_val = CommonUtil.remove_dataframe_indexer_duplication(self.input_df_good)
        ret_val_boolean = True if ret_val is not None else False
        self.assertEqual(True, ret_val_boolean)


if __name__ == '__main__':
    unittest.main()
