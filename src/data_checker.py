import pandas
from utils.io_util import IOUtil
import sys
import utils.log_util as logger
from knpackage.toolbox import get_run_parameters, get_run_directory_and_file

pipelines = [
    "ret"
]

checks = ["check_na", "check_real_number", "check_integer", "check_positive_number", "check_binary"]

class Checker:
    def __init__(self, run_parameters):
        self.run_parameters = run_parameters
        self.dataframe = IOUtil.load_data_file(self.run_parameters['spreadsheet_name_full_path']) \
            if "spreadsheet_name_full_path" in self.run_parameters.keys() else None
        self.out_df = pandas.DataFrame(index=pipelines, columns=checks)


    def condition_check(self):
        """
        Customized checks for input data (contains NA value, contains all real number, contains all positive number)
        Args:
            dataframe: input DataFrame to be checked
            check_na: check NA in DataFrame
            dropna_colwise: drop column which contains NA
            check_real_number: check only real number exists in DataFrame
            check_positive_number: check only positive number exists in DataFrame

        Returns:
            dataframe: cleaned DataFrame
        """
        output = []

        # checks if dataframe contains NA value
        output.append(False if self.dataframe.isnull().values.any() else True)
        # checks if dataframe contains only real number
        output.append(False if False in self.dataframe.applymap(lambda x: isinstance(x, (int, float))).values else True)
        output.append(False if False in self.dataframe.applymap(lambda x: isinstance(x, (int))).values else True)
        output.append(False if False in self.dataframe.applymap(lambda x: isinstance(x, (int, float)) and x >= 0).values else True)
        output.append(False if set(pandas.unique(self.dataframe.values.ravel())) != set([0,1]) else True)

        out_df = pandas.DataFrame([output], index=['data_statics'], columns=checks)
        IOUtil.write_to_file(out_df,
            "data_statics",
        self.run_parameters['results_directory'], "_ETL.tsv")


def checker():
    try:
        logger.init()
        run_directory, run_file = get_run_directory_and_file(sys.argv)
        run_parameters = get_run_parameters(run_directory, run_file)
        obj = Checker(run_parameters)
        ret = obj.condition_check()

    except Exception as err:
        logger.logging.append("ERROR: {}".format(str(err)))
        raise RuntimeError(str(err))


if __name__ == "__main__":
    checker()
