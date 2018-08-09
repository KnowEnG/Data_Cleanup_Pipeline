import sys
import pandas
from utils.io_util import IOUtil
import utils.log_util as logger
from knpackage.toolbox import get_run_parameters, get_run_directory_and_file

checks_values = ["contains_na", "check_real_number", "check_integer",
                 "check_positive_number", "check_binary"]
checks_idx_header = ["contains_na", "contains_duplication"]


class Checker:
    def __init__(self, run_parameters):
        self.run_parameters = run_parameters
        self.dataframe = IOUtil.load_data_file_wo_empty_line(self.run_parameters['spreadsheet_name_full_path']) \
            if "spreadsheet_name_full_path" in self.run_parameters.keys() else None
        self.output_values = pandas.DataFrame(index=checks_values)
        self.output_idx_header = pandas.DataFrame(index=checks_idx_header)

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
        self.output_idx_header["index"] = Checker.check_index_header(pandas.DataFrame(self.dataframe.index.values))
        self.output_idx_header["header"] = Checker.check_index_header(pandas.DataFrame(self.dataframe.columns.values))
        self.output_values["value"] = Checker.check_values(dataframe=self.dataframe)
        IOUtil.write_to_file(self.output_values, "data_statistics_values", self.run_parameters['results_directory'],
                             "_ETL.tsv")
        IOUtil.write_to_file(self.output_idx_header, "data_statistics_index_header",
                             self.run_parameters['results_directory'], "_ETL.tsv")

    @staticmethod
    def check_index_header(dataframe):
        output = []
        # checks if dataframe contains NA value
        output.append(True if dataframe.isnull().values.any() else False)
        # checks if index/header has duplicates
        output.append(True if True in dataframe.duplicated() else False)
        series = pandas.Series(output)

        return series.values

    @staticmethod
    def check_values(dataframe):
        output = []
        # checks if dataframe contains NA value
        output.append(True if dataframe.isnull().values.any() else False)
        # checks if dataframe contains only real number
        output.append(False if False in dataframe.applymap(lambda x: isinstance(x, (int, float))).values else True)
        # checks if dataframe contains only integer number
        output.append(False if False in dataframe.applymap(lambda x: isinstance(x, (int))).values else True)
        # checks if dataframe contains only positive real number
        output.append(False if False in dataframe.applymap(
            lambda x: isinstance(x, (int, float)) and x >= 0).values else True)
        # checks if dataframe is binary
        output.append(False if set(pandas.unique(dataframe.values.ravel())) != set([0, 1]) else True)

        series = pandas.Series(output)

        return series.values


def checker():
    try:
        logger.init()
        run_directory, run_file = get_run_directory_and_file(sys.argv)
        run_parameters = get_run_parameters(run_directory, run_file)
        obj = Checker(run_parameters)
        obj.condition_check()

    except Exception as err:
        logger.logging.append("ERROR: {}".format(str(err)))
        raise RuntimeError(str(err))


if __name__ == "__main__":
    checker()
