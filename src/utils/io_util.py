import os
import pandas
import utils.log_util as logger
from utils.spreadsheet import SpreadSheet

class IOUtil:
    @staticmethod
    def load_data_file_wo_empty_line(file_path):
        """
        Loads data file as a DataFrame object and removes empty line by a given file path. 

        Args:
            file_path: input file, which is uploaded from frontend

        Returns:
            input_df_wo_empty_ln: user input as a DataFrame, which doesn't have empty line
        """
        input_df = IOUtil.load_data_file_default(file_path)

        if input_df is None:
            return None

        # removes rows with 'NA' values (which is a valid value in Gene name)
        input_df_wo_empty_ln = SpreadSheet.remove_empty_row(input_df)
        if input_df_wo_empty_ln is None or input_df_wo_empty_ln.empty:
            logger.logging.append(
                'ERROR: Input data {} becomes empty after removing empty row. Please provide a valid input data.'.format(
                    file_path))
            return None
        return input_df_wo_empty_ln


    @staticmethod
    def load_data_file_default(file_path):
        """
        Loads data file as a DataFrame object.
        
        Args:
            file_path: input file, which is uploaded from frontend

        Returns:
            input_df: user input as a DataFrame, which doesn't have empty line
        """
        if not file_path or not file_path.strip() or not os.path.exists(file_path):
            logger.logging.append('ERROR: Input file path is not valid: {}. Please provide a valid input path.'.format(file_path))
            return None
        try:
            # loads the header
            header_df = pandas.read_csv(file_path, sep='\t', nrows=1, header=None)
            new_header = header_df.values.tolist()[0][1:]
            # loads input data
            input_df = pandas.read_csv(file_path, sep='\t', skiprows=[0], index_col=0, header=None,
                                       error_bad_lines=False, warn_bad_lines=True)
            del input_df.index.name
            # reassigns the new_header to input_df
            input_df.columns = new_header

            if input_df.shape == (0,0):
                logger.logging.append('ERROR: Input data {} is empty. Please provide a valid input data.'.format(file_path))
                return None


            # casting index and columns to String type
            input_df.index = input_df.index.map(str)
            input_df.columns = input_df.columns.map(str)


            logger.logging.append('INFO: Successfully loaded input data: {} with {} row(s) and {} '
                           'column(s)'.format(file_path, input_df.shape[0], input_df.shape[1]))
            return input_df

        except Exception as err:
            logger.logging.append('ERROR: {}'.format(str(err)))
            return None

    @staticmethod
    def load_data_file_single_column_no_header(file_path):
        """
        Loads data file with only single column without header included as a DataFrame object. 
        
        Args:
            file_path: 

        Returns:

        """
        if not file_path or not file_path.strip() or not os.path.exists(file_path):
            logger.logging.append(
                'ERROR: Input file path is not valid: {}. Please provide a valid input path.'.format(file_path))
            return None

        # loads input data
        input_df = pandas.read_csv(file_path, sep='\t', index_col=0, header=None, error_bad_lines=False, warn_bad_lines=True)

        if input_df.shape == (0, 0):
            logger.logging.append('ERROR: Input data {} is empty. Please provide a valid input data.'.format(file_path))
            return None

        input_df.index = input_df.index.map(str)

        logger.logging.append('INFO: Successfully loaded input data: {} with {} row(s) and {} '
                              'column(s)'.format(file_path, input_df.shape[0], input_df.shape[1]))

        return input_df


    @staticmethod
    def write_to_file(target_file, target_path, result_directory, suffix, use_index=True, use_header=True, na_rep=''):
        """
        Write to a csv file.

        Args:
            target_file: the file which will be write to disk
            target_path: the location the target_file which will be written to
            result_directory: target_file directory
            suffix: output file suffix

        Returns:
            NA
        """
        output_file_basename = os.path.splitext(os.path.basename(os.path.normpath(target_path)))[0]
        target_file.to_csv(result_directory + '/' + output_file_basename + suffix,
                           sep='\t', index=use_index, header=use_header, na_rep=na_rep)





