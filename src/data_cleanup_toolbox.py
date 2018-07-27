"""
    This module serves as a connecting function between front end and back end.
    It validates/cleans the user spreadsheet data and returns a boolean value to
    indicate if the user spreadsheet is valid or not. 
"""
import pandas
import utils.log_util as logger
from utils.io_util import IOUtil
from utils.check_util import CheckUtil
from utils.transformation_util import TransformationUtil
from utils.common_util import CommonUtil
from utils.spreadsheet import SpreadSheet


class Pipelines:
    def __init__(self, run_parameters):
        self.run_parameters = run_parameters
        self.user_spreadsheet_df = IOUtil.load_data_file(self.run_parameters['spreadsheet_name_full_path']) \
            if "spreadsheet_name_full_path" in self.run_parameters.keys() else None
        self.phenotype_df = IOUtil.load_data_file(self.run_parameters['phenotype_name_full_path']) \
            if 'phenotype_name_full_path' in self.run_parameters.keys() else None
        self.pasted_gene_df = IOUtil.load_data_file_wo_remove_empty_line(self.run_parameters['pasted_gene_list_full_path']) \
            if "pasted_gene_list_full_path" in self.run_parameters.keys() else None
        self.signature_df = IOUtil.load_data_file(self.run_parameters['signature_name_full_path']) \
            if "signature_name_full_path" in self.run_parameters.keys() else None
        self.Pvalue_gene_phenotype = IOUtil.load_data_file(self.run_parameters['Pvalue_gene_phenotype_full_path']) \
            if "Pvalue_gene_phenotype_full_path" in self.run_parameters.keys() else None
        self.expression_sample = IOUtil.load_data_file(self.run_parameters['expression_sample_full_path']) \
            if "expression_sample_full_path" in self.run_parameters.keys() else None
        self.TFexpression = IOUtil.load_data_file_wo_remove_empty_line(self.run_parameters['TFexpression_full_path']) \
            if "TFexpression_full_path" in self.run_parameters.keys() else None

    def run_geneset_characterization_pipeline(self):
        """
        Runs data cleaning for geneset_characterization_pipeline.

        Args:
            NA

        Returns:
            validation_flag: Boolean type value indicating if input data is valid or not.
            message: A message indicates the status of current check.
        """
        if self.user_spreadsheet_df is None:
            return False, logger.logging

        # Checks only non-negative real number appears in user spreadsheet, drop na column wise
        user_spreadsheet_val_chked = SpreadSheet.check_user_spreadsheet_data(self.user_spreadsheet_df, check_na=True,
                                                                             check_real_number=True,
                                                                             check_positive_number=True)
        if user_spreadsheet_val_chked is None:
            return False, logger.logging

        # Removes NA value and duplication on column and row name
        user_spreadsheet_df_checked = SpreadSheet.remove_dataframe_indexer_duplication(user_spreadsheet_val_chked)

        # Checks the validity of gene name to see if it can be ensemble or not
        user_spreadsheet_df_cleaned, map_filtered_dedup, mapping = SpreadSheet.map_ensemble_gene_name(
            user_spreadsheet_df_checked,
            self.run_parameters)
        if user_spreadsheet_df_cleaned is None:
            return False, logger.logging

        IOUtil.write_to_file(user_spreadsheet_df_cleaned, self.run_parameters['spreadsheet_name_full_path'],
                             self.run_parameters['results_directory'], "_ETL.tsv")

        # writes dedupped mapping between user_supplied_gene_name and ensemble name to a file
        IOUtil.write_to_file(map_filtered_dedup, self.run_parameters['spreadsheet_name_full_path'],
                             self.run_parameters['results_directory'], "_MAP.tsv", use_index=True, use_header=False)

        # writes user supplied gene name along with its mapping status to a file
        IOUtil.write_to_file(mapping, self.run_parameters['spreadsheet_name_full_path'],
                             self.run_parameters['results_directory'],
                             "_User_To_Ensembl.tsv", use_index=False, use_header=True)

        logger.logging.append(
            "INFO: Cleaned user spreadsheet has {} row(s), {} column(s).".format(
                user_spreadsheet_df_cleaned.shape[0],
                user_spreadsheet_df_cleaned.shape[1]))
        return True, logger.logging

    def run_samples_clustering_pipeline(self):
        """
        Runs data cleaning for samples_clustering_pipeline.

        Args:
            NA

        Returns:
            validation_flag: Boolean type value indicating if input data is valid or not.
            message: A message indicates the status of current check.
        """
        if self.user_spreadsheet_df is None:
            return False, logger.logging

        logger.logging.append("INFO: Start to process user spreadsheet data.")
        # Checks if only non-negative real number appears in user spreadsheet and drop na column wise
        user_spreadsheet_val_chked = SpreadSheet.check_user_spreadsheet_data(self.user_spreadsheet_df,
                                                                             dropna_colwise=True,
                                                                             check_real_number=True,
                                                                             check_positive_number=True)
        if user_spreadsheet_val_chked is None:
            return False, logger.logging

        # Removes NA value and duplication on column and row name
        user_spreadsheet_df_checked = SpreadSheet.remove_dataframe_indexer_duplication(user_spreadsheet_val_chked)

        # Checks the validity of gene name to see if it can be ensemble or not
        user_spreadsheet_df_cleaned, map_filtered_dedup, mapping = SpreadSheet.map_ensemble_gene_name(user_spreadsheet_df_checked,
                                                                         self.run_parameters)

        if 'gg_network_name_full_path' in self.run_parameters.keys() and \
                not CommonUtil.check_network_data_intersection(user_spreadsheet_df_cleaned.index,
                                                               self.run_parameters):
            return False, logger.logging

        # The logic here ensures that even if phenotype data doesn't fits requirement, the rest pipelines can still run.
        if user_spreadsheet_df_cleaned is None:
            return False, logger.logging
        else:
            IOUtil.write_to_file(user_spreadsheet_df_cleaned, self.run_parameters['spreadsheet_name_full_path'],
                                 self.run_parameters['results_directory'], "_ETL.tsv")

            # writes dedupped mapping between user_supplied_gene_name and ensemble name to a file
            IOUtil.write_to_file(map_filtered_dedup, self.run_parameters['spreadsheet_name_full_path'],
                                 self.run_parameters['results_directory'], "_MAP.tsv", use_index=True, use_header=False)

            # writes user supplied gene name along with its mapping status to a file
            IOUtil.write_to_file(mapping, self.run_parameters['spreadsheet_name_full_path'],
                                 self.run_parameters['results_directory'],
                                 "_User_To_Ensembl.tsv", use_index=False, use_header=True)
            logger.logging.append(
                "INFO: Cleaned user spreadsheet has {} row(s), {} column(s).".format(
                    user_spreadsheet_df_cleaned.shape[0],
                    user_spreadsheet_df_cleaned.shape[1]))

        if self.phenotype_df is not None:
            logger.logging.append("INFO: Start to process phenotype data.")
            phenotype_df_cleaned = CommonUtil.check_phenotype_intersection(self.phenotype_df,
                                                                           self.user_spreadsheet_df.columns.values)
            if phenotype_df_cleaned is None:
                logger.logging.append("ERROR: Phenotype is emtpy. Please provide a valid phenotype data.")
                return False, logger.logging
            else:
                IOUtil.write_to_file(phenotype_df_cleaned, self.run_parameters['phenotype_name_full_path'],
                                     self.run_parameters['results_directory'], "_ETL.tsv")
                logger.logging.append("INFO: Cleaned phenotype data has {} row(s), {} "
                                      "column(s).".format(phenotype_df_cleaned.shape[0], phenotype_df_cleaned.shape[1]))
        return True, logger.logging

    def run_gene_prioritization_pipeline(self):
        """
        Runs data cleaning for gene_prioritization_pipeline.

        Args:
            NA.

        Returns:
            validation_flag: Boolean type value indicating if input data is valid or not.
            message: A message indicates the status of current check.
        """
        # Checks user spreadsheet data and phenotype data
        if self.user_spreadsheet_df is None or self.phenotype_df is None:
            return False, logger.logging

        # Imputes na value on user spreadsheet data
        user_spreadsheet_df_imputed = SpreadSheet.impute_na(self.user_spreadsheet_df,
                                                            option=self.run_parameters['impute'])
        if user_spreadsheet_df_imputed is None:
            return False, logger.logging

        # Checks if value of inputs satisfy certain criteria: see details in function validate_inputs_for_gp_fp
        user_spreadsheet_val_chked, phenotype_val_checked = CommonUtil.validate_inputs_for_gp_fp(
            user_spreadsheet_df_imputed, self.phenotype_df, self.run_parameters["correlation_measure"])
        if user_spreadsheet_val_chked is None or phenotype_val_checked is None:
            return False, logger.logging
        # Removes NA value and duplication on column and row name
        user_spreadsheet_df_checked = SpreadSheet.remove_dataframe_indexer_duplication(user_spreadsheet_val_chked)
        # Checks the validity of gene name to see if it can be ensemble or not
        user_spreadsheet_df_cleaned,map_filtered_dedup,mapping = SpreadSheet.map_ensemble_gene_name(user_spreadsheet_df_checked,
                                                                         self.run_parameters)
        if user_spreadsheet_df_cleaned is None or phenotype_val_checked is None:
            return False, logger.logging
        # Stores cleaned phenotype data (transposed) to a file, dimension: phenotype x sample
        IOUtil.write_to_file(phenotype_val_checked, self.run_parameters['phenotype_name_full_path'],
                             self.run_parameters['results_directory'], "_ETL.tsv")
        IOUtil.write_to_file(user_spreadsheet_df_cleaned, self.run_parameters['spreadsheet_name_full_path'],
                             self.run_parameters['results_directory'], "_ETL.tsv")
        # writes dedupped mapping between user_supplied_gene_name and ensemble name to a file
        IOUtil.write_to_file(map_filtered_dedup, self.run_parameters['spreadsheet_name_full_path'],
                             self.run_parameters['results_directory'], "_MAP.tsv", use_index=True, use_header=False)

        # writes user supplied gene name along with its mapping status to a file
        IOUtil.write_to_file(mapping, self.run_parameters['spreadsheet_name_full_path'],
                             self.run_parameters['results_directory'],
                             "_User_To_Ensembl.tsv", use_index=False, use_header=True)
        logger.logging.append(
            "INFO: Cleaned user spreadsheet has {} row(s), {} column(s).".format(
                user_spreadsheet_df_cleaned.shape[0],
                user_spreadsheet_df_cleaned.shape[1]))
        logger.logging.append(
            "INFO: Cleaned phenotype data has {} row(s), {} column(s).".format(phenotype_val_checked.shape[0],
                                                                               phenotype_val_checked.shape[1]))
        return True, logger.logging

    def run_phenotype_prediction_pipeline(self):
        """
        Runs data cleaning for phenotype_prediction_pipeline.

        Args:
            NA.
        Returns:
            validation_flag: Boolean type value indicating if input data is valid or not.
            message: A message indicates the status of current check.
        """
        # spreadsheet dimension: sample x phenotype, phenotype dimension : sample x phenotype
        if self.user_spreadsheet_df is None or self.phenotype_df is None:
            return False, logger.logging

        # Checks if user spreadsheet contains only real number and drop na column wise
        user_spreadsheet_dropna = SpreadSheet.check_user_spreadsheet_data(self.user_spreadsheet_df,
                                                                          dropna_colwise=True,
                                                                          check_real_number=True)

        if user_spreadsheet_dropna is None or user_spreadsheet_dropna.empty:
            logger.logging.append("ERROR: After drop NA, user spreadsheet data becomes empty.")
            return None, None

        # Checks for valid intersection between phenotype data and user spreadsheet data
        dataframe_header = list(user_spreadsheet_dropna.columns.values)
        phenotype_df_pxs_trimmed = CheckUtil.check_intersection_for_phenotype_and_user_spreadsheet(dataframe_header,
                                                                                                   self.phenotype_df)

        # Removes NA value and duplication on column and row name
        user_spreadsheet_df_cleaned = SpreadSheet.remove_dataframe_indexer_duplication(user_spreadsheet_dropna)
        if user_spreadsheet_df_cleaned is None or phenotype_df_pxs_trimmed is None:
            return False, logger.logging

        # Stores cleaned phenotype data (transposed) to a file, dimension: phenotype x sample
        IOUtil.write_to_file(phenotype_df_pxs_trimmed, self.run_parameters['phenotype_name_full_path'],
                             self.run_parameters['results_directory'], "_ETL.tsv")
        IOUtil.write_to_file(user_spreadsheet_df_cleaned, self.run_parameters['spreadsheet_name_full_path'],
                             self.run_parameters['results_directory'], "_ETL.tsv")

        logger.logging.append(
            "INFO: Cleaned user spreadsheet has {} row(s), {} column(s).".format(
                user_spreadsheet_df_cleaned.shape[0],
                user_spreadsheet_df_cleaned.shape[1]))
        logger.logging.append(
            "INFO: Cleaned phenotype data has {} row(s), {} column(s).".format(phenotype_df_pxs_trimmed.shape[0],
                                                                               phenotype_df_pxs_trimmed.shape[1]))
        return True, logger.logging

    def run_general_clustering_pipeline(self):
        """
        Runs data cleaning for general_clustering_pipeline.

        Args:
            NA.

        Returns:
            validation_flag: Boolean type value indicating if input data is valid or not.
            message: A message indicates the status of current check.
        """
        if self.user_spreadsheet_df is None:
            return False, logger.logging

        # Checks intersection between user spreadsheet data and phenotype data
        phenotype_df_cleaned = None
        if self.phenotype_df is not None:
            phenotype_df_cleaned = CommonUtil.check_phenotype_intersection(self.phenotype_df,
                                                                           self.user_spreadsheet_df.columns.values)
            if phenotype_df_cleaned is None:
                logger.logging.append("ERROR: Phenotype is emtpy. Please provide a valid phenotype data.")
                return False, logger.logging
        logger.logging.append("INFO: Start to process user spreadsheet data.")

        # Checks if user spreadsheet contains na value and only real number
        user_spreadsheet_df_val_check = SpreadSheet.check_user_spreadsheet_data(self.user_spreadsheet_df,
                                                                                check_na=True,
                                                                                dropna_colwise=True,
                                                                                check_real_number=True)
        if user_spreadsheet_df_val_check is None:
            return False, logger.logging

        user_spreadsheet_df_rm_na_header = SpreadSheet.remove_na_header(user_spreadsheet_df_val_check)
        if user_spreadsheet_df_rm_na_header is None:
            return False, logger.logging

        # Removes NA value and duplication on column and row name
        user_spreadsheet_df_cleaned = SpreadSheet.remove_dataframe_indexer_duplication(user_spreadsheet_df_rm_na_header)
        if user_spreadsheet_df_cleaned is None:
            return False, logger.logging

        IOUtil.write_to_file(user_spreadsheet_df_cleaned, self.run_parameters['spreadsheet_name_full_path'],
                             self.run_parameters['results_directory'], "_ETL.tsv")
        logger.logging.append(
            "INFO: Cleaned user spreadsheet has {} row(s), {} column(s).".format(
                user_spreadsheet_df_cleaned.shape[0],
                user_spreadsheet_df_cleaned.shape[1]))

        if phenotype_df_cleaned is not None:
            IOUtil.write_to_file(phenotype_df_cleaned, self.run_parameters['phenotype_name_full_path'],
                                 self.run_parameters['results_directory'], "_ETL.tsv")
            logger.logging.append(
                "INFO: Cleaned phenotype data has {} row(s), {} column(s).".format(phenotype_df_cleaned.shape[0],
                                                                                   phenotype_df_cleaned.shape[1]))
        return True, logger.logging

    def run_pasted_gene_set_conversion(self):
        """
        Runs data cleaning for pasted_gene_set_conversion.

        Args:
            NA.

        Returns:
             validation_flag: Boolean type value indicating if input data is valid or not.
            message: A message indicates the status of current check.

        """
        from utils.redis_util import RedisUtil

        # Gets redis database instance by its credential
        redis_db = RedisUtil(self.run_parameters['redis_credential'],
                             self.run_parameters['source_hint'],
                             self.run_parameters['taxonid'])

        # Reads pasted_gene_list as a dataframe
        if self.pasted_gene_df is None:
            return False, logger.logging
        logger.logging.append("INFO: Successfully load spreadsheet data: {} with {} gene(s).".format(
            self.run_parameters['pasted_gene_list_full_path'], self.pasted_gene_df.shape[0]))

        # Removes nan index rows
        input_small_genes_df = SpreadSheet.remove_na_index(self.pasted_gene_df)

        # casting index to String type
        input_small_genes_df.index = input_small_genes_df.index.map(str)

        if input_small_genes_df is None or len(input_small_genes_df.index) == 0:
            logger.logging.append("ERROR: Input data is empty. Please upload valid input data.")
            return False, logger.logging

        input_small_genes_df["user_supplied_gene_name"] = input_small_genes_df.index

        # Converts pasted_gene_list to ensemble name
        redis_ret = redis_db.get_node_info(input_small_genes_df.index, "Gene")
        ensemble_names = [x[1] for x in redis_ret]
        input_small_genes_df.index = pandas.Series(ensemble_names)

        # Filters out the unmapped genes
        mapped_small_genes_df = input_small_genes_df[~input_small_genes_df.index.str.contains(r'^unmapped.*$')]

        # Filters the duplicate gene name and write them along with their corresponding user supplied gene name to a file
        mapped_small_genes_df[(~mapped_small_genes_df.index.str.contains(
            r'^unmapped.*$') & mapped_small_genes_df.index.duplicated())][
            'user_supplied_gene_name'] = 'duplicate ensembl name'

        input_small_genes_df['status'] = input_small_genes_df.index

        IOUtil.write_to_file(input_small_genes_df, self.run_parameters['pasted_gene_list_full_path'],
                             self.run_parameters['results_directory'], "_User_To_Ensembl.tsv", use_index=False,
                             use_header=True)

        # Reads the univeral_gene_list
        universal_genes_df = IOUtil.load_data_file_wo_remove_empty_line(self.run_parameters['temp_redis_vector'])
        if universal_genes_df is None:
            return False, logger.logging

        # Inserts a column with value 0
        universal_genes_df.insert(0, 'value', 0)
        # Finds the intersection between pasted_gene_list and universal_gene_list
        common_idx = universal_genes_df.index.intersection(mapped_small_genes_df.index)
        logger.logging.append(
            "INFO: Found {} common gene(s) that shared between pasted gene list and universal gene list.".format(
                len(common_idx)))
        # inserts a column with value 1
        universal_genes_df.loc[common_idx] = 1
        # names the column of universal_genes_df to be 'uploaded_gene_set'
        universal_genes_df.columns = ["uploaded_gene_set"]
        del universal_genes_df.index.name

        # outputs final results
        IOUtil.write_to_file(mapped_small_genes_df, self.run_parameters['pasted_gene_list_full_path'],
                             self.run_parameters['results_directory'], "_MAP.tsv")
        IOUtil.write_to_file(universal_genes_df, self.run_parameters['pasted_gene_list_full_path'],
                             self.run_parameters['results_directory'], "_ETL.tsv")

        logger.logging.append("INFO: Universal gene list contains {} genes.".format(universal_genes_df.shape[0]))
        logger.logging.append("INFO: Mapped gene list contains {} genes.".format(mapped_small_genes_df.shape[0]))
        return True, logger.logging

    def run_feature_prioritization_pipeline(self):
        """
        Run data cleaning for feature prioritization pipeline.

        Args:
            NA.

        Returns:
            validation_flag: Boolean type value indicating if input data is valid or not.
            message: A message indicates the status of current check.
        """
        from knpackage.toolbox import get_spreadsheet_df

        if self.user_spreadsheet_df is None or self.phenotype_df is None:
            return False, logger.logging

        # Imputes na value on user spreadsheet data
        user_spreadsheet_df_imputed = SpreadSheet.impute_na(self.user_spreadsheet_df,
                                                            option=self.run_parameters['impute'])
        if user_spreadsheet_df_imputed is None:
            return False, logger.logging

        # Checks if value of inputs satisfy certain criteria
        user_spreadsheet_val_chked, phenotype_val_chked = CommonUtil.validate_inputs_for_gp_fp(
            user_spreadsheet_df_imputed,
            self.phenotype_df, self.run_parameters[
                'correlation_measure'])
        if user_spreadsheet_val_chked is None or phenotype_val_chked is None:
            return False, logger.logging

        IOUtil.write_to_file(user_spreadsheet_val_chked, self.run_parameters['spreadsheet_name_full_path'],
                             self.run_parameters['results_directory'], "_ETL.tsv")
        logger.logging.append(
            "INFO: Cleaned user spreadsheet has {} row(s), {} column(s).".format(
                user_spreadsheet_val_chked.shape[0],
                user_spreadsheet_val_chked.shape[1]))

        if self.run_parameters['correlation_measure'] == 't_test':
            phenotype_df = get_spreadsheet_df(self.run_parameters['phenotype_name_full_path'])
            phenotype_output = TransformationUtil.phenotype_expander(phenotype_df)
        else:
            phenotype_output = phenotype_val_chked

        IOUtil.write_to_file(phenotype_output, self.run_parameters['phenotype_name_full_path'],
                             self.run_parameters['results_directory'], "_ETL.tsv")
        logger.logging.append(
            "INFO: Cleaned phenotypic data has {} row(s), {} column(s).".format(phenotype_val_chked.shape[0],
                                                                                phenotype_val_chked.shape[1]))
        return True, logger.logging

    def run_signature_analysis_pipeline(self):
        """
        Runs data cleaning for signature_analysis_pipeline.

        Args:
             NA.

        Returns:
            validation_flag: Boolean type value indicating if input data is valid or not.
            message: A message indicates the status of current check.
        """
        if self.signature_df is None or self.user_spreadsheet_df is None:
            return False, logger.logging

        # Removes NA index for both signature data and user spreadsheet data
        signature_df = SpreadSheet.remove_na_index(self.signature_df)
        user_spreadsheet_df = SpreadSheet.remove_na_index(self.user_spreadsheet_df)

        # Checks if only real number and non-NA value appear in user spreadsheet
        if SpreadSheet.check_user_spreadsheet_data(user_spreadsheet_df, check_na=True,
                                                   check_real_number=True,
                                                   check_positive_number=False) is None:
            return False, logger.logging

        # Checks duplicate columns and rows in user spreadsheet data
        if CheckUtil.check_duplicates(user_spreadsheet_df, check_column=True, check_row=True):
            logger.logging.append("ERROR: Found duplicates on user spreadsheet data. Rejecting...")
            return False, logger.logging

        # Checks intersection of genes between signature data and user spreadsheet data
        intersection = CheckUtil.find_intersection(signature_df.index, user_spreadsheet_df.index)
        if intersection is None:
            logger.logging.append('ERROR: Cannot find intersection between spreadsheet genes and signature genes.')
            return False, logger.logging
        logger.logging.append(
            "INFO: Found {} intersected gene(s) between phenotype and spreadsheet data.".format(len(intersection)))

        # Checks number of unique value in userspread sheet equals to 2
        if not SpreadSheet.check_unique_values(user_spreadsheet_df, cnt=2):
            logger.logging.append(
                "ERROR: user spreadsheet data doesn't meet the requirment of having at least two unique values.")
            return False, logger.logging

        # Checks intersection among network data, signature data and user spreadsheet data
        if 'gg_network_name_full_path' in self.run_parameters.keys() and \
                not CommonUtil.check_network_data_intersection(intersection,
                                                               self.run_parameters):
            return False, logger.logging

        # The logic here ensures that even if phenotype data doesn't fits requirement, the rest pipelines can still run.
        if user_spreadsheet_df is None:
            return False, logger.logging
        else:
            IOUtil.write_to_file(user_spreadsheet_df,
                                 self.run_parameters['spreadsheet_name_full_path'],
                                 self.run_parameters['results_directory'], "_ETL.tsv")
            logger.logging.append(
                "INFO: Cleaned user spreadsheet has {} row(s), {} column(s).".format(
                    user_spreadsheet_df.shape[0],
                    user_spreadsheet_df.shape[1]))

        if signature_df is not None:
            IOUtil.write_to_file(signature_df, self.run_parameters['signature_name_full_path'],
                                 self.run_parameters['results_directory'], "_ETL.tsv")
            logger.logging.append(
                "INFO: Cleaned phenotype data has {} row(s), {} column(s).".format(signature_df.shape[0],
                                                                                   signature_df.shape[1]))
        return True, logger.logging


    def run_simplified_inpherno_pipeline(self):
        """
        Runs data cleaning for simplified_inpherno_pipeline.

        Args:
            NA.

        Returns:
            validation_flag: Boolean type value indicating if input data is valid or not.
            message: A message indicates the status of current check.
        """
        if self.Pvalue_gene_phenotype is None or self.expression_sample is None or self.TFexpression is None:
            return False, logger.logging
        if SpreadSheet.check_user_spreadsheet_data(self.Pvalue_gene_phenotype, check_real_number=True) is None:
            return False, logger.logging

        if SpreadSheet.check_user_spreadsheet_data(self.expression_sample, check_real_number=True) is None:
            return False, logger.logging

        if SpreadSheet.check_user_spreadsheet_data(self.TFexpression, check_real_number=True, check_na=True) is None:
            return False, logger.logging

        IOUtil.write_to_file(self.Pvalue_gene_phenotype, self.run_parameters['Pvalue_gene_phenotype_full_path'],
                             self.run_parameters['results_directory'], "_ETL.tsv")
        IOUtil.write_to_file(self.Pvalue_gene_phenotype, self.run_parameters['expression_sample_full_path'],
                             self.run_parameters['results_directory'], "_ETL.tsv")
        IOUtil.write_to_file(self.Pvalue_gene_phenotype, self.run_parameters['TFexpression_full_path'],
                             self.run_parameters['results_directory'], "_ETL.tsv", use_header=False)
        return True, logger.logging
