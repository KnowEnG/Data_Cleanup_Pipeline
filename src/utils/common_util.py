import utils.log_util as logger
from utils.check_util import CheckUtil
from utils.transformation_util import TransformationUtil


class CommonUtil:
    @staticmethod
    def remove_dataframe_indexer_duplication(input_dataframe):
        """
        Checks the validity of user input spreadsheet data file, including duplication and nan

        Args:
            input_dataframe: user spreadsheet input file DataFrame, which is uploaded from frontend
            run_parameters: run_file parameter dictionary

        Returns:
            flag: Boolean value indicates the status of current check
            message: A message indicates the status of current check
        """
        logger.logging.append("INFO: Start to run sanity checks for input data.")

        # Case 1: removes NA rows in index
        input_dataframe_idx_na_rmd = TransformationUtil.remove_na_index(input_dataframe)
        if input_dataframe_idx_na_rmd is None:
            return None

        # Case 2: checks the duplication on column name and removes it if exists
        input_dataframe_col_dedup = TransformationUtil.remove_duplicate_column_name(input_dataframe_idx_na_rmd)
        if input_dataframe_col_dedup is None:
            return None

        # Case 3: checks the duplication on gene name and removes it if exists
        input_dataframe_genename_dedup = TransformationUtil.remove_duplicate_row_name(input_dataframe_col_dedup)
        if input_dataframe_genename_dedup is None:
            return None

        logger.logging.append("INFO: Finished running sanity check for input data.")

        return input_dataframe_genename_dedup

    @staticmethod
    def check_phenotype_intersection(phenotype_df, user_spreadsheet_df_header):
        '''
        Pre-processing phenotype data. This includes checking for na index, duplicate column name and row name.
        Args:
            phenotype_df: input phenotype dataframe to be checked

        Returns:
            phenotype_df_genename_dedup: cleaned phenotype dataframe
        '''
        logger.logging.append("INFO: Start to pre-process phenotype data.")

        phenotype_df_genename_dedup = CommonUtil.remove_dataframe_indexer_duplication(phenotype_df)
        if phenotype_df_genename_dedup is None:
            return None

        # checks the intersection on phenotype
        intersection = CheckUtil.find_intersection(phenotype_df_genename_dedup.index.values,
                                                   user_spreadsheet_df_header)
        if intersection is None:
            return None

        logger.logging.append(
            "INFO: Found {} intersected gene(s) between phenotype and spreadsheet data.".format(len(intersection)))
        logger.logging.append("INFO: Finished running sanity check for phenotype data.")
        return phenotype_df_genename_dedup

    @staticmethod
    def validate_inputs_for_gp_fp(user_spreadsheet_df, phenotype_df, correlation_measure):
        """
        Input data check for Gene_Prioritization_Pipeline/Feature_Prioritization_Pipeline.

        Args:
            run_parameters: input configuration table

        Returns:
            user_spreadsheet_df_dropna: cleaned user spreadsheet
            phenotype_df_pxs: phenotype data

        """
        # Checks na, real number in user spreadsheet
        user_spreadsheet_df_chk = CheckUtil.check_user_spreadsheet_data(user_spreadsheet_df, dropna_colwise=True,
                                                                        check_real_number=True)
        if user_spreadsheet_df_chk is None or user_spreadsheet_df_chk.empty:
            logger.logging.append("ERROR: After drop NA, user spreadsheet data becomes empty.")
            return None, None

        # Checks value of phenotype dataframe for t-test and pearson
        logger.logging.append("INFO: Start to run checks for phenotypic data.")
        phenotype_df_chk = CheckUtil.check_phenotype_data(phenotype_df, correlation_measure)
        if phenotype_df_chk is None:
            return None, None

        # Checks intersection between user_spreadsheet_df and phenotype data
        user_spreadsheet_df_header = list(user_spreadsheet_df_chk.columns.values)
        phenotype_df_trimmed = CheckUtil.check_intersection_for_phenotype_and_user_spreadsheet(
            user_spreadsheet_df_header,
            phenotype_df_chk)
        if phenotype_df_trimmed is None or phenotype_df_trimmed.empty:
            logger.logging.append("ERROR: After drop NA, phenotype data becomes empty.")
            return None, None

        logger.logging.append("INFO: Finished running checks for phenotypic data.")

        return user_spreadsheet_df_chk, phenotype_df_trimmed

    @staticmethod
    def check_network_data_intersection(list_of_genes, run_parameters):
        """
        Checks intersection of genes between network data and input list
        Args:
            list_of_genes: user input genes list
            run_parameters: configuration file

        Returns:
            True/False indicating if an intersection is discovered

        """
        from knpackage.toolbox import get_network_df, extract_network_node_names, find_unique_node_names
        logger.logging.append("INFO: Start to process gene-gene network data.")
        # Loads network dataframe to check number of genes intersected between spreadsheet and network
        network_df = get_network_df(run_parameters['gg_network_name_full_path'])
        if network_df.empty:
            logger.logging.append("ERROR: Input data {} is empty. Please provide a valid input data.".format(
                run_parameters['gg_network_name_full_path']))
            return False

        node_1_names, node_2_names = extract_network_node_names(network_df)
        unique_gene_names = find_unique_node_names(node_1_names, node_2_names)
        intersection = CheckUtil.find_intersection(unique_gene_names, list_of_genes)
        if intersection is None:
            logger.logging.append(
                'ERROR: Cannot find intersection among spreadsheet genes, signature genes and network genes.')
            return False
        logger.logging.append("INFO: Found {} intersected genes on gene-gene network data.".format(len(intersection)))
        return True
