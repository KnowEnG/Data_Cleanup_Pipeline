import pandas
import utils.log_util as logger
from utils.redis_util import RedisUtil
from utils.io_util import IOUtil


class GeneMappingUtil:
    @staticmethod
    def map_ensemble_gene_name(dataframe, run_parameters):
        """
        Checks if the gene name follows ensemble format.

        Args:
            dataframe: input DataFrame
            run_parameters: user configuration from run_file

        Returns:
             output_df_mapped_dedup: cleaned DataFrame
        """

        redis_db = RedisUtil(run_parameters['redis_credential'],
                             run_parameters['source_hint'],
                             run_parameters['taxonid'])
        # copy index to new column named with 'user_supplied_gene_name'
        dataframe = dataframe.assign(user_supplied_gene_name=dataframe.index)
        redis_ret = redis_db.get_node_info(dataframe.index, "Gene")
        # extract ensemble names as a list from a call to redis database
        ensemble_names = [x[1] for x in redis_ret]

        # resets dataframe's index with ensembel name
        dataframe.index = pandas.Series(ensemble_names)
        # extracts all mapped rows in dataframe
        output_df_mapped = dataframe[~dataframe.index.str.contains(r'^unmapped.*$')]
        if output_df_mapped.empty:
            logger.logging.append("ERROR: No valid ensemble name can be found.")
            return None

        # removes the temporary added column to keep original shape
        output_df_mapped = output_df_mapped.drop('user_supplied_gene_name', axis=1)
        output_df_mapped_dedup = output_df_mapped[~output_df_mapped.index.duplicated()]

        dup_cnt = output_df_mapped.shape[0] - output_df_mapped_dedup.shape[0]
        if dup_cnt > 0:
            logger.logging.append("INFO: Found {} duplicate Ensembl gene name.".format(dup_cnt))

        # The following logic is to generate UNMAPPED/MAP file with two columns
        # extract two columns, index is ensembl name and column 'user_supplied_gene_name' is user supplied gene
        mapping = dataframe[['user_supplied_gene_name']]

        # filter the mapped gene
        map_filtered = mapping[~mapping.index.str.contains(r'^unmapped.*$')]
        logger.logging.append("INFO: Mapped {} gene(s) to ensemble name.".format(map_filtered.shape[0]))

        # filter the unmapped gene
        unmap_filtered = mapping[mapping.index.str.contains(r'^unmapped.*$')]
        if unmap_filtered.shape[0] > 0:
            logger.logging.append("INFO: Unable to map {} gene(s) to ensemble name.".format(unmap_filtered.shape[0]))

        # filter out the duplicated ensemble gene name
        map_filtered_dedup = map_filtered[~map_filtered.index.duplicated()]

        # writes dedupped mapping between user_supplied_gene_name and ensemble name to a file
        IOUtil.write_to_file(map_filtered_dedup, run_parameters['spreadsheet_name_full_path'],
                             run_parameters['results_directory'],
                             "_MAP.tsv", use_index=True, use_header=False)

        # adds a status column
        mapping = mapping.assign(status=dataframe.index)

        # filter the duplicate gene name and write them along with their corresponding user supplied gene name to a file
        mapping.loc[(~dataframe.index.str.contains(
            r'^unmapped.*$') & mapping.index.duplicated()), 'status'] = 'duplicate ensembl name'

        # writes user supplied gene name along with its mapping status to a file
        IOUtil.write_to_file(mapping, run_parameters['spreadsheet_name_full_path'],
                             run_parameters['results_directory'],
                             "_User_To_Ensembl.tsv", use_index=False, use_header=True)

        return output_df_mapped_dedup
