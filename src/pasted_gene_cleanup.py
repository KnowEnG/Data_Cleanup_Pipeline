import sys
import redis_utilities as redutil
import data_cleanup_toolbox as datacln
from knpackage.toolbox import get_run_parameters, get_run_directory_and_file


def pasted_gene_cleanup(run_parameters):
    redis_db = redutil.get_database(run_parameters['redis_credential'])

    input_small_genes_df = datacln.load_data_file(run_parameters['pasted_gene_list_full_path'])
    input_small_genes_df.index = input_small_genes_df.index.map(lambda x: redutil.conv_gene(redis_db, x, run_parameters['source_hint'], run_parameters['taxonid']))

    mapped_small_genes_df = input_small_genes_df[~input_small_genes_df.index.str.contains(r'^unmapped.*$')]
    mapped_small_genes_df['value'] = 1

    temp_redis_retrived_df = datacln.load_data_file(run_parameters['temp_redis_vector'])
    temp_redis_retrived_df['value'] = 0

    common_idx = temp_redis_retrived_df.index.intersection(mapped_small_genes_df.index)
    temp_redis_retrived_df.loc[common_idx] = 1
    temp_redis_retrived_df.columns = ["uploaded_gene_set"]
    del temp_redis_retrived_df.index.name

    output_file_basename = datacln.get_file_basename(run_parameters['pasted_gene_list_full_path'])
    input_small_genes_df.to_csv(run_parameters['results_directory'] + '/' + output_file_basename + "_MAP.tsv", sep='\t', header=False, index=True)
    temp_redis_retrived_df.to_csv(run_parameters['results_directory'] + '/' + output_file_basename + "_ETL.tsv", sep='\t', header=True, index=True)


def main():
    run_directory, run_file = get_run_directory_and_file(sys.argv)
    run_parameters = get_run_parameters(run_directory, run_file)
    pasted_gene_cleanup(run_parameters)


if __name__ == "__main__":
    main()

