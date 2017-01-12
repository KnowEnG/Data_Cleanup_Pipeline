import sys
import redis_utilities as redutil
import data_cleanup_toolbox as datacln
from knpackage.toolbox import get_run_parameters, get_run_directory_and_file


def pasted_gene_cleanup(run_parameters):
    # gets redis database instance by its credential
    redis_db = redutil.get_database(run_parameters['redis_credential'])

    # reads pasted_gene_list as a dataframe
    input_small_genes_df = datacln.load_data_file(run_parameters['pasted_gene_list_full_path'])
    input_small_genes_df["original_gene_name"] = input_small_genes_df.index

    # converts pasted_gene_list to ensemble name
    input_small_genes_df.index = input_small_genes_df.index.map(lambda x: redutil.conv_gene(redis_db, x, run_parameters['source_hint'], run_parameters['taxonid']))

    # filters out the unmapped genes
    mapped_small_genes_df = input_small_genes_df[~input_small_genes_df.index.str.contains(r'^unmapped.*$')]
    # insert a column with column name: value 
    mapped_small_genes_df.insert(0, 'value', 1)

    # reads the univeral_gene_list
    universal_genes_df = datacln.load_data_file(run_parameters['temp_redis_vector'])
    # inserts a column with value 0
    universal_genes_df.insert(0, 'value', 0)

    # finds the intersection between pasted_gene_list and universal_gene_list
    common_idx = universal_genes_df.index.intersection(mapped_small_genes_df.index)
    # inserts a column with value 1
    universal_genes_df.loc[common_idx] = 1

    # names the column of universal_genes_df to be 'uploaded_gene_set'
    universal_genes_df.columns = ["uploaded_gene_set"]
    del universal_genes_df.index.name

    # outputs final results
    output_file_basename = datacln.get_file_basename(run_parameters['pasted_gene_list_full_path'])
    input_small_genes_df.to_csv(run_parameters['results_directory'] + '/' + output_file_basename + "_MAP.tsv", sep='\t', header=False, index=True)
    universal_genes_df.to_csv(run_parameters['results_directory'] + '/' + output_file_basename + "_ETL.tsv", sep='\t', header=True, index=True)


def main():
    run_directory, run_file = get_run_directory_and_file(sys.argv)
    run_parameters = get_run_parameters(run_directory, run_file)
    pasted_gene_cleanup(run_parameters)


if __name__ == "__main__":
    main()

