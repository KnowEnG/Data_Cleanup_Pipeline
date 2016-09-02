"""
This is a module to extract data from the network database
given edge type and taxon value.
"""
import os.path
import csv
import logging
import pandas
import mysql.connector as sql
from pandas import DataFrame

log = logging.getLogger()

def read_database(etype, taxon, global_log):
    """
    This is to extract data from network database and create raw.edge file.
    Parameters:
    etype: the edge type of gene set collection selected by user.
    taxon: the taxon value of gene set collection.
    """
    log = global_log
    log.info("Loading data from database: knownbs.dyndns.org")
    folder_path = etype + '.' + taxon
    conn = sql.connect(host="knownbs.dyndns.org", port="3307", user="KNviewer", password="dbdev249")
    cursor = conn.cursor()
    cursor.execute("use KnowNet")
    cursor.execute("""SELECT n1_id, n2_id, weight, et_name \
	    FROM edge e, node_species ns1, node_species ns2 \
	    WHERE e.et_name=%s \
	    AND e.n1_id = ns1.node_id \
	    AND e.n2_id = ns2.node_id \
	    AND ns1.taxon=%s \
	    AND ns2.taxon=%s;""", (etype, taxon, taxon))
    result = cursor.fetchall()
    file_path = os.path.join(folder_path, "nodes.mm")
    data_frame = DataFrame(result)
    print("the org size is " + str(len(data_frame.index)))

    log.info(type(result))

    log.info(data_frame[:3])
    log.info("Creating lookup table")
    lookup_table = create_lookup_table(data_frame, folder_path)

    log.info("Creating symmetric Matrix")
    df_comb_symmetric = create_symmetric_matrix(data_frame, lookup_table)

    df_comb_symmetric.to_csv(file_path, header=None, index=None, sep='\t')
    conn.close()


def create_lookup_table(data_frame, folder_path):
    """
    Creates lookup table based on all the gene name
    Args:
        df: the dataframe which contains all the gene string
        folder_path: the location to store the lookup table

    Returns:
        lookup_table: the output lookup table

    """
    # create lookup table
    lookup = pandas.concat([data_frame[0], data_frame[1]]).unique()
    lookup.sort()
    lookup_table = {}

    file_path = os.path.join(folder_path, "nodes.idx_map")
    writer = csv.writer(open(file_path, "w"))
    for idx, val in enumerate(lookup):
        lookup_table[val] = idx
        writer.writerow([val, idx])
    return lookup_table


def break_bidirectional_link(df_symmetric_two_nodes):
    return df_symmetric_two_nodes


def create_symmetric_matrix(data_frame, lookup_table):
    """
    Creates a sparse symmetric matrix based on a dataframe and lookup table
    Args:
        df: dataframe to be operate on
        lookup_table: lookup_table which contains the index
                      mapping for dataframe column0 and column1

    Returns:
        df_comb_symmetric: a sparse symmetric matrix which contains
                           three columns: gene_1, gene_2, weight

    """
    # Temporary array to store the converted data frame


    temp_array = []
    for row in data_frame.itertuples():
        cur_strings_to_sort = [row[1], row[2]]
        cur_strings_to_sort.sort()
        dict_tmp = []
        dict_tmp.append(row[1])
        dict_tmp.append(row[2])
        dict_tmp.append(row[3])
        dict_tmp.append(row[4])
        dict_tmp.append(cur_strings_to_sort[0])
        dict_tmp.append(cur_strings_to_sort[1])
        temp_array.append(dict_tmp)

    data_frame = DataFrame(temp_array)
    print(data_frame[:3])

    data_frame.sort_values(by=2, ascending=True)
    # Remove duplicates
    data_frame.drop_duplicates()
    print("after drop duplicates size = " + str(len(data_frame.index)))

    # Generates two extra columns by concatenating
    # col1 and col2 in two different orders
    data_frame['concatA'] = data_frame[0] + data_frame[1]
    data_frame['concatB'] = data_frame[1] + data_frame[0]
    print("after concat size = " + str(len(data_frame.index)))

    # Filters out the single directional link
    data_frame['success'] = data_frame['concatA'].isin(data_frame['concatB']).astype(int)

    # Selects existing symmetric relationship
    # ~23%
    df_symmetric = data_frame.loc[(data_frame['success'] == 1)]
    file_path = os.path.join(
        "/Users/jingge2/PycharmProjects/KnowEng/Gene-Set-Characterization/toolbox/PPI_direct_interaction.9606",
        "symmetric")
    df_symmetric.to_csv(file_path, header=None, index=None, sep=',')
    print("the existing symmetric size is " + str(len(df_symmetric.index)))

    # ~20% of data
    df_symmetric_two_nodes = df_symmetric.loc[(data_frame['concatA'] != data_frame['concatB'])]
    print("the existing real symmetric size is " + str(len(df_symmetric_two_nodes.index)))
    print(df_symmetric_two_nodes[:3])
    df_symmetric_two_nodes.sort_values([0,1,2,3, 'concatA', 'concatB'], ascending=False)
    file_path = os.path.join(
        "/Users/jingge2/PycharmProjects/KnowEng/Gene-Set-Characterization/toolbox/PPI_direct_interaction.9606",
        "symmetric_two_nodes")
    df_symmetric_two_nodes.to_csv(file_path, header=None, index=None, sep=',')

    unique_node_col0 = df_symmetric_two_nodes[0].tolist()
    unique_node_col1 = df_symmetric_two_nodes[1].tolist()
    print(len(unique_node_col0))

    temp_dict01 = {}
    temp_dict10 = {}
    for i, val in enumerate(unique_node_col0):
        temp_dict01[val] = unique_node_col1[i]
        temp_dict10[unique_node_col1[i]] = val

    final_list = {}
    for key, value in temp_dict01.items():
        if temp_dict10[value] == key:
            final_list[key] = value

    print("the final list is " + str(len(final_list)))


    # ~2.5% of data
    single_node_cyclic = df_symmetric.loc[(df_symmetric['concatA'] == df_symmetric['concatB'])]
    print("number of single onde = " + str(len(single_node_cyclic.index)))
    file_path = os.path.join(
        "/Users/jingge2/PycharmProjects/KnowEng/Gene-Set-Characterization/toolbox/PPI_direct_interaction.9606",
        "single_node")
    single_node_cyclic.to_csv(file_path, header=None, index=None, sep=',')

    # Generates bidirectional network for those who only have
    # single directional relationship stored in database
    df_unsymmetric_a = data_frame[data_frame['success'] == 0]

    print("the unsymmetric size == " + str(len(df_unsymmetric_a.index)))

    df_unsymmetric_b = df_unsymmetric_a[[1, 0, 2, 3, 'concatA', 'concatB', 'success']]
    df_unsymmetric = df_unsymmetric_a.append(df_unsymmetric_b, ignore_index=True)
    df_comb_symmetric = df_symmetric.append(df_unsymmetric, ignore_index=True)

    # Drops the extra columns
    df_comb_symmetric = df_comb_symmetric.drop([3, 'concatA', 'concatB', 'success'], axis=1)
    # Reindex row
    df_comb_symmetric = df_comb_symmetric.reset_index(drop=True)

    print("the final matrix size is " + str(len(df_comb_symmetric.index)))

    return df_comb_symmetric
