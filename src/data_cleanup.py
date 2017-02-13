import sys
from knpackage.toolbox import get_run_parameters, get_run_directory_and_file


def geneset_characterization_pipeline(run_parameters):
    """
    Runs geneset_characterization_pipeline
    Args:
        run_parameters: input configuration as dictionary

    Returns:
        validation_flag: Boolean type value indicating if input data is valid or not
        message: A message indicates the status of current check
    """
    from data_cleanup_toolbox import run_geneset_characterization_pipeline
    validation_flag, message = run_geneset_characterization_pipeline(run_parameters)
    return validation_flag, message


def samples_clustering_pipeline(run_parameters):
    """
    Runs samples_clustering_pipeline
    Args:
        run_parameters: input configuration as dictionary
        
    Returns:
        validation_flag: Boolean type value indicating if input data is valid or not
        message: A message indicates the status of current check
    """
    from data_cleanup_toolbox import run_samples_clustering_pipeline
    validation_flag, message = run_samples_clustering_pipeline(run_parameters)
    return validation_flag, message


def gene_priorization_pipeline(run_parameters):
    """
    Runs gene_priorization_pipeline
    Args:
        run_parameters: input configuration as dictionary
        
    Returns:
        validation_flag: Boolean type value indicating if input data is valid or not
        message: A message indicates the status of current check
    """
    from data_cleanup_toolbox import run_gene_priorization_pipeline
    validation_flag, message = run_gene_priorization_pipeline(run_parameters)
    return validation_flag, message


def post_processing_phenotype_clustering_data(run_parameters):
    from data_cleanup_toolbox import run_post_processing_phenotype_clustering_data
    output = run_post_processing_phenotype_clustering_data(run_parameters)

SELECT = {
    "geneset_characterization_pipeline": geneset_characterization_pipeline,
    "samples_clustering_pipeline": samples_clustering_pipeline,
    "gene_priorization_pipeline": gene_priorization_pipeline,
    "post_processing_phenotype_clustering_data" : post_processing_phenotype_clustering_data
}


def data_cleanup():
    run_directory, run_file = get_run_directory_and_file(sys.argv)
    run_parameters = get_run_parameters(run_directory, run_file)
    SELECT[run_parameters['pipeline_type']](run_parameters)


if __name__ == "__main__":
    data_cleanup()
