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
    from data_cleanup_toolbox import run_geneset_characterization_pipeline, generate_logging
    validation_flag, message = run_geneset_characterization_pipeline(run_parameters)
    generate_logging(validation_flag, message, run_parameters["results_directory"] + "/log_geneset_characterization_pipeline.yml")


def samples_clustering_pipeline(run_parameters):
    """
    Runs samples_clustering_pipeline
    Args:
        run_parameters: input configuration as dictionary
        
    Returns:
        validation_flag: Boolean type value indicating if input data is valid or not
        message: A message indicates the status of current check
    """
    from data_cleanup_toolbox import run_samples_clustering_pipeline, generate_logging
    validation_flag, message = run_samples_clustering_pipeline(run_parameters)
    generate_logging(validation_flag, message, run_parameters["results_directory"] + "/log_samples_clustering_pipeline.yml")


def gene_prioritization_pipeline(run_parameters):
    """
    Runs gene_prioritization_pipeline
    Args:
        run_parameters: input configuration as dictionary
        
    Returns:
        validation_flag: Boolean type value indicating if input data is valid or not
        message: A message indicates the status of current check
    """
    from data_cleanup_toolbox import run_gene_prioritization_pipeline, generate_logging
    validation_flag, message = run_gene_prioritization_pipeline(run_parameters)
    generate_logging(validation_flag, message, run_parameters["results_directory"] + "/log_gene_prioritization_pipeline.yml")


SELECT = {
    "geneset_characterization_pipeline": geneset_characterization_pipeline,
    "samples_clustering_pipeline": samples_clustering_pipeline,
    "gene_prioritization_pipeline": gene_prioritization_pipeline
}


def data_cleanup():
    run_directory, run_file = get_run_directory_and_file(sys.argv)
    run_parameters = get_run_parameters(run_directory, run_file)
    SELECT[run_parameters['pipeline_type']](run_parameters)


if __name__ == "__main__":
    data_cleanup()
