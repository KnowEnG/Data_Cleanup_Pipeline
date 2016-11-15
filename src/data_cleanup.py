import sys
from knpackage.toolbox import get_run_parameters, get_run_directory_and_file


def geneset_characterization_pipeline(run_parameters):
    from data_cleanup_toolbox import run_geneset_characterization_pipeline
    validation_flag, error_msg = run_geneset_characterization_pipeline(run_parameters)
    return validation_flag, error_msg


def sample_clustering_pipeline(run_parameters):
    from data_cleanup_toolbox import run_sample_clustering_pipeline
    validation_flag, error_msg = run_sample_clustering_pipeline(run_parameters)
    return validation_flag, error_msg


def gene_priorization_pipeline(run_parameters):
    from data_cleanup_toolbox import run_gene_priorization_pipeline
    validation_flag, error_msg = run_gene_priorization_pipeline(run_parameters)
    return validation_flag, error_msg


SELECT = {
    "geneset_characterization_pipeline": geneset_characterization_pipeline,
    "sample_clustering_pipeline": sample_clustering_pipeline,
    "gene_priorization_pipeline": gene_priorization_pipeline
}


def data_cleanup():
    run_directory, run_file = get_run_directory_and_file(sys.argv)
    run_parameters = get_run_parameters(run_directory, run_file)
    SELECT[run_parameters['pipeline_type']](run_parameters)


if __name__ == "__main__":
    data_cleanup()
