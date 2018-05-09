import sys
import utils.log_util as logger
from knpackage.toolbox import get_run_parameters, get_run_directory_and_file
from data_cleanup_toolbox import Pipelines


SELECT = {
    "samples_clustering_pipeline": "run_samples_clustering_pipeline",
    "general_clustering_pipeline": "run_general_clustering_pipeline",
    "geneset_characterization_pipeline": "run_geneset_characterization_pipeline",
    "gene_prioritization_pipeline": "run_gene_prioritization_pipeline",
    "phenotype_prediction_pipeline": "run_phenotype_prediction_pipeline",
    "pasted_gene_set_conversion": "run_pasted_gene_set_conversion",
    "feature_prioritization_pipeline": "run_feature_prioritization_pipeline",
    "signature_analysis_pipeline": "run_signature_analysis_pipeline"
}


def run_pipelines(run_parameters, method):
    validation_flag, message = getattr(Pipelines(run_parameters), method)()
    logger.generate_logging(validation_flag, message,
                            run_parameters["results_directory"] + "/log_" + run_parameters["pipeline_type"] + ".yml")


def data_cleanup():
    try:
        logger.init()
        run_directory, run_file = get_run_directory_and_file(sys.argv)
        run_parameters = get_run_parameters(run_directory, run_file)
        run_pipelines(run_parameters, SELECT[run_parameters['pipeline_type']])
    except Exception as err:
        logger.logging.append("ERROR: {}".format(str(err)))
        raise RuntimeError(str(err))


if __name__ == "__main__":
    data_cleanup()
