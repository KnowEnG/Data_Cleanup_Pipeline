import sys

from knpackage.toolbox import get_run_parameters, get_run_directory_and_file
import logger


def geneset_characterization_pipeline(run_parameters):
    """
    Runs geneset_characterization_pipeline
    Args:
        run_parameters: input configuration as dictionary

    Returns:
        validation_flag: Boolean type value indicating if input data is valid or not
        message: A message indicates the status of current check
    """
    from data_cleanup_toolbox import GeneSetCharacterizationPipeline

    obj = GeneSetCharacterizationPipeline(run_parameters)
    validation_flag, message = obj.run_geneset_characterization_pipeline()
    logger.generate_logging(validation_flag, message, run_parameters["results_directory"] + "/log_geneset_characterization_pipeline.yml")

def samples_clustering_pipeline(run_parameters):
    """
    Runs samples_clustering_pipeline
    Args:
        run_parameters: input configuration as dictionary
        
    Returns:
        validation_flag: Boolean type value indicating if input data is valid or not
        message: A message indicates the status of current check
    """
    from data_cleanup_toolbox import SamplesClusteringPipeline
    obj = SamplesClusteringPipeline(run_parameters)
    validation_flag, message = obj.run_samples_clustering_pipeline()
    logger.generate_logging(validation_flag, message,
                     run_parameters["results_directory"] + "/log_samples_clustering_pipeline.yml")


def gene_prioritization_pipeline(run_parameters):
    """
    Runs gene_prioritization_pipeline
    Args:
        run_parameters: input configuration as dictionary
        
    Returns:
        validation_flag: Boolean type value indicating if input data is valid or not
        message: A message indicates the status of current check
    """
    from data_cleanup_toolbox import GenePrioritizationPipeline
    obj = GenePrioritizationPipeline(run_parameters)
    validation_flag, message = obj.run_gene_prioritization_pipeline()
    logger.generate_logging(validation_flag, message,
                     run_parameters["results_directory"] + "/log_gene_prioritization_pipeline.yml")


def phenotype_prediction_pipeline(run_parameters):
    """
        Runs phenotype_prediction_pipeline
        Args:
            run_parameters: input configuration as dictionary

        Returns:
            validation_flag: Boolean type value indicating if input data is valid or not
            message: A message indicates the status of current check
    """
    from data_cleanup_toolbox import PhenotypePredictionPipeline
    obj = PhenotypePredictionPipeline(run_parameters)
    validation_flag, message = obj.run_phenotype_prediction_pipeline()
    logger.generate_logging(validation_flag, message,
                     run_parameters["results_directory"] + "/log_phenotype_prediction_pipeline.yml")


def pasted_gene_set_conversion(run_parameters):
    """
    Runs pasted_gene_set_conversion
    Args:
        run_parameters: input configuration as dictionary
    Returns:
        validation_flag: Boolean type value indicating if input data is valid or not
        message: A message indicates the status of current chec
    """
    from data_cleanup_toolbox import PastedGeneSetConversion
    obj = PastedGeneSetConversion(run_parameters)
    validation_flag, message = obj.run_pasted_gene_set_conversion()
    logger.generate_logging(validation_flag, message,
                     run_parameters["results_directory"] + "/log_pasted_gene_set_conversion.yml")


def general_clustering_pipeline(run_parameters):
    """
    Runs general_clustering_pipeline
    Args:
        run_parameters: input configuration as dictionary
    Returns:
        validation_flag: Boolean type value indicating if input data is valid or not
        message: A message indicates the status of current check
    """
    from data_cleanup_toolbox import GeneralClusteringPipeline
    obj = GeneralClusteringPipeline(run_parameters)
    validation_flag, message = obj.run_general_clustering_pipeline()
    logger.generate_logging(validation_flag, message,
                     run_parameters["results_directory"] + "/log_general_clustering_pipeline.yml")


def feature_prioritization_pipeline(run_parameters):
    """
    Runs feature_prioritization_pipeline
    Args:
        run_parameters: input configuration as dictionary
    Returns:
        validation_flag: Boolean type value indicating if input data is valid or not
        message: A message indicates the status of current check
    """
    from data_cleanup_toolbox import FeaturePrioritizationPipeline
    obj = FeaturePrioritizationPipeline(run_parameters)
    validation_flag, message = obj.run_feature_prioritization_pipeline()
    logger.generate_logging(validation_flag, message,
                     run_parameters["results_directory"] + "/log_feature_prioritization_pipeline.yml")


def signature_analysis_pipeline(run_parameters):
    from data_cleanup_toolbox import SignatureAnalysisPipeline
    obj = SignatureAnalysisPipeline(run_parameters)
    validation_flag, message = obj.run_signature_analysis_pipeline()
    logger.generate_logging(validation_flag, message,
                     run_parameters["results_directory"] + "/log_signature_analysis_pipeline.yml")

SELECT = {
    "samples_clustering_pipeline": samples_clustering_pipeline,
    "general_clustering_pipeline": general_clustering_pipeline,
    "geneset_characterization_pipeline": geneset_characterization_pipeline,
    "gene_prioritization_pipeline": gene_prioritization_pipeline,
    "phenotype_prediction_pipeline": phenotype_prediction_pipeline,
    "pasted_gene_set_conversion": pasted_gene_set_conversion,
    "feature_prioritization_pipeline": feature_prioritization_pipeline,
    "signature_analysis_pipeline": signature_analysis_pipeline
}

def data_cleanup():
    try:
        logger.init()
        run_directory, run_file = get_run_directory_and_file(sys.argv)
        run_parameters = get_run_parameters(run_directory, run_file)
        SELECT[run_parameters['pipeline_type']](run_parameters)
    except Exception as err:
        logger.logging.append("ERROR: {}".format(str(err)))
        raise RuntimeError(str(err))

if __name__ == "__main__":
    data_cleanup()
