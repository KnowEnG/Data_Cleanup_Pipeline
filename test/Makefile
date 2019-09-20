# ----------------------------------------------------------------
# Variables setup Section                                        -
# ----------------------------------------------------------------
SCRIPT =        ../src/data_cleanup.py
STATIS =        ../src/data_checker.py
RUN_DIR =       ./run_dir
DATA_DIR =      ../data/spreadsheets
RESULTS_DIR =   $(RUN_DIR)/results
RUN_FILES_DIR = ../data/run_files

RMR =           rm -rf
GZIP =          gzip
GZIP_D =        gzip -df
MKDIR_P =       mkdir -p

.PHONY: env_setup create_run_dir copy_run_files clean_dir_recursively final_clean 

# ----------------------------------------------------------------
# Benchmarks setup Section                                       -
# ----------------------------------------------------------------
env_setup: create_run_dir copy_run_files

create_run_dir:
	$(MKDIR_P) $(RESULTS_DIR) 

copy_run_files:
	cp $(RUN_FILES_DIR)/*.yml $(RUN_DIR)

# ----------------------------------------------------------------
# Large Benchmarks RUN Section                                   -
# ----------------------------------------------------------------
run_data_cleaning:
	python3 $(SCRIPT) -run_directory $(RUN_DIR) -run_file data_cleanup.yml

run_pasted_gene_set_conversion:
	python3 $(SCRIPT) -run_directory $(RUN_DIR) -run_file pasted_gene_set_conversion.yml

# ----------------------------------------------------------------
# Small Benchmarks RUN Section                                   -
# ----------------------------------------------------------------
run_data_checker:
	python3 ../src/data_checker.py -run_directory $(RUN_DIR) -run_file TEST_1_data_cleanup.yml

run_data_cleaning_small:
	python3 $(SCRIPT) -run_directory $(RUN_DIR) -run_file TEST_1_data_cleanup.yml

run_samples_clustering_pipeline:
	python3 $(SCRIPT) -run_directory $(RUN_DIR) -run_file TEST_1_samples_clustering_pipeline.yml

run_gene_prioritization_pipeline_pearson:
	python3 $(SCRIPT) -run_directory $(RUN_DIR) -run_file TEST_1_gene_prioritization_pipeline_pearson.yml

run_gene_prioritization_pipeline_t_test:
	python3 $(SCRIPT) -run_directory $(RUN_DIR) -run_file TEST_1_gene_prioritization_pipeline_t_test.yml

run_geneset_characterization_pipeline:
	python3 $(SCRIPT) -run_directory $(RUN_DIR) -run_file TEST_1_geneset_characterization_pipeline.yml

run_phenotype_prediction_pipeline:
	python3 $(SCRIPT) -run_directory $(RUN_DIR) -run_file TEST_1_phenotype_prediction_pipeline.yml

run_general_clustering_pipeline:
	python3 $(SCRIPT) -run_directory $(RUN_DIR) -run_file TEST_1_general_clustering_pipeline.yml

run_feature_prioritization_pipeline_pearson:
	python3 $(SCRIPT) -run_directory $(RUN_DIR) -run_file TEST_1_feature_prioritization_pipeline_pearson.yml

run_feature_prioritization_pipeline_t_test_binary:
	python3 $(SCRIPT) -run_directory $(RUN_DIR) -run_file TEST_1_feature_prioritization_pipeline_t_test.yml

run_feature_prioritization_pipeline_t_test_replace:
	python3 $(SCRIPT) -run_directory $(RUN_DIR) -run_file TEST_2_feature_prioritization_pipeline_t_test.yml

run_feature_prioritization_pipeline_t_test_expand:
	python3 $(SCRIPT) -run_directory $(RUN_DIR) -run_file TEST_3_feature_prioritization_pipeline_t_test.yml

run_feature_prioritization_pipeline_t_test_mixed:
	python3 $(SCRIPT) -run_directory $(RUN_DIR) -run_file TEST_4_feature_prioritization_pipeline_t_test.yml

run_feature_prioritization_pipeline_edgeR_binary:
	python3 $(SCRIPT) -run_directory $(RUN_DIR) -run_file TEST_1_feature_prioritization_pipeline_edgeR.yml

run_feature_prioritization_pipeline_edgeR_replace:
	python3 $(SCRIPT) -run_directory $(RUN_DIR) -run_file TEST_2_feature_prioritization_pipeline_edgeR.yml

run_feature_prioritization_pipeline_edgeR_expand:
	python3 $(SCRIPT) -run_directory $(RUN_DIR) -run_file TEST_3_feature_prioritization_pipeline_edgeR.yml

run_feature_prioritization_pipeline_edgeR_mixed:
	python3 $(SCRIPT) -run_directory $(RUN_DIR) -run_file TEST_4_feature_prioritization_pipeline_edgeR.yml

run_signature_analysis_pipeline:
	python3 $(SCRIPT) -run_directory $(RUN_DIR) -run_file TEST_1_signature_analysis_pipeline.yml

run_file_format_test:
	python3 $(SCRIPT) -run_directory $(RUN_DIR) -run_file TEST_2_data_cleanup.yml

run_simplified_inpherno_pipeline:
	python3 $(SCRIPT) -run_directory $(RUN_DIR) -run_file TEST_1_simplified_inpherno_pipeline.yml

run_status_checker:
	python3 $(STATIS) -run_directory $(RUN_DIR) -run_file data_status.yml 

# ----------------------------------------------------------------
# - VERIFICATION TESTS RUN SECTION                                       -
# ----------------------------------------------------------------
verification_tests:
	python3 ./integration/verify_benchmarks.py

# ----------------------------------------------------------------
# - UNIT TESTS RUN SECTION                                       -
# ----------------------------------------------------------------
unit_tests:
	cd unit; make all_unit_tests

# ----------------------------------------------------------------
# - CLEAN UP SECTION                                             -
# ----------------------------------------------------------------
final_clean: clean_dir_recursively

clean_dir_recursively:
	$(RMR) $(RUN_DIR)


