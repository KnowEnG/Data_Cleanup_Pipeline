# Commands that will be used in targets
MKDIR_P = mkdir -p
RMR = rm -rf
GZIP = gzip
GZIP_D = gzip -df
DATA_DIR = ../data/spreadsheets
# Directories that will be used in targets
RUN_DIR = ./run_dir
RUN_FILES_DIR = ../data/run_files
RESULTS_DIR = $(RUN_DIR)/results
SCRIPT = ../src/data_cleanup.py

.PHONY: env_setup create_run_dir copy_run_files clean_dir_recursively final_clean 

# Setup the running environment
env_setup: create_run_dir copy_run_files

run_data_cleaning:
	python3 $(SCRIPT) -run_directory $(RUN_DIR) -run_file data_cleanup.yml

# Clean the pipeline after running the pipelines you desire
final_clean: clean_dir_recursively

# Greate run directory
create_run_dir:
	$(MKDIR_P) $(RESULTS_DIR) 

# Copy run_files
copy_run_files:
	cp $(RUN_FILES_DIR)/data_cleanup.yml $(RUN_DIR)

# Remove run directory
clean_dir_recursively:
	$(RMR) $(RUN_DIR)
