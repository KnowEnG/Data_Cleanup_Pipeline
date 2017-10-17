# KnowEnG's Data Cleanup Pipeline
 This is the Knowledge Engine for Genomics (KnowEnG), an NIH BD2K Center of Excellence, Data Cleanup Pipeline.
This pipeline **cleanup** the data of a given spreadsheet for subsequent processing by KnowEnG Analytics Platform.

## Detailed cleanup steps for each pipeline

### geneset_characterization_pipeline
  *After removing empty rows and columns, check if a spreadsheet:*
  1. is empty. 
  2. contains NA value/s. 
  3. contains value 0 and 1.
  4. gene name has NA value. 
  5. contains duplicate column names. 
  6. contains duplicate row names. 
  7. gene names can be mapped to ensemble gene name.
  
### samples_clustering_pipeline
  *After removing empty rows and columns, check if a spreadsheet:*
  1. contains NA value.
  2. contains real values (then replace with their absolute value)
  3. gene name contains NA value.
  4. contains duplicate column name.
  5. contains duplicate row name.
  6. gene name can be mapped to ensemble gene name.
  7. intersects  gene-gene network data (network option only) 

  *If the user provides with the phenotype data:*
  *After removing empty rows and columns, check if a phenotypic spreadsheet:*
  1. contains duplicate column name. 
  2. contains duplicate row name. 
  3. intersects with the genomic spreadsheet.

  *If the user provides with the network data:* 
  1. is empty.
  2. intersects with genomic spreadsheet.
  
  
### gene_prioritization_pipeline
  *After removing empty rows and columns, check if a spreadsheet:*
  1. genomic or phenotypic data is empty. 
  2. column contains NA.
  3. contains real value.
  5. contains NA gene name in user spreadsheet.
  6. contains duplicate column name. 
  7. contains duplicate row name. 
  8. gene name can be mapped to ensemble gene name.
  
  *If the user provides with the phenotypic spreadsheet:*
  1. for every single drug:
    1. drops NA.
    2. intersects header with spreadsheet header, number of intersection >= 2.
  2. for t_test, contains only value 0, 1 or NAN.
  3. for pearson test, contains only real value or NAN
  
### pasted_gene_list
  *After removing empty rows and columns, check if a spreadsheet:*
  1. input genes contains NA.
  2. casts index of input genes dataframe to string type
  3. intersects with universal genes list from redis database

### general_clustering_pipeline
  *After removing empty rows and columns, check if a spreadsheet:*
  1. contains NA value.
  2. contains real value. 
  3. contains NA value in gene name.
  4. contains NA value in header.
  5. contains duplicate row names. 
  6. contains duplicate column names. 
  
  *If the user provides with the phenotype data:*
  *After removing empty rows and columns, check if a phenotypic spreadsheet:*
  1. contains duplicate column name. 
  2. contains duplicate row name. 
  3. intersects with the genomic spreadsheet.

* * * 
## How to run this pipeline with our data
* * * 

### 1. Clone the Data_Cleanup_Pipeline Repo
```
 git clone https://github.com/KnowEnG-Research/Data_Cleanup_Pipeline.git
```
 
### 2. Install the following (Ubuntu or Linux)
```
 apt-get install -y python3-pip
 apt-get install -y libblas-dev liblapack-dev libatlas-base-dev gfortran
 pip3 install numpy==1.11.1
 pip3 install pandas==0.18.1
 pip3 install scipy==0.19.1
 pip3 install scikit-learn==0.17.1
 apt-get install -y libfreetype6-dev libxft-dev
 pip3 install matplotlib==1.4.2
 pip3 install xmlrunner
 pip3 install pyyaml
 pip3 install knpackage
 pip3 install redis
```

### 3. Change directory to Data_Cleanup_Pipeline

```
cd Data_Cleanup_Pipeline 
```

### 4. Change directory to test

```
cd test
```
 
### 5. Create a local directory "run_dir" and place all the run files in it
```
make env_setup
```

### 6. Use one of the following "make" commands to select and run a data cleanup pipeline


| **Command**                        | **Option**                                        | 
|:---------------------------------- |:------------------------------------------------- | 
| make run_data_cleaning          | example test with large dataset |
| make run_samples_clustering_pipeline          | samples clustering test                                       |
| make run_gene_prioritization_pipeline_pearson | pearson correlation test                   |
| make run_gene_prioritization_pipeline_t_test     | t-test correlation test          |
| make run_geneset_characterization_pipeline | geneset characterization test  |
| make run_general_clustering_pipeline          | general clustering test                                       |
| make run_pasted_gene_list          | pasted gene list test                                      |

* * * 
## How to run this pipeline with Your data
* * * 

__***Follow steps 1-4 above then do the following:***__

### * Create your run directory

 ```
 mkdir run_directory
 ```

### * Change directory to the run_directory

 ```
 cd run_directory
 ```

### * Create your results directory

 ```
 mkdir results_directory
 ```
 
### * Create run_paramters file  (YAML Format)
 ``` 
Look for examples of run_parameters in ./Data_Cleanup_Pipeline/data/run_files/TEMPLATE_data_cleanup.yml
 ```
### * Modify run_paramters file  (YAML Format)
```
set the spreadsheet, and drug_response (phenotype data) file names to point to your data
```

### * Run the Data Cleanup Pipeline:

  * Update PYTHONPATH enviroment variable
   ``` 
   export PYTHONPATH='../src':$PYTHONPATH    
   ```
   
  * Run (these relative paths assume you are in the test directory with setup as described above)
   ```
  python3 ../src/data_cleanup.py -run_directory ./run_dir -run_file TEMPLATE_data_cleanup.yml
   ```

* * * 
## Description of "run_parameters" file
* * * 

| **Key**                    | **Value**                            | **Comments**                                      |
| -------------------------- | ------------------------------------ | ------------------------------------------------- |
| pipeline_type              | **gene_priorization_pipeline**, ...  | Choose pipeline cleaning type                     |
| spreadsheet_name_full_path | directory+spreadsheet_name           | Path and file name of user genomic spreadsheet    |
| phenotype_full_path        | directory+phenotype_data_name        | Path and file name of user phenotypic spreadsheet |
| gg_network_name_full_path  | directory+gg_network_name            | Path and file name of user network                |
| results_directory          | directory                            | Directory to save the output files                |
| redis_credential           | host, password and port              | Credential to access gene names lookup            |
| taxonid                    | 9606                                 | Taxon id of the genes                             |
| source_hint                | ' '                                  | Hint for lookup ensembl names                     |
| correlation_measure        | t_test/pearson                       | Correlation measure gene prioritization pipeline  |



spreadsheet_name_full_path = TEST_1_gene_expression.tsv
phenotype_full_path = TEST_1_phenotype.tsv

* * * 
## Description of Output files saved in results directory
* * * 

* Output files

**input_file_name_ETL.tsv**.
Input file after Extract Transform Load (cleaning)

**input_file_name_MAP.tsv**.

| (translated gene)      | (input gene name)    |
| :--------------------: |:--------------------:|
| ENS00000012345         | abc_def_er           |
|...                     |...                   |
| ENS00000054321         | def_org_ifi          |


**input_file_name_UNMAPPED.tsv**.

| (input gene name)      | (unmapped-none) |
| :--------------------: |:---------------:|
| abcd_iffe              | unmapped-none   |
|...                     |...              |
| abdcefg_hijk           | unmapped-none   |
