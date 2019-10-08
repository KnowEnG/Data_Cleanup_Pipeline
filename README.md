# KnowEnG's Data Cleanup Pipeline
 This is the Knowledge Engine for Genomics (KnowEnG), an NIH BD2K Center of Excellence, Data Cleanup Pipeline.
This pipeline **cleanup** the data of a given spreadsheet for subsequent processing by KnowEnG Analytics Platform.

## Detailed cleanup steps for each pipeline

### geneset_characterization_pipeline
  *After removing empty rows and columns for user spreadsheet data, check :*
  1. if spreadsheet is empty, reject.
  2. if spreadsheet contains NaN value/s, drop the corresponding columns.  
  3. if spreadsheet contains only non-negative real value. If not, reject.
  4. if spreadsheet contains NaN value, reject. 
  5. if spreadsheet contains duplicate column names, remove the duplicated column.
  6. if spreadsheet contains duplicate row names, remove the duplicated row.
  7. if spreadsheet gene names can be mapped to ensemble gene name, then generates mapping files.
  
  
### samples_clustering_pipeline
  *After removing empty rows and columns for user spreadsheet data, check:*
  1.  if spreadsheet contains NaN value/s, drop the corresponding columns.
  2.  if spreadsheet contains only real, positive values, accept. If not, reject.
  3.  if spreadsheet contains NaN value in gene name, remove corresponding rows.
  4.  if spreadsheet contains duplicate column name, remove duplicate columns.
  5.  if spreadsheet contains duplicate row name, remove duplicate rows.
  6.  map spreadsheet gene name to ensemble name and generates mapping files.

  *If the user provides with the network data, check :* 
  1. if network data is empty, reject.
  2. if network data can not be intersected with genomic spreadsheet, reject.
  
  *If the user provides with the phenotype data, after removing empty rows and columns, check :*
  3. if phenotypic data cannot be intersected with the genomic spreadsheet, reject. 

  
### gene_prioritization_pipeline
  *After removing empty rows and columns for user spreadsheet data, check :*
  1. based on impute option user selected:
     a. reject: reject user spreadsheet if it contains any missing values.
     b. average: replace missing values with the mean value of the containing row.
     c. remove: drop any columns with missing values.
  2. reject if genomic or phenotypic data is empty. 
  3. if spreadsheet contains non-real values, reject.
  4. remove any rows whose gene names are missing.
  5. if spreadsheet contains duplicate column name, remove duplicate columns. 
  6. if spreadsheet contains duplicate row name, remove duplicate rows. 
  7. map spreadsheet gene name to ensemble name and generates mapping files.
  
  *After removing empty rows and columns phenotype data, check:*
  1. If the correlation measure is t-test or edgeR...
    a. Force any string phenotypes to lowercase.
    b. Convert each phenotype to binary encoding. For each phenotype, let `num_distinct_values` be the number of distinct values, excluding NA, in the phenotype.
        - if `num_distinct_values` < 2, drop the phenotype.
        - if `num_distinct_values` == 2 and the two distinct values are 0 and 1, leave the phenotype unchanged.
        - if `num_distinct_values` == 2 and the two distinct values are not 0 and 1, replace all instances of one of the distinct values with 0 and replace all instances of the other distinct value with 1. Preserve any missing values. Edit the phenotype name to indicate which of the original values is now represented by 1.
        - if `num_distinct_values` > 2, expand the phenotype into `num_distinct_values` indicator phenotypes; any NAs will be preserved.
    c. For each of the binary phenotypes present at the end of step 1b, count the number of samples having value 0
       and the number of samples having value 1. If either of those counts is less than 2, drop the phenotype.
    d. Confirm at least one phenotype remains.
  2. for pearson test, check if a phenotypic data contains only numeric value. If not, reject.
  3. for every single drug:
    1. drops NA for the current drug.
    2. intersects header with spreadsheet. If an intersection exists, add this drug to a common list until iterate through all drugs. 
    3. checks if the common list return by step 2 is emtpy. If it's empty, reject.
  
  
### pasted_gene_list
  *After removing empty rows and columns in user spreadsheet data, check:*
  1. if a spreadsheet input gene names contain NaN value/s, remove corresponding rows. 
  2. casts index of input genes dataframe to string type
  3. retrieve gene mapping status from database and creates a status column to existing dataframe
  4. if the dataframe from step 3 intersects with universal genes list from redis database, mark the intersected genes with value 1, else 0.


### general_clustering_pipeline
  *After removing empty rows and columns for user spreadsheet data, check :*
  1.  if spreadsheet contains NaN value/s, drop the corresponding columns.
  2.  if spreadsheet contains only real, values, accept. If not, reject.
  3. if spreadsheet contains NaN value in gene name, remove corresponding rows.
  4. if spreadsheet contains NaN value in header, remove corresponding columns. 
  5. if spreadsheet contains duplicate row names, remove duplicate rows.
  6. if spreadsheet contains duplicate column names, remove duplicate columns.
  
  *If the user provides with the phenotype data:*
  *After removing empty rows and columns, check :*
  1. if phenotypic spreadsheet contains duplicate column name, remove duplicate column. 
  2. if phenotypic spreadsheet contains duplicate row name, remove duplicate row. 
  3. if phenotypic spreadsheet intersects with the genomic spreadsheet, accept. If not, reject.


### signature_analysis_pipeline
  *After removing empty rows and columns for user spreadsheet data, check :*
  1. if spreadsheet contains NaN value/s, drop the corresponding columns.
  2. if spreadsheet contains only positive, real value, accept. If not, reject. 
  3. if spreadsheet contains duplicate row names, reject. 
  4. if spreadsheet contains duplicate column names, reject. 
  5. if spreadsheet contains at least two unique values per column, accpet. If not, reject.
  6. map spreadsheet gene name to ensemble name and generates mapping files.

  *After removing empty rows and columns for signature data, check :*
  1. if signature data can be intersected with spreadsheet.

  *If the user provides with the network data, check :*
  1. if the unique genes in network data has intersection with signature data and spreadsheet data. 


### feature_prioritization_pipeline
  *After removing empty rows and columns in user spreadsheet data, check :*
  1. based on impute option user selected:
     a. reject: reject user spreadsheet if there is NA.
     b. average: replace NA value with mean of each row.
     c. remove: drop any columns with missing values.
  2. if spreadsheet contains non-real values, reject.
  3. if correlation is edgeR and spreadsheet contains negative values, reject;
     otherwise, accept.
 
  *After removing empty rows and columns, check:*
  1. If the correlation measure is t-test or edgeR...
    a. Force any string phenotypes to lowercase.
    b. Convert each phenotype to binary encoding. For each phenotype, let `num_distinct_values` be the number of distinct values, excluding NA, in the phenotype.
        - if `num_distinct_values` < 2, drop the phenotype.
        - if `num_distinct_values` == 2 and the two distinct values are 0 and 1, leave the phenotype unchanged.
        - if `num_distinct_values` == 2 and the two distinct values are not 0 and 1, replace all instances of one of the distinct values with 0 and replace all instances of the other distinct value with 1. Preserve any missing values. Edit the phenotype name to indicate which of the original values is now represented by 1.
        - if `num_distinct_values` > 2, expand the phenotype into `num_distinct_values` indicator phenotypes; any NAs will be preserved.
    c. For each of the binary phenotypes present at the end of step 1b, count the number of samples having value 0
       and the number of samples having value 1. If either of those counts is less than 2, drop the phenotype.
    d. Confirm at least one phenotype remains.
  2. for pearson test, check if a phenotypic data contains only numeric value. If not, reject.
  3. for every single phenotype:
    1. drops NA for the current phenotype.
    2. intersects header with spreadsheet. If an intersection exists, add this phenotype to a common list until iterate through all phenotypes. 
    3. checks if the common list return by step 2 is emtpy. If it's empty, reject.

### phenotype_prediction_pipeline
  *After removing empty rows and columns in user spreadsheet data, check :*
  1. if spreadsheet contains NaN value/s, drop the corresponding columns.
  2. if spreadsheet contains only real value, accept. If not, reject.
  3. if spreadsheet contains duplicate row names, remove duplicate rows. 
  4. if spreadsheet contains duplicate column names, remove duplicate columns. 
  5. map spreadsheet gene name to ensemble name and generates mapping files.
 
  *After removing empty rows and columns in phenotype data, check :*
  1. if phenotypic data intersects with spreadsheet on phenotype.
  2. if phenotypic data for pearson test, contains only real value or NaN.
  3. for every single drug:
    1. drops NA for the current drug.
    2. intersects header with spreadsheet. If an intersection exists, add this drug to a common list until iterate through all drugs. 
    3. checks if the common list return by step 2 is emtpy. If it's empty, reject.


### simplified_inpherno_pipeline
  *After removing empty rows and columns in user spreadsheet data, check :*
  1. if expression_sample data contains only real value, accept. If not, reject.
  2. if expression_sample data's gene name can be mapped to ensemble gene name, then generates mapping files.

  *After removing empty rows and columns in Pvalue gene phenotype data, check :*
  1. if Pvalue_gene_phenotype data contains only real value, accept. If not, reject.
  2. if Pvalue_gene_phenotype's gene name can be mapped to ensemble gene name, then generates mapping files.

  *After removing empty rows and columns in TF expression data, check :*
  1. if TFexpression data contains only real value and doesn't contain NA, accept. If not, reject.
  2. if TFexpression data's gene name can be mapped to ensemble gene name, then generates mapping files.


* * * 
## How to run this pipeline with our data
* * * 

### 1. Clone the Data_Cleanup_Pipeline Repo
```
 git clone https://github.com/KnowEnG/Data_Cleanup_Pipeline.git
```
 
### 2. Install the following (Ubuntu or Linux)
```
 apt-get install -y python3-pip
 apt-get install -y libblas-dev liblapack-dev libatlas-base-dev gfortran
 pip3 install numpy
 pip3 install pandas
 pip3 install scipy==0.19.1
 pip3 install scikit-learn==0.19.2
 apt-get install -y libfreetype6-dev libxft-dev
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
| make run_phenotype_prediction_pipeline  | phenotype prediction pipeline test                                      |
| make run_feature_prioritization_pipeline_pearson          | feature prioritization pipeline test                                      |
| make run_feature_prioritization_pipeline_t_test_binary          | feature prioritization pipeline test                                      |
| make run_feature_prioritization_pipeline_t_test_replace         | feature prioritization pipeline test                                      |
| make run_feature_prioritization_pipeline_t_test_expand          | feature prioritization pipeline test                                      |
| make run_feature_prioritization_pipeline_t_test_mixed           | feature prioritization pipeline test                                      |
| make run_feature_prioritization_pipeline_edgeR_binary           | feature prioritization pipeline test                                      |
| make run_feature_prioritization_pipeline_edgeR_replace          | feature prioritization pipeline test                                      |
| make run_feature_prioritization_pipeline_edgeR_expand           | feature prioritization pipeline test                                      |
| make run_feature_prioritization_pipeline_edgeR_mixed            | feature prioritization pipeline test                                      |
| make run_signature_analysis_pipeline          | signature analysis pipeline test                                      |
| make run_simplified_inpherno_pipeline  | simplified_inpherno_pipeline test       |

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
   export PYTHONPATH='../':$PYTHONPATH    
   ```
   
  * Run (these relative paths assume you are in the test directory with setup as described above)
   ```
  python3 -m kndatacleanup.data_cleanup -run_directory ./run_dir -run_file TEMPLATE_data_cleanup.yml
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
| correlation_measure        | t_test/pearson/edgeR* (*FP only)     | Correlation measure gene/feature prioritization pipeline  |



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

