# KnowEnG's Data Cleanup Pipeline
 This is the Knowledge Engine for Genomics (KnowEnG), an NIH BD2K Center of Excellence, Data Cleanup Pipeline.

This pipeline **cleanup** the data of a given spreadsheet. Given a spreadsheet this pipeline maps gene-label row names to Ensemble-label row names and checks data formats. It will go through the following steps

## Detailed cleanup logic for each pipeline
* geneset_characterization_pipeline
  1. removes empty rows in user spreasheet and checks if the user spreadsheet is empty. If so, rejects it.
  2. checks if the user spreadsheet contains NA value. If so, rejects it.
  3. checks if the user spreadsheet only contains value 0 and 1. If not, rejects it.
  4. checks if the gene name in user spreasheet contains NA value. If so, removes the row.
  5. checks if the user spreadsheet contains duplicate column name. If so, removes the duplicates.
  6. checks if the user spreadsheet contains duplicate row name. If so, removes the duplicates.
  7. checks if the gene name in user spreadsheet can be mapped to ensemble gene name. If no one could be mapped, rejects the spreadshset.
  
* samples_clustering_pipeline
  1. removes empty rows in user spreasheet and checks if the user spreadsheet is empty. If so, rejects it.
  2. checks if the user spreadsheet contains NA value. If so, rejects it.
  3. checks if the user spreadsheet only contains real value. If not, rejects it.
  4. convert all values within user spreadsheet to be absolute value.
  5. checks if the gene name in user spreasheet contains NA value. If so, removes the row.
  6. checks if the user spreadsheet contains duplicate column name. If so, removes the duplicates.
  7. checks if the user spreadsheet contains duplicate row name. If so, removes the duplicates.
  8. checks if the gene name in user spreadsheet can be mapped to ensemble gene name. If no one could be mapped, rejects the spreadshset.
  9. checks if there is intersected ensembl name between user spreadsheet and gene-gene network data. If no intersection, rejects the spreadsheet.

  If the user provides with the phenotype data:
  1. checks if the phenotype data is empty. If so, rejects it.
  2. checks if the phenotype contains duplicate column name. If so, removes the duplicates.
  3. checks if the phenotype contains duplicate row name. If so, removes the duplicates.
  4. checks if the intersection between user spreadsheet and phenotype is empty. If so, rejects it.

  If the user provides with the phenotype data:
  1. checks if the network data is empty. If so, rejects it.
  2. checks if there is no intersection between two columns in network data. If so, rejects it. 

* gene_prioritization_pipeline
  1. removes empty rows in user spreasheet and checks if the user spreadsheet is empty. If so, rejects it.
  2. checks if either user spreadsheet or phenotype data is empty. If so, rejects it.
  3. removes any column that contains NA value in user spreadsheet. Rejects the processed user spreadsheet if it becomes empty.
  4. checks if the user spreadsheet only contains real value. If not, rejects it.
  5. phenotype data check:
    1. for every single drug, drops NA in phenotype data and intersects its header with the header spreadsheet to check 
    if there is common columns (>=2) left. If not, removes this drug from phenotype data.
    2. for t_test, checks if the phenotype contains only value 0, 1 or NAN.
    3. for pearson test, checks if the phenotype contains only real value or NAN.
  6. checks if the gene name in user spreadsheet contains NA value. If so, removes the row.
  7. checks if the user spreadsheet contains duplicate column name. If so, removes the duplicates.
  8. checks if the user spreadsheet contains duplicate row name. If so, removes the duplicates.
  9. checks if the gene name in user spreadsheet can be mapped to ensemble gene name. If no one could be mapped, rejects the spreadshset.
  
* pasted_gene_list
  1. removes NA from input genes dataframe.
  2. casts index of input genes dataframe to string type
  3. if input genes dataframe is empty, the program exits. If input genes dataframe is not empty, checks the intersection 
  between universal genes list and input genes dataframe. Returns the universal genes list with intersected genes set to value 1 and not intersected genes to 0.
  
  
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
 pip3 install scipy==0.18.0
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
   
  * Run
   ```
  python3 ../src/data_cleanup.py -run_directory ./ -run_file TEMPLATE_data_cleanup.yml
   ```

* * * 
## Description of "run_parameters" file
* * * 

| **Key**                   | **Value** | **Comments** |
| ------------------------- | --------- | ------------ |
| pipeline_type                    | **gene_priorization_pipeline**, **samples_clustering_pipeline**, **geneset_characterization_pipeline**  | Choose pipeline cleaning type |
| spreadsheet_name_full_path | directory+spreadsheet_name |  Path and file name of user supplied gene sets |
| phenotype_full_path | directory+phenotype_data_name | Path and file name of user supplied phenotype data |
| gg_network_name_full_path | directory+gg_network_name | Path and file name of user supplied gene-gene network data |
| results_directory | directory | Directory to save the output files |
| redis_credential| host, password and port | Credential to access gene names lookup|
| taxonid| 9606 | Taxon id of the genes |
| source_hint| ' ' | Hint for lookup ensembl names |
| correlation_measure| t_test/pearson | Correlation measure to run gene_prioritization_pipeline |


spreadsheet_name_full_path = TEST_1_gene_expression.tsv
phenotype_full_path = TEST_1_phenotype.tsv

* * * 
## Description of Output files saved in results directory
* * * 

* Output files

**input_file_name_ETL.tsv**.
Input file after Extract Transform Load (cleaning)

**input_file_name_MAP.tsv**.

| (translated gene) | (input gene name) |
 | :--------------------: |:--------------------:|
 | ENS00000012345 | abc_def_er|
 |...|...|
 | ENS00000054321 | def_org_ifi |


**input_file_name_UNMAPPED.tsv**.

| (input gene name) | (unmapped-none) |
 | :--------------------: |:--------------------:|
 | abcd_iffe | unmapped-none|
 |...|...|
 | abdcefg_hijk | unmapped-none |

