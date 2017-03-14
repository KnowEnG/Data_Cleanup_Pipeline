# KnowEnG's Data Cleanup Pipeline
 This is the Knowledge Engine for Genomics (KnowEnG), an NIH BD2K Center of Excellence, Data Cleanup Pipeline.

This pipeline **cleanup** the data of a given spreadsheet. Given a spreadsheet this pipeline maps gene-label row names to Ensemble-label row names and checks data formats. It will go through the following steps

## Detailed cleanup logic for each pipeline
* geneset_characterization_pipeline
  1. checks if the user spreadsheet is empty. If so, rejects it.
  2. checks if the user spreadsheet contains NA value. If so, rejects it.
  3. checks if the user spreadsheet only contains value 0 and 1. If not, rejects it.
  4. checks if the index in user spreasheet contains NA value. If so, removes the row.
  5. checks if the user spreadsheet contains duplicate column name. If so, removes the duplicates.
  6. checks if the user spreadsheet contains duplicate row name. If so, removes the duplicates.
  7. checks if the gene name in user spreadsheet can be mapped to ensemble gene name. If no one could be mapped, rejects the spreadshset.
  
* samples_clustering_pipeline
  1. checks if the user spreadsheet is empty. If so, rejects it.
  2. checks if the user spreadsheet contains NA value. If so, rejects it.
  3. checks if the user spreadsheet only contains real value. If not, rejects it.
  4. convert all values within user spreadsheet to be absolute value.
  5. checks if the index in user spreasheet contains NA value. If so, removes the row.
  6. checks if the user spreadsheet contains duplicate column name. If so, removes the duplicates.
  7. checks if the user spreadsheet contains duplicate row name. If so, removes the duplicates.
  8. checks if the gene name in user spreadsheet can be mapped to ensemble gene name. If no one could be mapped, rejects the spreadshset.


* gene_prioritization_pipeline
  1. checks if the user spreadsheet is empty. If so, rejects it.
  2. checks if either user spreadsheet or phenotype data is empty. If so, rejects it.
  3. removes any column if user spreadsheet contains NA value. Rejects the processed user spreadsheet if it becomes empty.
  4. checks if the user spreadsheet only contains real value. If not, rejects it.
  5. correlation measure specific check:
    1. for t_test, checks if the phenotype contains only value 0 and 1.
    2. for pearson test, checks if the phenotype is real value.
  6. checks if the index in user spreadsheet contains NA value. If so, removes the row.
  7. checks if the user spreadsheet contains duplicate column name. If so, removes the duplicates.
  8. checks if the user spreadsheet contains duplicate row name. If so, removes the duplicates.
  9. checks if the gene name in user spreadsheet can be mapped to ensemble gene name. If no one could be mapped, rejects the spreadshset.
  
  
* * * 
## How to run this pipeline with Our data
* * * 
###1. Get Access to KnowEnG-Research Repository:
Email omarsobh@illinois.edu infrastructure team (IST) lead to:

* __Access__ KnowEnG-Research github repo

###2. Clone the Data_Cleanup_Pipeline Repo
```
 git clone https://github.com/KnowEnG-Research/Data_Cleanup_Pipeline.git
```
 
###3. Install the following (Ubuntu or Linux)
```
 apt-get install -y python3-pip
 apt-get install -y libblas-dev liblapack-dev libatlas-base-dev gfortran
 pip3 install numpy==1.11.1
 pip3 install pandas==0.18.1
 pip3 install scipy==0.18.0
 pip3 install scikit-learn==0.17.1
 apt-get install -y libfreetype6-dev libxft-dev
 pip3 install matplotlib==1.4.2
 pip3 install pyyaml
 pip3 install knpackage
 pip3 install redis
```

###4. Change directory to Data_Cleanup_Pipeline

```
cd Data_Cleanup_Pipeline 
```

###5. Change directory to test

```
cd test
```
 
###6. Create a local directory "run_dir" and place all the run files in it
```
make env_setup
```

###7. Use following "make" commands to run a data cleanup pipeline
```
make run_data_cleaning
```


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

### * Run the Samples Clustering Pipeline:

  * Update PYTHONPATH enviroment variable
   ``` 
   export PYTHONPATH='../src':$PYTHONPATH    
   ```
   
  * Run
   ```
  python3 ../src/data_cleanup.py -run_directory ./ -run_file TEMPLATE_data_cleanup.yml
   ```
