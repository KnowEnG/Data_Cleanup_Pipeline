# Verification directory files are benchmark result files
The Makefile in the test directory contains the targes, needed to build the **Data Cleanup Pipeline** benchmarks.

* Follow the instructions on the **Data Cleanup Pipeline** landing page to set up the environmet:
```
    cd Data_Cleanup_Pipeline/test
    make env_setup
```
### 1. Run the small data test
```
    make run_data_cleaning_small
```

* Compare the results with verification file starts with prefix: **TEST_1_gene_expression**
**Note that the order of columns and rows may change.**


### 2. Run the large data test
```
    make run_data_cleaning
```
* Compare the results with verification file starts with prefix: **GDSC_Expression_ensembl**
**Note that the order of columns and rows may change.**

### 3. Run pasted gene list test
```
    make run_pasted_gene_list
```
* Compare the results with verification file starts with prefix: **TEST_pasted_gene_list**
**Note that the order of columns and rows may change.**
