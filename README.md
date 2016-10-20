# KnowEnG's Data Cleanup Pipeline
 This is the Knowledge Engine for Genomics (KnowEnG), an NIH BD2K Center of Excellence, Samples Clustering Pipeline.

This pipeline **cleanup** the data of a given spreadsheet. Given a spreadsheet this pipeline maps gene-label row names to Ensemble-label row names and checks data formats. It will go through the following steps

There are four clustering methods that one can choose from:


| **Options**                                      | **Method**                           | **Parameters** |
| ------------------------------------------------ | -------------------------------------| -------------- |
| Check values                                       | check_value_set                    | user_spreadsheet_dataframe            |
| Check duplicates                             | check_duplicate_gene_name              | user_spreadsheet_dataframe         |
| Map gene name to ensembl name           | check_ensemble_gene_name                   | user_spreadsheet_dataframe        |
| Create final mapped spreadsheet and mapping table | check_ensemble_gene_name | user_spreadsheet_dataframe, config     |

