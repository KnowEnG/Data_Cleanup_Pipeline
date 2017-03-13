# Building The Data Cleanup Pipeline Docker Image
The Dockefile in this directory contains all the commands, in order, needed to build the **Data Cleanup Pipeline** docker image.


* Run the "make" command to build the **Data Cleanup Pipeline** docker image (output: docker image called "data_cleanup_pipeline" and a tag with today's date and time):
```
    make build_docker_image
```

* Login to docker hub. When prompted, enter your password and press enter:
```
    make login_to_dockerhub username=(enter your docker login here) email=(enter your email here)
```

* Upload your image to docker hub:
```
    make push_to_dockerhub
```

* * * 
## How to run this docker image
* * * 

1 Change directory to the directory  where you want to run.

2 docker run -v \`pwd\`:/home/test/run_dir/ -it knowengdev/data_cleanup_pipeline:10_20_2016 

3 cd test

4 make env_setup

5 edit the .yml file (use the comments to see options)

* Check on docker.hub to get the latest image. 

* If you don't "cp" your data into the volume you mounted it will disappear when you exit docker.

***
## Detailed logic for each pipeline
***
* geneset_characterization_pipeline
  1. checks if the user spreadsheet contains NA value. If so, reject it.
  2. checks if the user spreadsheet only contains value 0 and 1. If not, rejects it.
  3. checks if the index in user spreasheet contains NA value. If so, removes the row.
  4. checks if the user spreadsheet contains duplicate column name. If so, removes the duplicates.
  5. checks if the user spreadsheet contains duplicate row name. If so, removes the duplicates.
  6. checks if the gene name in user spreadsheet can be mapped to ensemble gene name. If no one could be mapped, rejects the spreadshset.
  
* samples_clustering_pipeline
  1. checks if the user spreadsheet contains NA value. If so, reject it.
  2. checks if the user spreadsheet only contains real value. If not, rejects it.
  3. convert all values within user spreadsheet to be absolute value.
  4. checks if the index in user spreasheet contains NA value. If so, removes the row.
  5. checks if the user spreadsheet contains duplicate column name. If so, removes the duplicates.
  6. checks if the user spreadsheet contains duplicate row name. If so, removes the duplicates.
  7. checks if the gene name in user spreadsheet can be mapped to ensemble gene name. If no one could be mapped, rejects the spreadshset.


* gene_prioritization_pipeline
  1. checks if either user spreadsheet or phenotype data is empty. If so, rejects it.
  2. removes any column if user spreadsheet contains NA value. Rejects the processed user spreadsheet if it becomes empty.
  3. checks if the user spreadsheet only contains real value. If not, rejects it.
  4. correlation measure specific check:
    1. for t_test, checks if the phenotype contains only value 0 and 1.
    2. for pearson test, checks if the phenotype is real value.
  5. checks if the index in user spreadsheet contains NA value. If so, removes the row.
  6. checks if the user spreadsheet contains duplicate column name. If so, removes the duplicates.
  7. checks if the user spreadsheet contains duplicate row name. If so, removes the duplicates.
  8. checks if the gene name in user spreadsheet can be mapped to ensemble gene name. If no one could be mapped, rejects the spreadshset.