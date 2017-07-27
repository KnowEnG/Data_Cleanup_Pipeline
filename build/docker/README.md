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
### 1. Check on docker.hub to get the latest image tag: 07_26_2017 used here.

### 2. Change directory to the directory  where you want to run and start the container.
```
docker run -v \`pwd\`:/home/test/run_dir/ -it knowengdev/data_cleanup_pipeline:07_26_2017 
```
### 3. Inside the container change to the test directory.
```
cd test
```
### 4. Run set up.
```
make env_setup
```
### 5. edit and run the (data_cleanup) .yml file (use the comments to see options).
```
make run_data_cleaning
```
### If you don't "cp" your data into the volume you mounted it will disappear when you exit docker.
