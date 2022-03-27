# Project

I chose the Million Song dataset. The processign will be done in batch. We do only a single batch, but the code would work on any number of batch.


## High level Pipeline

![pipeline](/resources/pipeline.png)

What we use:
- Google Cloud
- Terraform
- Docker / Docker-compose
- Airflow
- Pyspark
- Google Data Studio

## Requirements
- Google Cloud account with a slight bit of credit
- Either a computing instance on GCP or you can run this repository locally too if your PC can handle 2GB of storage, has docker, docker-compose and at least 4 cores, and 8GB of RAM

## Explanation
This project treats the data of the [million song dataset](http://millionsongdataset.com/). We only use a small subsample, as the full dataset is 300GB.

So our project could be leverage to understand and analyse song, mostly trends throughout the years and genre linked to it.
It also contains technical data such as tempo, keys, beats per measure, etc.

Our code could scale to the big dataset with proper resources scaling.

# How To Run

First let's create the project and Resources:

### Setup for Accessing GCP

1. Create a project under Project  > New Project
    * Get the ID of your project => Save it for later
    * Go to IAM & Admin > Service Accounts
    * Create a Service Account
    * Call it whatever name you want, and add description as `service account`.
    * Click `Create an Continue`
    * Give the role `Viewer` for now.
    * Click on the user.
    * Go to `Keys`
    * Create new key as json, it download the key automatically.
    * Copy the key file to the `.google` folder of the project (I let an example one to show where it should be). Call it `google_crendentials.json`


![project](/resources/project.JPG)


2. [IAM Roles](https://cloud.google.com/storage/docs/access-control/iam-roles) for Service account:
   * Go to the *IAM* section of *IAM & Admin* https://console.cloud.google.com/iam-admin/iam
   * Click the *Edit principal* icon for your service account.
   * Add these roles in addition to *Viewer* : **Storage Admin** + **Storage Object Admin** + **BigQuery Admin**


![role](/resources/Viewer.JPG)


3. Enable these APIs for your project:
   * https://console.cloud.google.com/apis/library/iam.googleapis.com
   * https://console.cloud.google.com/apis/library/iamcredentials.googleapis.com
  
4. Edit the terraform Variable
   * variable "project" default = "YOUR PROJECT ID HERE"

5. Run terraform
    * For making the ressources : `docker-compose run --rm terraform init`
    *  `docker-compose run --rm terraform apply` -> enter yes
    * Later, when you are done with the project, run `docker-compose run --rm terraform destroy` to REMOVE the resources

6. Add all project related variable to env files
   * After you created the infrastrucutre with Terraform, check all the project id / name and replace the variable in the `.env` file with 
   * ```
         GOOGLE_PROJECT_ID=PROJECT_ID
         GOOGLE_BIGQUERY_DATASET=million_song (This is probably the same but just in case check https://console.cloud.google.com/bigquery)
         GOOGLE_CLOUD_STORAGE_ID=BUCKET_NAME from https://console.cloud.google.com/storage/browser
         
      ````

   **PLEASE ALSO COPY THOSE 3 LINES in the .env file inside the `airflow` folder. Docker cannot use parent directory as context**

### Run Airflow and get the data
1. Go to https://console.cloud.google.com/storage/browser and check the data lake has been created. Keep the name to be able to use it later.
2. Go in the `airflow` directory of the project
3. In the folder `.google` make sure `google_credentials.json` is there from previous terraform step.
4. Run docker-compose build
5. Run docker-compose up airflow-init
6. Run docker-compose up
7. Go on `localhost:8080` if you are running the code locally, else forward port 8080 to your machine (if using a cloud compute engine). Passowrd and user are  `airflow`
8. Enable the worflow if not enabled, and wait for 5-10 minutes for it to be finished
9. Verify that https://console.cloud.google.com/storage/browser contains a raw folder with `millionsongsubset.parquet` file.
10. You can kill the docker for airflow at this point


### Transform the data with pyspark
1. Go back to the root of the folder.
2. You can either run the notebook, or run the script to transform the data and load them in BigQuery.
   1. Notebook: run `docker-compose up --build pyspark-notebook`
      * Go to  http://localhost:8899/notebooks/Spark%20Transformation%20notebook.ipynb
      * Run cell by cell
   2. Script: `docker-compose up --build pyspark-script`. Wait for execution.
   3. Check the resulting tables in bigquery: https://console.cloud.google.com/bigquery

Some hacky stuff hapenned in spark transformation. For example, i save the DF first in a normal table, to then get the schema, create with API the partitioned table, and then insert in this new partitioned table.

# The dashboard

Iframe (if if does not work, click on [this link](https://datastudio.google.com/reporting/32e47161-22b5-46a0-9884-bff6410ec528) OR look at dahsboard.pdf in resources folder)

<iframe width="600" height="450" src="https://datastudio.google.com/embed/reporting/32e47161-22b5-46a0-9884-bff6410ec528/page/RG2oC" frameborder="0" style="border:0" allowfullscreen></iframe>