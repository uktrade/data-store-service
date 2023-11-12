# Data Store Service

The Data Store Service stores data that changes infrequently. It takes data from

- A variety of external sources that expose data via HTTP
- CSV upload through a web interface

and exposes it via a HAWK authenticated API that is used by

- A data-flow pipeline, DSSGenericPipeline, that ingests the data into the Data Workspace datasets database
- External clients


## External sources into the Data Workspace datasets database

Triggered by a job in Jenkins every night, data is pulled from external sources into the Data Store Service, and via data-flow is ingested into the Data Workspace datasets database. The following sequence diagram shows the high level steps involved for each source.

```mermaid
sequenceDiagram
    participant Jenkins
    participant Source
    participant DSS task
    participant DSS S3
    participant DSS DB
    participant DSS API

    Jenkins ->> DSS task: cf run-task
    activate DSS task

    DSS task ->> Source: Fetch list of all files
    activate Source
    Source -->> DSS task: list of all files
    DSS task ->> DSS S3: Fetch list of all files
    activate DSS S3
    DSS S3 -->> DSS task: list of all files
    loop For each file not in DSS S3
      DSS task ->> Source: Fetch file contents
      Source -->> DSS task: File contents
      deactivate Source
      DSS task ->> DSS S3: PUT file contents
    end

    DSS task ->> DSS DB: SELECT processed files <br>from operations.datafile_registry
    activate DSS DB
    DSS DB -->> DSS task: list of processed files

    loop For each unprocessed file
      DSS task ->> DSS S3: Fetch file contents
      DSS S3 -->> DSS task: File contents
      deactivate DSS S3
      DSS task ->> DSS DB: INSERT file contents
      DSS task ->> DSS DB: INSERT file into operations.datafile_registry
    end

    DSS task ->> data-flow: trigger DSSGenericPipeline pipeline
    deactivate DSS task
    activate data-flow

    loop For each page of data
       data-flow ->> DSS API: fetch page of data
       activate DSS API
       DSS API ->> DSS DB: fetch page of data
       DSS DB -->> DSS API: page of data
       deactivate DSS DB
       DSS API -->> data-flow: page of data
       deactivate DSS API
       data-flow ->> data-flow S3: save page
       activate data-flow S3
    end

    loop For each page of data
       data-flow ->> data-flow S3: fetch page of data
       data-flow S3 -->> data-flow: page of data
       deactivate data-flow S3
       data-flow ->> datasets db: INSERT page
       deactivate data-flow
    end

    Note over data-flow, data-flow S3: data-flow has more<br>components than this - <br>this is a simplified view
```

## Architecture

The backend is built in Python using the Flask framework. Authentication implemented using Hawk and OAUTH2(SSO) authentication. The majority of the functionality is through API calls but a light front end is provided for documentation and dashboarding. This front end uses React and d3 and uses the webpack javascript module bundler. 

## Running locally

To run the Data Store Service locally

1. Clone this repository

   ```shell
   git clone git@github.com:uktrade/data-store-service.git
   cd data-store-service
   ```

2. Copy `.envs/docker.env` to `.env`

   ```shell
   cp .envs/docker.env .env
   ```

3. Start the Data Store Service

   ```shell
   docker compose up --build
   ```

4. Go to http://localhost:5050/healthcheck

## Running tests

Once the Data Store Service is running locally, to run all the tests run

```shell
docker exec -it data-dss_web-1 make run_tests
```

or to run the tests for a specific directory, run

```shell
docker exec -it data-dss_web-1 make run_tests TEST=<tests/test_directory>
```

## Config

### When using docker-compose
Place environment variables in .env file.

### When using host machine
Config variables can be specified in a few ways and are loaded using the following order of priority:

1. Look for variable in existing System environment variables
2. If not found in step 1, look for variable in `.env` (this only works if USE_DOTENV is set to 1, see .envs/sample.env for an example file)
3. If not found in step 2, look for variable in `local_testing.yml` (this only works if TESTING is set to 1)
4. If not found in step 3, look for variable in `local.yml` (this only works if TESTING is set to 0)
5. If not found in step 4, look for variable in `default.yml`


## Example dataset
ONS Postcode directory: http://geoportal.statistics.gov.uk/datasets/ons-postcode-directory-november-2019


## Running database migrations

Database migrations currently are not run automatically on deployment because there is concern that they do not work well for the pipeline tables. Until this is resolved, migrations need to be run manually and consideration needs to be given to how this will affect deploying structural database changes.

1) Deploy the migrations to the relevant environment.
2) `cf v3-ssh data-store-service-<env>` - SSH into the instance
3) `/tmp/lifecycle/shell` - active the app environment
4) `./manage.py db upgrade` - run the migrations
