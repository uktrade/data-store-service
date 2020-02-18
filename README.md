# Data store service
The data store service aims to store data from external sources that changes infrequently.

This data is then pushed to data workspaces for analysts to use.

## Installation
The backend is built in Python using the Flask framework. Authentication implemented using Hawk and OAUTH2(SSO) authentication. The majority of the functionality is through API calls but a light front end is provided for documentation and dashboarding. This front end uses React and d3 and uses the webpack javascript module bundler. 

### Docker installation
1. Copy `.envs/docker.env` to `.env`
2. `docker-compose build`
3. `docker-compose up`
4. Go to http://localhost:5050/healthcheck

### Docker run tests
1. `docker exec -it data_dss_web_1 make run_tests`


## Example dataset
ONS Postcode directory: http://geoportal.statistics.gov.uk/datasets/ons-postcode-directory-november-2019

## Creating an S3 bucket
An s3 bucket can be created using the Cloud Foundry command line tools, e.g.

1. `cf create-service aws-s3-bucket default data-store-service-s3`
2. `cf bind-service data-store-service data-store-service-s3`
3. `cf restage data-store-service`
4. To get access outside of PaaS you will need to create a service key, `cf create-service-key data-store-service-s3 s3-key -c '{"allow_external_access": true}'`
5. To get the credentials for s3 use then command, `cf service-key data-store-service-s3 s3-key`

Reference: 
https://docs.cloud.service.gov.uk/deploying_services/s3/#connect-to-an-s3-bucket-from-your-app
