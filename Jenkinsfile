pipeline {
  agent none

  parameters {
    string(name: 'GIT_COMMIT', defaultValue: 'master', description: 'Commit SHA or origin branch to deploy')
  }

  stages {
    stage('release: dev') {
      steps {
        ci_pipeline("dev", params.GIT_COMMIT)
      }
    }


    stage('release: staging') {
      when {
          expression {
              milestone label: "release-staging"
              input message: 'Deploy to staging? Note: migrations are not automated. Have you run any required migrations?'
              return true
          }
          beforeAgent true
      }

      steps {
        ci_pipeline("staging", params.GIT_COMMIT)
      }
    }


    stage('release: prod') {
      when {
          expression {
              milestone label: "release-prod"
              input message: 'Deploy to production? Note: migrations are not automated. Have you run any required migrations?'
              return true
          }
          beforeAgent true
      }

      steps {
        ci_pipeline("prod", params.GIT_COMMIT)
      }
    }
  }
}

void ci_pipeline(env, version) {
  lock("data-store-service-ci-pipeline-${env}") {
    build job: "ci-pipeline", parameters: [
        string(name: "Team", value: "data-workspace-apps"),
        string(name: "Project", value: "data-store-service"),
        string(name: "Environment", value: env),
        string(name: "Version", value: version)
    ]
  }
}
