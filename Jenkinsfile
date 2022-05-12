pipeline {
  agent any
  stages {
    stage('Build Image') {
      steps {
        sh 'docker build -t neewee/bodhee:Excel_ftp_Portescap${APP_VERSION} .'
      }
    }

    stage('docker login') {
      steps {
        sh 'docker login -u soorkod -p hudY4tI8x1TSufKkXyKO%'
      }
    }

    stage('docker push') {
      steps {
        sh 'docker push neewee/bodhee:Excel_ftp_Portescap${APP_VERSION}'
      }
    }

    stage('remove image') {
      steps {
        sh 'docker rmi neewee/bodhee:Excel_ftp_Portescap${APP_VERSION}'
      }
    }

  }
  environment {
    APP_VERSION = '1.0.0'
  }
}