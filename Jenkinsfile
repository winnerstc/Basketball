pipeline {
    agent any  // or { label 'hadoop-edge' } if you have a specific node

    stages {
        stage('Checkout') {
            steps {
                // Jenkins will check out the code from SCM automatically
                checkout scm
            }
        }

        // --- your Sqoop stages here (still commented out if you want) ---

        stage('Run silver Players script') {
            steps {
                sh '''
                  echo "Running silver cleaning script..."
                  spark-submit silver_players.py
                '''
            }
        }

        stage('Run silver Games script') {
            steps {
                sh '''
                  echo "Running silver cleaning script..."
                  spark-submit silver_games.py
                '''
            }
        }

        stage('Run silver Player Stats script') {
            steps {
                sh '''
                  echo "Running silver cleaning script..."
                  spark-submit silver_playerstats.py
                '''
            }
        }

        stage('Run silver Team Histories script') {
            steps {
                sh '''
                  echo "Running silver cleaning script..."
                  spark-submit silver_teamhistories.py
                '''
            }
        }

        stage('Run silver Team Statistics script') {
            steps {
                sh '''
                  echo "Running silver cleaning script..."
                  spark-submit silver_teamstatistics.py
                '''
            }
        }

        stage('Run gold script') {
            steps {
                sh '''
                  echo "Running gold script..."
                  spark-submit silver-to-gold.py
                '''
            }
        }

        stage('Run Unit Tests') {
            steps {
                sh '''#!/bin/bash
                set -e
                source ${VENV}/bin/activate
                pytest --junitxml=pytest.xml
                '''
            }
        }
    } // end stages

    post {
        success {
            echo "Build & silver cleaning succeeded."
        }
        failure {
            echo "Build FAILED – check logs."
        }
    }
}
