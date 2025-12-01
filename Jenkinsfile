pipeline {
    agent any  // or { label 'hadoop-edge' } if you have a specific node

    stages {
        stage('Checkout') {
            steps {
                // Jenkins will check out the code from SCM automatically
                checkout scm
            }
        }

        stage('Run silver script') {
            steps {
                sh '''
                  echo "Running silver cleaning script..."
                  # Replace this with whatever you run manually:
                  # spark-submit /path/to/silver_players.py
                  spark-submit silver_players.py
                '''
            }
        }
        stage('Run silver script') {
            steps {
                sh '''
                  echo "Running silver cleaning script..."
                  # Replace this with whatever you run manually:
                  # spark-submit /path/to/silver_players.py
                  spark-submit silver_games.py
                '''
            }
        }
        stage('Run silver script') {
            steps {
                sh '''
                  echo "Running silver cleaning script..."
                  # Replace this with whatever you run manually:
                  # spark-submit /path/to/silver_players.py
                  spark-submit silver_playerstats.py
                '''
            }
        }
        stage('Run silver script') {
            steps {
                sh '''
                  echo "Running silver cleaning script..."
                  # Replace this with whatever you run manually:
                  # spark-submit /path/to/silver_players.py
                  spark-submit silver_teamhistories.py
                '''
            }
        }
        stage('Run silver script') {
            steps {
                sh '''
                  echo "Running silver cleaning script..."
                  # Replace this with whatever you run manually:
                  # spark-submit /path/to/silver_players.py
                  spark-submit silver_teamstatistics.py
                '''
            }
        }
        stage('Run gold script') {
            steps {
                sh '''
                  echo "Running silver cleaning script..."
                  # Replace this with whatever you run manually:
                  # spark-submit /path/to/silver_players.py
                  spark-submit silver-to-gold.py
                '''
            }
        }
    }

    post {
        success {
            echo "Build & silver cleaning succeeded."
        }
        failure {
            echo "Build FAILED – check logs."
        }
    }
}
