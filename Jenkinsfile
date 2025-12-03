pipeline {
    agent any 
     tools {
        jdk 'JDK11'   // must match the Name you set
    }
     // or { label 'hadoop-edge' } if you have a specific node
      environment {
            VENV = 'unit_testing_bd'
        }
    stages {
        stage('Checkout') {
            steps {
                // Jenkins will check out the code from SCM automatically
                checkout scm
            }
        }


        stage('Sqoop Incremental Using HDFS for GAMES') {
            steps {
                sh '''#!/bin/bash
                set -e

                echo "============================"
                echo "  READ TIMESTAMP FROM HDFS  "
                echo "============================"

                LAST_VALUE=$(hdfs dfs -cat /tmp/DE011025/NBA/bronze/games/part* \
                    | cut -d',' -f2 \
                    | sort \
                    | tail -n 1)

                echo "LAST VALUE FROM BRONZE = ${LAST_VALUE}"

                echo "============================"
                echo "     RUN SQOOP IMPORT       "
                echo "============================"

                sqoop import \
                --connect jdbc:postgresql://18.134.163.221:5432/testdb \
                --username admin \
                --password admin123 \
                --driver org.postgresql.Driver \
                --query "SELECT * FROM games WHERE \\\"gameDateTimeEst\\\" > '${LAST_VALUE}' AND \\$CONDITIONS" \
                --target-dir /tmp/DE011025/NBA/bronze/games \
                --fields-terminated-by ',' \
                --as-textfile \
                --num-mappers 1 \
                --append

                echo "============================"
                echo "   SQOOP INCREMENTAL DONE   "
                echo "============================"
                '''
            }
        }

        stage('Sqoop Incremental Using HDFS for PLAYER_STATISTICS') {
            steps {
                sh '''#!/bin/bash
                set -e

                echo "============================"
                echo "  READ TIMESTAMP FROM HDFS  "
                echo "============================"

                LAST_VALUE=$(hdfs dfs -cat /tmp/DE011025/NBA/bronze/player_statistics/part* \
                    | cut -d',' -f2 \
                    | sort \
                    | tail -n 1)

                echo "LAST VALUE FROM BRONZE = ${LAST_VALUE}"

                echo "============================"
                echo "     RUN SQOOP IMPORT       "
                echo "============================"

                sqoop import \
                --connect jdbc:postgresql://18.134.163.221:5432/testdb \
                --username admin \
                --password admin123 \
                --driver org.postgresql.Driver \
                --query "SELECT * FROM player_statistics WHERE \\\"gameDateTimeEst\\\" > '${LAST_VALUE}' AND \\$CONDITIONS" \
                --target-dir /tmp/DE011025/NBA/bronze/player_statistics \
                --fields-terminated-by ',' \
                --as-textfile \
                --num-mappers 1 \
                --append

                echo "============================"
                echo "   SQOOP INCREMENTAL DONE   "
                echo "============================"
                '''
            }
        }
        stage('Sqoop Incremental Using HDFS for TEAM_STATISTICS') {
            steps {
                sh '''#!/bin/bash
                set -e

                echo "============================"
                echo "  READ TIMESTAMP FROM HDFS  "
                echo "============================"

                LAST_VALUE=$(hdfs dfs -cat /tmp/DE011025/NBA/bronze/team_statistics/part* \
                    | cut -d',' -f2 \
                    | sort \
                    | tail -n 1)

                echo "LAST VALUE FROM BRONZE = ${LAST_VALUE}"

                echo "============================"
                echo "     RUN SQOOP IMPORT       "
                echo "============================"

                sqoop import \
                --connect jdbc:postgresql://18.134.163.221:5432/testdb \
                --username admin \
                --password admin123 \
                --driver org.postgresql.Driver \
                --query "SELECT * FROM team_statistics WHERE \\\"gameDateTimeEst\\\" > '${LAST_VALUE}' AND \\$CONDITIONS" \
                --target-dir /tmp/DE011025/NBA/bronze/team_statistics \
                --fields-terminated-by ',' \
                --as-textfile \
                --num-mappers 1 \
                --append

                echo "============================"
                echo "   SQOOP INCREMENTAL DONE   "
                echo "============================"
                '''
            }
        }
        stage('Setup Python Environment') {
            steps {
                sh '''
                echo %JAVA_HOME%
                python3 -m venv ${VENV}
                source ${VENV}/bin/activate
                pip install --upgrade pip
                pip install -r requirements.txt
                '''
            }
        }
        

        stage('Run silver Silver Players script') {
            steps {
                sh '''
                  echo "Running silver cleaning script..."
                  spark-submit silver_players.py
                '''
            }
        }

        stage('Run silver Silver Games script') {
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
    }
    stage('Run Unit Tests') {
        steps {
            sh '''
            source ${VENV}/bin/activate
            pytest --junitxml=pytest.xml
            '''
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
