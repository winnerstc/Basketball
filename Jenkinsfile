pipeline {
    agent any  // or { label 'hadoop-edge' } if you have a specific node

    stages {
        stage('Checkout') {
            steps {
                // Jenkins will check out the code from SCM automatically
                checkout scm
            }
        }

        stage('Sqoop games'){
            steps {
                sh '''#!/bin/bash
                set -e

                HIVE_DB="nba_bronze"
                HIVE_TABLE="games"
                CHECK_COL="gameid"
                TARGET_DIR="/tmp/DE011025/NBA/bronze/games"

                echo "Getting last ${CHECK_COL} from Hive..."

                LAST_VALUE=$(
                ( hive -S -e "SELECT COALESCE(MAX(${CHECK_COL}),0) FROM ${HIVE_DB}.${HIVE_TABLE}" 2>/dev/null || echo 0 ) | tail -n 1
                )

                echo "Last imported ${CHECK_COL}: ${LAST_VALUE}"

                sqoop import \
                --connect jdbc:postgresql://18.134.163.221:5432/testdb \
                --username admin \
                --password admin123 \
                --driver org.postgresql.Driver \
                --query "SELECT * FROM games WHERE gameId > ${LAST_VALUE} AND \\$CONDITIONS" \
                --split-by gameid \
                --target-dir ${TARGET_DIR} \
                --fields-terminated-by ',' \
                --as-textfile \
                --num-mappers 1 \
                --delete-target-dir
                '''
            }
        }


        stage('Sqoop PlayerStatistics'){
            steps {
                sh '''
                #!/bin/bash
                set -e
                HIVE_DB="nba_bronze"
                HIVE_TABLE="player_statistics"
                CHECK_COL="gameId"
                TARGET_DIR="/tmp/DE011025/NBA/bronze/player_statistics"

                LAST_VALUE=$(
                  ( hive -e "SELECT COALESCE(MAX(${CHECK_COL}),0) FROM ${HIVE_DB}.${HIVE_TABLE}" 2>/dev/null || echo 0 ) | tail -n 1
                )

                echo "Last imported ${CHECK_COL}: ${LAST_VALUE}"

                sqoop import \
                  --connect jdbc:postgresql://18.134.163.221:5432/testdb \
                  --username admin \
                  --password admin123 \
                  --table player_statistics \
                  --target-dir ${TARGET_DIR} \
                  --fields-terminated-by ',' \
                  --as-textfile \
                  --num-mappers 1 \
                  --incremental append \
                  --check-column ${CHECK_COL} \
                  --last-value ${LAST_VALUE}
                '''
            }
        }
        
        stage('Sqoop Players'){
            steps {
                sh '''
                #!/bin/bash
                set -e
                HIVE_DB="nba_bronze"
                HIVE_TABLE="players"
                CHECK_COL="personId"
                TARGET_DIR="/tmp/DE011025/NBA/bronze/players"

                LAST_VALUE=$(
                  ( hive -e "SELECT COALESCE(MAX(${CHECK_COL}),0) FROM ${HIVE_DB}.${HIVE_TABLE}" 2>/dev/null || echo 0 ) | tail -n 1
                )

                echo "Last imported ${CHECK_COL}: ${LAST_VALUE}"

                sqoop import \
                  --connect jdbc:postgresql://18.134.163.221:5432/testdb  \
                  --username admin \
                  --password admin123 \
                  --table players \
                  --target-dir ${TARGET_DIR} \
                  --fields-terminated-by ',' \
                  --as-textfile \
                  --num-mappers 1 \
                  --incremental append \
                  --check-column ${CHECK_COL} \
                  --last-value ${LAST_VALUE}
                '''
            }
        }

        stage('Sqoop TeamHistories'){
            steps {
                sh '''
                #!/bin/bash
                set -e
                HIVE_DB="nba_bronze"
                HIVE_TABLE="team_histories"
                CHECK_COL="teamId"
                TARGET_DIR="/tmp/DE011025/NBA/bronze/team_histories"

                LAST_VALUE=$(
                  ( hive -e "SELECT COALESCE(MAX(${CHECK_COL}),0) FROM ${HIVE_DB}.${HIVE_TABLE}" 2>/dev/null || echo 0 ) | tail -n 1
                )

                echo "Last imported ${CHECK_COL}: ${LAST_VALUE}"

                sqoop import \
                  --connect jdbc:postgresql://18.134.163.221:5432/testdb  \
                  --username admin \
                  --password admin123 \
                  --table team_histories \
                  --target-dir ${TARGET_DIR} \
                  --fields-terminated-by ',' \
                  --as-textfile \
                  --num-mappers 1 \
                  --incremental append \
                  --check-column ${CHECK_COL} \
                  --last-value ${LAST_VALUE}
                '''
            }
        }

        stage('Sqoop TeamStatistics'){
            steps {
                sh '''
                #!/bin/bash
                set -e
                HIVE_DB="nba_bronze"
                HIVE_TABLE="team_statistics"
                CHECK_COL="gameId"
                TARGET_DIR="/tmp/DE011025/NBA/bronze/team_statistics"

                LAST_VALUE=$(
                  ( hive -e "SELECT COALESCE(MAX(${CHECK_COL}),0) FROM ${HIVE_DB}.${HIVE_TABLE}" 2>/dev/null || echo 0 ) | tail -n 1
                )

                echo "Last imported ${CHECK_COL}: ${LAST_VALUE}"

                sqoop import \
                  --connect jdbc:postgresql://18.134.163.221:5432/testdb \
                  --username admin \
                  --password admin123 \
                  --table team_statistics \
                  --target-dir ${TARGET_DIR} \
                  --fields-terminated-by ',' \
                  --as-textfile \
                  --num-mappers 1 \
                  --incremental append \
                  --check-column ${CHECK_COL} \
                  --last-value ${LAST_VALUE}
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

    post {
        success {
            echo "Build & silver cleaning succeeded."
        }
        failure {
            echo "Build FAILED – check logs."
        }
    }
}
