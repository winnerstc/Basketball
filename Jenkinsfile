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

//         stage('Sqoop games'){
//         steps {
//             sh '''#!/bin/bash
//             set -e

//             HIVE_DB="nba_bronze"
//             HIVE_TABLE="games"
//             CHECK_COL="gamedatetimeest"             # HIVE column, lowercase
//             TARGET_DIR="/tmp/DE011025/NBA/bronze/games"

//             echo "Getting last ${CHECK_COL} from Hive..."

//             # Run Hive, disable header, grab the last line, strip whitespace
//             LAST_VALUE=$(hive -e "SELECT COALESCE(MAX(gamedatetimeest),0) FROM nba_bronze.games" 2>/dev/null || echo 0  | tail -n 1)

//             echo "Raw LAST_VALUE from Hive: '${LAST_VALUE}'"

//             if [ -z "${LAST_VALUE}" ]; then
//             echo "LAST_VALUE is empty, defaulting to 0"
//             LAST_VALUE=0
//             fi

//             echo "Final LAST_VALUE used: ${LAST_VALUE}"

//             sqoop import \
//             --connect jdbc:postgresql://18.134.163.221:5432/testdb \
//             --username admin \
//             --password admin123 \
//             --driver org.postgresql.Driver \
//             --query "SELECT * FROM games WHERE \\\"gameDateTimeEst\\\" > '${LAST_VALUE}' AND \\$CONDITIONS" \
//             --split-by "\"gameId\"" \
//             --target-dir ${TARGET_DIR} \
//             --fields-terminated-by ',' \
//             --as-textfile \
//             --num-mappers 1 \
//             --delete-target-dir
//             '''
        
//             }

//         }

        stage('Sqoop Incremental Using HDFS') {
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


//         stage('Sqoop PlayerStatistics'){
//         steps {
//             sh '''#!/bin/bash
//             set -e

//             HIVE_DB="nba_bronze"
//             HIVE_TABLE="player_statistics"
//             CHECK_COL="gameid"
//             TARGET_DIR="/tmp/DE011025/NBA/bronze/player_statistics"

//             echo "Getting last ${CHECK_COL} from Hive..."

//             LAST_VALUE=$(
//             ( hive -e "SELECT COALESCE(MAX(gamedatetimeest),0) FROM nba_bronze.player_statistics" 2>/dev/null || echo 0  | tail -n 1)
//             )

//             echo "Last imported ${CHECK_COL}: ${LAST_VALUE}"

//             sqoop import \
//             --connect jdbc:postgresql://18.134.163.221:5432/testdb \
//             --username admin \
//             --password admin123 \
//             --driver org.postgresql.Driver \
//             --query "SELECT * FROM player_statistics WHERE \\\"gameDateTimeEst\\\" > '${LAST_VALUE}' AND \\$CONDITIONS" \
//             --split-by "\"gameId\"" \
//             --target-dir ${TARGET_DIR} \
//             --fields-terminated-by ',' \
//             --as-textfile \
//             --num-mappers 1 \
//             --delete-target-dir
//             '''
//         }
//     }

        
//         stage('Sqoop Players'){
//         steps {
//             sh '''#!/bin/bash
//             set -e

//             HIVE_DB="nba_bronze"
//             HIVE_TABLE="players"
//             CHECK_COL="personid"
//             TARGET_DIR="/tmp/DE011025/NBA/bronze/players"

//             echo "Getting last ${CHECK_COL} from Hive..."

//             LAST_VALUE=$(
//             ( hive -e "SELECT COALESCE(MAX(personid),0) FROM nba_bronze.players" 2>/dev/null || echo 0  | tail -n 1)
//             )

//             echo "Last imported ${CHECK_COL}: ${LAST_VALUE}"

//             sqoop import \
//             --connect jdbc:postgresql://18.134.163.221:5432/testdb  \
//             --username admin \
//             --password admin123 \
//             --driver org.postgresql.Driver \
//             --query "SELECT * FROM players WHERE \\\"personId\\\" > ${LAST_VALUE} AND \\$CONDITIONS" \
//             --split-by "\"personId\"" \
//             --target-dir ${TARGET_DIR} \
//             --fields-terminated-by ',' \
//             --as-textfile \
//             --num-mappers 1 \
//             --delete-target-dir
//             '''
//         }
//     }


//       stage('Sqoop TeamHistories'){
//     steps {
//         sh '''#!/bin/bash
//         set -e

//         HIVE_DB="nba_bronze"
//         HIVE_TABLE="team_histories"
//         CHECK_COL="teamid"
//         TARGET_DIR="/tmp/DE011025/NBA/bronze/team_histories"

//         echo "Getting last ${CHECK_COL} from Hive..."

//         LAST_VALUE=$(
//           (hive -e "SELECT COALESCE(MAX(teamid),0) FROM nba_bronze.team_histories" 2>/dev/null || echo 0  | tail -n 1)
//         )

//         echo "Last imported ${CHECK_COL}: ${LAST_VALUE}"

//         sqoop import \
//           --connect jdbc:postgresql://18.134.163.221:5432/testdb  \
//           --username admin \
//           --password admin123 \
//           --driver org.postgresql.Driver \
//           --query "SELECT * FROM team_histories WHERE \\\"teamId\\\" > ${LAST_VALUE} AND \\$CONDITIONS" \
//           --split-by "\"teamId\"" \
//           --target-dir ${TARGET_DIR} \
//           --fields-terminated-by ',' \
//           --as-textfile \
//           --num-mappers 1 \
//           --delete-target-dir
//         '''
//     }
// }


//         stage('Sqoop TeamStatistics'){
//     steps {
//         sh '''#!/bin/bash
//         set -e

//         HIVE_DB="nba_bronze"
//         HIVE_TABLE="team_statistics"
//         CHECK_COL="gameid"
//         TARGET_DIR="/tmp/DE011025/NBA/bronze/team_statistics"

//         echo "Getting last ${CHECK_COL} from Hive..."

//         LAST_VALUE=$(
//           ( hive -e "SELECT COALESCE(MAX(gamedatetimeest),0) FROM nba_bronze.team_statistics" 2>/dev/null || echo 0  | tail -n 1)
//         )

//         echo "Last imported ${CHECK_COL}: ${LAST_VALUE}"

//         sqoop import \
//           --connect jdbc:postgresql://18.134.163.221:5432/testdb \
//           --username admin \
//           --password admin123 \
//           --driver org.postgresql.Driver \
//           --query "SELECT * FROM team_statistics WHERE \\\"gameDateTimeEst\\\" > '${LAST_VALUE}' AND \\$CONDITIONS" \
//           --split-by "\"gameId\"" \
//           --target-dir ${TARGET_DIR} \
//           --fields-terminated-by ',' \
//           --as-textfile \
//           --num-mappers 1 \
//           --delete-target-dir
//         '''
//     }
// }

       

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

        stage('Run Unit Tests') {
        steps {
            sh '''
            source ${VENV}/bin/activate
            pytest --junitxml=pytest.xml
            '''
        }
        }
        

        // stage('Run silver Silver Players script') {
        //     steps {
        //         sh '''
        //           echo "Running silver cleaning script..."
        //           spark-submit silver_players.py
        //         '''
        //     }
        // }

        // stage('Run silver Silver Games script') {
        //     steps {
        //         sh '''
        //           echo "Running silver cleaning script..."
        //           spark-submit silver_games.py
        //         '''
        //     }
        // }

        // stage('Run silver Player Stats script') {
        //     steps {
        //         sh '''
        //           echo "Running silver cleaning script..."
        //           spark-submit silver_playerstats.py
        //         '''
        //     }
        // }

        // stage('Run silver Team Histories script') {
        //     steps {
        //         sh '''
        //           echo "Running silver cleaning script..."
        //           spark-submit silver_teamhistories.py
        //         '''
        //     }
        // }

        // stage('Run silver Team Statistics script') {
        //     steps {
        //         sh '''
        //           echo "Running silver cleaning script..."
        //           spark-submit silver_teamstatistics.py
        //         '''
        //     }
        // }

        // stage('Run gold script') {
        //     steps {
        //         sh '''
        //           echo "Running gold script..."
        //           spark-submit silver-to-gold.py
        //         '''
        //     }
        // }
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
