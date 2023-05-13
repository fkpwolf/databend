#!/bin/bash

set -e

BENCHMARK_ID=${BENCHMARK_ID:-$(date +%s)}
BENCHMARK_DATASET=${BENCHMARK_DATASET:-hits}
BENCHMARK_SIZE=${BENCHMARK_SIZE:-Medium}
BENCHMARK_VERSION=${BENCHMARK_VERSION:-}
BENCHMARK_DATABASE=${BENCHMARK_DATABASE:-default}

if [[ -z "${BENCHMARK_VERSION}" ]]; then
    echo "Please set BENCHMARK_VERSION to run the benchmark."
    exit 1
fi

CLOUD_USER=${CLOUD_USER:-}
CLOUD_PASSWORD=${CLOUD_PASSWORD:-}
CLOUD_GATEWAY=${CLOUD_GATEWAY:-}
CLOUD_WAREHOUSE=${CLOUD_WAREHOUSE:-benchmark-${BENCHMARK_ID}}

if [[ -z "${CLOUD_USER}" || -z "${CLOUD_PASSWORD}" || -z "${CLOUD_GATEWAY}" ]]; then
    echo "Please set CLOUD_USER, CLOUD_PASSWORD and CLOUD_GATEWAY to run the benchmark."
    exit 1
fi

echo "Checking script dependencies..."
python3 --version
yq --version
bendsql --version

echo "Preparing benchmark metadata..."
echo '{}' >result.json
yq -i ".date = \"$(date -u +%Y-%m-%d)\"" result.json
yq -i '.tags = ["s3"]' result.json
case ${BENCHMARK_SIZE} in
Medium)
    yq -i '.cluster_size = "16"' result.json
    yq -i '.machine = "Medium"' result.json
    ;;
Large)
    yq -i '.cluster_size = "64"' result.json
    yq -i '.machine = "Large"' result.json
    ;;
*)
    echo "Unsupported benchmark size: ${BENCHMARK_SIZE}"
    exit 1
    ;;
esac

echo "#######################################################"
echo "Running benchmark for Databend Cloud with S3 storage..."

export BENDSQL_DSN="databend://${CLOUD_USER}:${CLOUD_PASSWORD}@${CLOUD_GATEWAY}:443/${BENCHMARK_DATABASE}?warehouse=${CLOUD_WAREHOUSE}"

echo "Creating warehouse..."
echo "DROP WAREHOUSE IF EXISTS '${CLOUD_WAREHOUSE}';" | bendsql
echo "CREATE WAREHOUSE '${CLOUD_WAREHOUSE}' WITH version='${BENCHMARK_VERSION}' warehouse_size='${BENCHMARK_SIZE}';" | bendsql
echo "SHOW WAREHOUSES;" | bendsql --output table

max_retry=15
counter=0
until bendsql --query="SHOW WAREHOUSES LIKE '${CLOUD_WAREHOUSE}'" | grep -q "Running"; do
    if [[ $counter -gt $max_retry ]]; then
        echo "Failed to start warehouse ${CLOUD_WAREHOUSE} in time."
        exit 1
    fi
    echo "Waiting for warehouse to be ready..."
    counter=$((counter + 1))
    sleep 10
done

echo "Running queries..."

# analyze table
echo "Analyze table..."
bendsql <"${BENCHMARK_DATASET}/analyze.sql"

function run_query() {
    local query_num=$1
    local seq=$2
    local query=$3

    local q_time
    q_time=$(echo "$query" | bendsql --time)
    if [[ -n $q_time ]]; then
        echo "Q${query_num}[$seq] succeeded in $q_time seconds"
        yq -i ".result[${query_num}] += [${q_time}]" result.json
    else
        echo "Q${query_num}[$seq] failed"
    fi
}

TRIES=3
QUERY_NUM=0
while read -r query; do
    echo "Running Q${QUERY_NUM}: ${query}"
    yq -i ".result += [[]]" result.json
    for i in $(seq 1 $TRIES); do
        run_query "$QUERY_NUM" "$i" "$query"
    done
    QUERY_NUM=$((QUERY_NUM + 1))
done <"${BENCHMARK_DATASET}/queries.sql"

echo "Cleaning up..."
echo "DROP WAREHOUSE IF EXISTS '${CLOUD_WAREHOUSE}';" | bendsql
