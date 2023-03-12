#!/bin/sh

set -o errexit

SCRIPT_PATH="$(cd "$(dirname "$0")" >/dev/null 2>&1 && pwd)"

rm -fr .databend/

echo " === start 3 meta node cluster"

nohup ./target/debug/databend-meta --config-file=./tests/metactl/config/databend-meta-node-1.toml &
python3 scripts/ci/wait_tcp.py --timeout 5 --port 9191

sleep 1

nohup ./target/debug/databend-meta --config-file=./tests/metactl/config/databend-meta-node-2.toml &
python3 scripts/ci/wait_tcp.py --timeout 5 --port 28202

sleep 1

nohup ./target/debug/databend-meta --config-file=./tests/metactl/config/databend-meta-node-3.toml &
python3 scripts/ci/wait_tcp.py --timeout 5 --port 28302

sleep 1

curl -sL http://127.0.0.1:28101/v1/cluster/status

echo " === stop 3 meta node cluster"
killall databend-meta
sleep 2

echo " === export meta node data"

./target/debug/databend-metactl --export --raft-dir ./.databend/meta1 --db meta.db


rm -fr .databend/

echo " === import old meta node data to new cluster"
./target/debug/databend-metactl --import --raft-dir ./.databend/new_meta1 --id=4 --db meta.db --initial-cluster 4=localhost:29103,127.0.0.1:19191 5=localhost:29203,127.0.0.1:29191 6=localhost:29303,127.0.0.1:39191
./target/debug/databend-metactl --import --raft-dir ./.databend/new_meta2 --id=5 --db meta.db --initial-cluster 4=localhost:29103,127.0.0.1:19191 5=localhost:29203,127.0.0.1:29191 6=localhost:29303,127.0.0.1:39191
# test cluster config without grpc address
./target/debug/databend-metactl --import --raft-dir ./.databend/new_meta3 --id=6 --db meta.db --initial-cluster 4=localhost:29103 5=localhost:29203 6=localhost:29303

echo " === check if state machine is complete by checking key 'LastMembership'"
if ./target/debug/databend-metactl --export --raft-dir ./.databend/new_meta1 | grep LastMembership; then
    echo "=== 'LastMembership' is found"
else
    echo "=== 'LastMembership' is not found"
    exit 1;
fi

echo " === start 3 new meta node cluster"
nohup ./target/debug/databend-meta --config-file=./tests/metactl/config/new-databend-meta-node-1.toml &
python3 scripts/ci/wait_tcp.py --timeout 5 --port 19191

nohup ./target/debug/databend-meta --config-file=./tests/metactl/config/new-databend-meta-node-2.toml &
python3 scripts/ci/wait_tcp.py --timeout 5 --port 29191

nohup ./target/debug/databend-meta --config-file=./tests/metactl/config/new-databend-meta-node-3.toml &
python3 scripts/ci/wait_tcp.py --timeout 5 --port 39191

echo " === sleep 3 sec to wait for membership to commit"
time sleep 5

echo " === dump new cluster state:"
curl -sL http://127.0.0.1:28101/v1/cluster/status
echo ""

echo " === check new cluster state has the voters 4"
curl -sL http://127.0.0.1:28101/v1/cluster/status \
    | grep '{"name":"4","endpoint":{"addr":"localhost","port":29103},"grpc_api_advertise_address":"127.0.0.1:19191"}'

echo " === check new cluster state has the voters 5"
curl -sL http://127.0.0.1:28101/v1/cluster/status \
    | grep '{"name":"5","endpoint":{"addr":"localhost","port":29203},"grpc_api_advertise_address":"127.0.0.1:29191"}'

echo " === check new cluster state has the voters 6"
curl -sL http://127.0.0.1:28101/v1/cluster/status \
    | grep '{"name":"6","endpoint":{"addr":"localhost","port":29303},"grpc_api_advertise_address":"127.0.0.1:39191"}'
echo ""

killall databend-meta
