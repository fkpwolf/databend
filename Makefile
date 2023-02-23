HUB ?= datafuselabs
TAG ?= latest
PLATFORM ?= linux/amd64,linux/arm64
VERSION ?= latest
ADD_NODES ?= 0
NUM_CPUS ?= 2
TENANT_ID ?= "tenant"
CLUSTER_ID ?= "test"

CARGO_TARGET_DIR ?= $(CURDIR)/target

# Remove this env after rust 1.68 has been released.
export CARGO_UNSTABLE_SPARSE_REGISTRY = true

# Setup dev toolchain
setup:
	bash ./scripts/setup/dev_setup.sh

fmt:
	cargo fmt --all

lint:
	cargo fmt --all

	# Check unused deps(make setup to install)
	cargo machete
	# Check typos(make setup to install)
	typos
	# Cargo.toml file formatter(make setup to install)
	taplo fmt
	# Python file formatter(make setup to install)
	black tests/
	# Bash file formatter(make setup to install)
	shfmt -l -w scripts/*

	cargo clippy --workspace --all-targets -- -D warnings

lint-yaml:
	yamllint -f auto .

check-license:
	license-eye -v info -c .licenserc.yaml header check

run: build-release
	BUILD_PROFILE=release bash ./scripts/ci/deploy/databend-query-standalone.sh

run-debug: build
	bash ./scripts/ci/deploy/databend-query-standalone.sh

run-debug-management: build
	bash ./scripts/ci/deploy/databend-query-management-mode.sh

build:
	bash ./scripts/build/build-debug.sh

build-release:
	bash ./scripts/build/build-release.sh
ifeq ($(shell uname),Linux) # Macs don't have objcopy
	# Reduce binary size by compressing binaries.
	objcopy --compress-debug-sections=zlib-gnu ${CARGO_TARGET_DIR}/release/databend-query
	objcopy --compress-debug-sections=zlib-gnu ${CARGO_TARGET_DIR}/release/databend-meta
	objcopy --compress-debug-sections=zlib-gnu ${CARGO_TARGET_DIR}/release/databend-metactl
endif

build-native:
	bash ./scripts/build/build-native.sh

unit-test:
	ulimit -n 10000;ulimit -s 16384; RUST_LOG="ERROR" bash ./scripts/ci/ci-run-unit-tests.sh

miri:
	cargo miri setup
	MIRIFLAGS="-Zmiri-disable-isolation" cargo miri test --no-default-features

embedded-meta-test: build
	rm -rf ./_meta_embedded*
	bash ./scripts/ci/ci-run-tests-embedded-meta.sh

stateless-test: build
	rm -rf ./_meta*/
	rm -rf .databend
	ulimit -n 10000;ulimit -s 16384; bash ./scripts/ci/ci-run-tests-embedded-meta.sh

sqllogic-test: build
	rm -rf ./_meta*/
	ulimit -n 10000;ulimit -s 16384; bash ./scripts/ci/ci-run-sqllogic-tests.sh

stateless-cluster-test: build
	rm -rf ./_meta*/
	bash ./scripts/ci/ci-run-stateless-tests-cluster.sh

stateless-cluster-test-tls: build
	rm -rf ./_meta*/
	bash ./scripts/ci/ci-run-stateless-tests-cluster-tls.sh

metactl-test:
	bash ./tests/metactl/test-metactl.sh
	bash ./tests/metactl/test-metactl-restore-new-cluster.sh

meta-kvapi-test:
	bash ./tests/meta-kvapi/test-meta-kvapi.sh

meta-bench: build-release
	bash ./scripts/benchmark/run-meta-benchmark.sh 10 1000

test: unit-test stateless-test sqllogic-test metactl-test

profile:
	bash ./scripts/ci/ci-run-profile.sh

docs:
	bash ./scripts/build/build-website.sh

clean:
	cargo clean
	rm -f ./nohup.out ./tests/suites/0_stateless/*.stdout-e
	rm -rf ./_meta*/ ./_logs*/ ./src/query/service_logs*/ ./src/meta/service/_logs*/ ./stateless_test_data/
	rm -rf ./src/common/base/_logs*/ ./src/meta/raft-store/_logs*/ ./src/meta/sled-store/_logs*/
	rm -rf ./.databend ./query/service/.databend ./meta/service/.databend

.PHONY: setup test run build fmt lint docker clean docs
