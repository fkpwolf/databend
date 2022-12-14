// Copyright 2021 Datafuse Labs.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

mod grpc_action;
mod grpc_client;
mod kv_api_impl;
mod message;

pub use grpc_action::MetaGrpcReq;
pub use grpc_action::RequestFor;
pub use grpc_client::ClientHandle;
pub use grpc_client::MetaGrpcClient;
pub use message::ClientWorkerRequest;
use once_cell::sync::Lazy;
use semver::BuildMetadata;
use semver::Prerelease;
use semver::Version;

pub static METACLI_COMMIT_SEMVER: Lazy<Version> = Lazy::new(|| {
    let build_semver = option_env!("VERGEN_GIT_SEMVER");
    let semver = build_semver.expect("VERGEN_GIT_SEMVER can not be None");

    let semver = semver.strip_prefix('v').unwrap_or(semver);

    Version::parse(semver).unwrap_or_else(|e| panic!("Invalid semver: {:?}: {}", semver, e))
});

/// Oldest compatible nightly metasrv version
///
/// ```text
/// now  --.
///        |
/// 8.79 . |
///      | |
///      | '---> 8.35
///      |    .--'
///      |    |
///      '-----> 8.30
///           |  |
///           |  |
/// 7.59 <----+--'
///
/// Client       Server
///
/// --> : pointing to min compatible version
/// ```
///
/// - 2022-10-19: after 0.8.79:
///   Update min compatible server to 0.8.35:
///   Since which, meta-server adds new API kv_api() to replace write_msg() and read_msg();
///   Feature commit: 69a05aca41036976fec37ad7a8b447e2868ef08b 2022-09-14
pub static MIN_METASRV_SEMVER: Version = Version {
    major: 0,
    minor: 8,
    patch: 35,
    pre: Prerelease::EMPTY,
    build: BuildMetadata::EMPTY,
};

pub fn to_digit_ver(v: &Version) -> u64 {
    v.major * 1_000_000 + v.minor * 1_000 + v.patch
}

pub fn from_digit_ver(u: u64) -> Version {
    Version::new(u / 1_000_000, u / 1_000 % 1_000, u % 1_000)
}
