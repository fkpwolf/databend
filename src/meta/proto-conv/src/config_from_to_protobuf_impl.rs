// Copyright 2022 Datafuse Labs.
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

use common_meta_types::StorageFsConfig;
use common_meta_types::StorageGcsConfig;
use common_meta_types::StorageOssConfig;
use common_meta_types::StorageS3Config;
use common_protos::pb;

use crate::reader_check_msg;
use crate::FromToProto;
use crate::Incompatible;
use crate::MIN_READER_VER;
use crate::VER;

impl FromToProto for StorageS3Config {
    type PB = pb::S3StorageConfig;

    fn get_pb_ver(p: &Self::PB) -> u64 {
        p.version
    }

    fn from_pb(p: pb::S3StorageConfig) -> Result<Self, Incompatible>
    where Self: Sized {
        reader_check_msg(p.version, p.min_reader_ver)?;

        Ok(StorageS3Config {
            region: p.region,
            endpoint_url: p.endpoint_url,
            access_key_id: p.access_key_id,
            secret_access_key: p.secret_access_key,
            security_token: p.security_token,
            bucket: p.bucket,
            root: p.root,
            master_key: p.master_key,
            disable_credential_loader: p.disable_credential_loader,
            enable_virtual_host_style: p.enable_virtual_host_style,
            role_arn: p.role_arn,
            external_id: p.external_id,
        })
    }

    fn to_pb(&self) -> Result<pb::S3StorageConfig, Incompatible> {
        Ok(pb::S3StorageConfig {
            version: VER,
            min_reader_ver: MIN_READER_VER,
            region: self.region.clone(),
            endpoint_url: self.endpoint_url.clone(),
            access_key_id: self.access_key_id.clone(),
            secret_access_key: self.secret_access_key.clone(),
            security_token: self.security_token.clone(),
            bucket: self.bucket.clone(),
            root: self.root.clone(),
            master_key: self.master_key.clone(),
            disable_credential_loader: self.disable_credential_loader,
            enable_virtual_host_style: self.enable_virtual_host_style,
            role_arn: self.role_arn.clone(),
            external_id: self.external_id.clone(),
        })
    }
}

impl FromToProto for StorageGcsConfig {
    type PB = pb::GcsStorageConfig;

    fn get_pb_ver(p: &Self::PB) -> u64 {
        p.version
    }

    fn from_pb(p: Self::PB) -> Result<Self, Incompatible>
    where Self: Sized {
        reader_check_msg(p.version, p.min_reader_ver)?;

        Ok(StorageGcsConfig {
            credential: p.credential,
            endpoint_url: p.endpoint_url,
            bucket: p.bucket,
            root: p.root,
        })
    }

    fn to_pb(&self) -> Result<Self::PB, Incompatible> {
        Ok(pb::GcsStorageConfig {
            version: VER,
            min_reader_ver: MIN_READER_VER,
            credential: self.credential.clone(),
            endpoint_url: self.endpoint_url.clone(),
            bucket: self.bucket.clone(),
            root: self.root.clone(),
        })
    }
}

impl FromToProto for StorageFsConfig {
    type PB = pb::FsStorageConfig;
    fn get_pb_ver(p: &Self::PB) -> u64 {
        p.version
    }

    fn from_pb(p: pb::FsStorageConfig) -> Result<Self, Incompatible>
    where Self: Sized {
        reader_check_msg(p.version, p.min_reader_ver)?;

        Ok(StorageFsConfig { root: p.root })
    }

    fn to_pb(&self) -> Result<pb::FsStorageConfig, Incompatible> {
        Ok(pb::FsStorageConfig {
            version: VER,
            min_reader_ver: MIN_READER_VER,
            root: self.root.clone(),
        })
    }
}

impl FromToProto for StorageOssConfig {
    type PB = pb::OssStorageConfig;
    fn get_pb_ver(p: &Self::PB) -> u64 {
        p.version
    }

    fn from_pb(p: pb::OssStorageConfig) -> Result<Self, Incompatible>
    where Self: Sized {
        reader_check_msg(p.version, p.min_reader_ver)?;

        Ok(StorageOssConfig {
            endpoint_url: p.endpoint_url,
            presign_endpoint_url: "".to_string(),
            bucket: p.bucket,
            root: p.root,

            access_key_id: p.access_key_id,
            access_key_secret: p.access_key_secret,
        })
    }

    fn to_pb(&self) -> Result<pb::OssStorageConfig, Incompatible> {
        Ok(pb::OssStorageConfig {
            version: VER,
            min_reader_ver: MIN_READER_VER,
            endpoint_url: self.endpoint_url.clone(),
            bucket: self.bucket.clone(),
            root: self.root.clone(),
            access_key_id: self.access_key_id.clone(),
            access_key_secret: self.access_key_secret.clone(),
        })
    }
}
