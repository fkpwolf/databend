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

//! This mod is the key point about `User` compatibility.
//! Everytime update anything in this file, update the `VER` and let the tests pass.

use std::collections::BTreeMap;
use std::collections::HashSet;
use std::convert::TryFrom;

use common_datavalues::chrono::DateTime;
use common_datavalues::chrono::Utc;
use common_meta_types as mt;
use common_protos::pb;
use common_storage::StorageFsConfig;
use common_storage::StorageGcsConfig;
use common_storage::StorageOssConfig;
use common_storage::StorageParams;
use common_storage::StorageS3Config;
use enumflags2::BitFlags;
use num::FromPrimitive;

use crate::check_ver;
use crate::FromToProto;
use crate::Incompatible;
use crate::MIN_COMPATIBLE_VER;
use crate::VER;

impl FromToProto for mt::AuthInfo {
    type PB = pb::AuthInfo;
    fn from_pb(p: pb::AuthInfo) -> Result<Self, Incompatible>
    where Self: Sized {
        check_ver(p.ver, p.min_compatible)?;

        match p.info {
            Some(pb::auth_info::Info::None(pb::auth_info::None {})) => Ok(mt::AuthInfo::None),
            Some(pb::auth_info::Info::Jwt(pb::auth_info::Jwt {})) => Ok(mt::AuthInfo::JWT),
            Some(pb::auth_info::Info::Password(pb::auth_info::Password {
                hash_value,
                hash_method,
            })) => Ok(mt::AuthInfo::Password {
                hash_value,
                hash_method: FromPrimitive::from_i32(hash_method).ok_or_else(|| Incompatible {
                    reason: format!("invalid PasswordHashMethod: {}", hash_method),
                })?,
            }),
            None => Err(Incompatible {
                reason: "AuthInfo cannot be None".to_string(),
            }),
        }
    }

    fn to_pb(&self) -> Result<pb::AuthInfo, Incompatible> {
        let info = match self {
            mt::AuthInfo::None => Some(pb::auth_info::Info::None(pb::auth_info::None {})),
            mt::AuthInfo::JWT => Some(pb::auth_info::Info::Jwt(pb::auth_info::Jwt {})),
            mt::AuthInfo::Password {
                hash_value,
                hash_method,
            } => Some(pb::auth_info::Info::Password(pb::auth_info::Password {
                hash_value: hash_value.clone(),
                hash_method: *hash_method as i32,
            })),
        };
        Ok(pb::AuthInfo {
            ver: VER,
            min_compatible: MIN_COMPATIBLE_VER,
            info,
        })
    }
}

impl FromToProto for mt::UserOption {
    type PB = pb::UserOption;
    fn from_pb(p: pb::UserOption) -> Result<Self, Incompatible>
    where Self: Sized {
        check_ver(p.ver, p.min_compatible)?;

        // ignore unknown flags
        let flags = BitFlags::<mt::UserOptionFlag, u64>::from_bits_truncate(p.flags);

        Ok(mt::UserOption::default()
            .with_flags(flags)
            .with_default_role(p.default_role))
    }

    fn to_pb(&self) -> Result<pb::UserOption, Incompatible> {
        Ok(pb::UserOption {
            ver: VER,
            min_compatible: MIN_COMPATIBLE_VER,
            flags: self.flags().bits(),
            default_role: self.default_role().cloned(),
        })
    }
}

impl FromToProto for mt::UserQuota {
    type PB = pb::UserQuota;
    fn from_pb(p: pb::UserQuota) -> Result<Self, Incompatible>
    where Self: Sized {
        check_ver(p.ver, p.min_compatible)?;

        Ok(Self {
            max_cpu: p.max_cpu,
            max_memory_in_bytes: p.max_memory_in_bytes,
            max_storage_in_bytes: p.max_storage_in_bytes,
        })
    }

    fn to_pb(&self) -> Result<pb::UserQuota, Incompatible> {
        Ok(pb::UserQuota {
            ver: VER,
            min_compatible: MIN_COMPATIBLE_VER,
            max_cpu: self.max_cpu,
            max_memory_in_bytes: self.max_memory_in_bytes,
            max_storage_in_bytes: self.max_storage_in_bytes,
        })
    }
}

impl FromToProto for mt::GrantObject {
    type PB = pb::GrantObject;
    fn from_pb(p: pb::GrantObject) -> Result<Self, Incompatible>
    where Self: Sized {
        check_ver(p.ver, p.min_compatible)?;

        match p.object {
            Some(pb::grant_object::Object::Global(pb::grant_object::GrantGlobalObject {})) => {
                Ok(mt::GrantObject::Global)
            }
            Some(pb::grant_object::Object::Database(pb::grant_object::GrantDatabaseObject {
                catalog,
                db,
            })) => Ok(mt::GrantObject::Database(catalog, db)),
            Some(pb::grant_object::Object::Table(pb::grant_object::GrantTableObject {
                catalog,
                db,
                table,
            })) => Ok(mt::GrantObject::Table(catalog, db, table)),
            _ => Err(Incompatible {
                reason: "GrantObject cannot be None".to_string(),
            }),
        }
    }

    fn to_pb(&self) -> Result<pb::GrantObject, Incompatible> {
        let object = match self {
            mt::GrantObject::Global => Some(pb::grant_object::Object::Global(
                pb::grant_object::GrantGlobalObject {},
            )),
            mt::GrantObject::Database(catalog, db) => Some(pb::grant_object::Object::Database(
                pb::grant_object::GrantDatabaseObject {
                    catalog: catalog.clone(),
                    db: db.clone(),
                },
            )),
            mt::GrantObject::Table(catalog, db, table) => Some(pb::grant_object::Object::Table(
                pb::grant_object::GrantTableObject {
                    catalog: catalog.clone(),
                    db: db.clone(),
                    table: table.clone(),
                },
            )),
        };
        Ok(pb::GrantObject {
            ver: VER,
            min_compatible: MIN_COMPATIBLE_VER,
            object,
        })
    }
}

impl FromToProto for mt::GrantEntry {
    type PB = pb::GrantEntry;
    fn from_pb(p: pb::GrantEntry) -> Result<Self, Incompatible>
    where Self: Sized {
        check_ver(p.ver, p.min_compatible)?;

        let privileges = BitFlags::<mt::UserPrivilegeType, u64>::from_bits(p.privileges);
        match privileges {
            Ok(privileges) => Ok(mt::GrantEntry::new(
                mt::GrantObject::from_pb(p.object.ok_or_else(|| Incompatible {
                    reason: "GrantEntry.object can not be None".to_string(),
                })?)?,
                privileges,
            )),
            Err(e) => Err(Incompatible {
                reason: format!("UserPrivilegeType error: {}", e),
            }),
        }
    }

    fn to_pb(&self) -> Result<pb::GrantEntry, Incompatible> {
        Ok(pb::GrantEntry {
            ver: VER,
            min_compatible: MIN_COMPATIBLE_VER,
            object: Some(self.object().to_pb()?),
            privileges: self.privileges().bits(),
        })
    }
}

impl FromToProto for mt::UserGrantSet {
    type PB = pb::UserGrantSet;
    fn from_pb(p: pb::UserGrantSet) -> Result<Self, Incompatible>
    where Self: Sized {
        check_ver(p.ver, p.min_compatible)?;

        let mut entries = Vec::new();
        for entry in p.entries.iter() {
            entries.push(mt::GrantEntry::from_pb(entry.clone())?);
        }
        let mut roles = HashSet::new();
        for role in p.roles.iter() {
            roles.insert(role.0.clone());
        }
        Ok(mt::UserGrantSet::new(entries, roles))
    }

    fn to_pb(&self) -> Result<pb::UserGrantSet, Incompatible> {
        let mut entries = Vec::new();
        for entry in self.entries().iter() {
            entries.push(entry.to_pb()?);
        }

        let mut roles = BTreeMap::new();
        for role in self.roles().iter() {
            roles.insert(role.clone(), true);
        }

        Ok(pb::UserGrantSet {
            ver: VER,
            min_compatible: MIN_COMPATIBLE_VER,
            entries,
            roles,
        })
    }
}

impl FromToProto for mt::UserInfo {
    type PB = pb::UserInfo;
    fn from_pb(p: pb::UserInfo) -> Result<Self, Incompatible>
    where Self: Sized {
        check_ver(p.ver, p.min_compatible)?;

        Ok(mt::UserInfo {
            name: p.name.clone(),
            hostname: p.hostname.clone(),
            auth_info: mt::AuthInfo::from_pb(p.auth_info.ok_or_else(|| Incompatible {
                reason: "UserInfo.auth_info cannot be None".to_string(),
            })?)?,
            grants: mt::UserGrantSet::from_pb(p.grants.ok_or_else(|| Incompatible {
                reason: "UserInfo.grants cannot be None".to_string(),
            })?)?,
            quota: mt::UserQuota::from_pb(p.quota.ok_or_else(|| Incompatible {
                reason: "UserInfo.quota cannot be None".to_string(),
            })?)?,
            option: mt::UserOption::from_pb(p.option.ok_or_else(|| Incompatible {
                reason: "UserInfo.option cannot be None".to_string(),
            })?)?,
        })
    }

    fn to_pb(&self) -> Result<pb::UserInfo, Incompatible> {
        Ok(pb::UserInfo {
            ver: VER,
            min_compatible: MIN_COMPATIBLE_VER,
            name: self.name.clone(),
            hostname: self.hostname.clone(),
            auth_info: Some(mt::AuthInfo::to_pb(&self.auth_info)?),
            grants: Some(mt::UserGrantSet::to_pb(&self.grants)?),
            quota: Some(mt::UserQuota::to_pb(&self.quota)?),
            option: Some(mt::UserOption::to_pb(&self.option)?),
        })
    }
}

impl FromToProto for mt::UserIdentity {
    type PB = pb::UserIdentity;
    fn from_pb(p: pb::UserIdentity) -> Result<Self, Incompatible>
    where Self: Sized {
        check_ver(p.ver, p.min_compatible)?;

        Ok(mt::UserIdentity {
            username: p.username.clone(),
            hostname: p.hostname,
        })
    }

    fn to_pb(&self) -> Result<pb::UserIdentity, Incompatible> {
        Ok(pb::UserIdentity {
            ver: VER,
            min_compatible: MIN_COMPATIBLE_VER,
            username: self.username.clone(),
            hostname: self.hostname.clone(),
        })
    }
}

impl FromToProto for mt::StageFileFormatType {
    type PB = pb::user_stage_info::StageFileFormatType;
    fn from_pb(p: pb::user_stage_info::StageFileFormatType) -> Result<Self, Incompatible>
    where Self: Sized {
        match p {
            pb::user_stage_info::StageFileFormatType::Csv => Ok(mt::StageFileFormatType::Csv),
            pb::user_stage_info::StageFileFormatType::Tsv => Ok(mt::StageFileFormatType::Tsv),
            pb::user_stage_info::StageFileFormatType::Json => Ok(mt::StageFileFormatType::Json),
            pb::user_stage_info::StageFileFormatType::NdJson => Ok(mt::StageFileFormatType::NdJson),
            pb::user_stage_info::StageFileFormatType::Avro => Ok(mt::StageFileFormatType::Avro),
            pb::user_stage_info::StageFileFormatType::Orc => Ok(mt::StageFileFormatType::Orc),
            pb::user_stage_info::StageFileFormatType::Parquet => {
                Ok(mt::StageFileFormatType::Parquet)
            }
            pb::user_stage_info::StageFileFormatType::Xml => Ok(mt::StageFileFormatType::Xml),
        }
    }

    fn to_pb(&self) -> Result<pb::user_stage_info::StageFileFormatType, Incompatible> {
        match *self {
            mt::StageFileFormatType::Csv => Ok(pb::user_stage_info::StageFileFormatType::Csv),
            mt::StageFileFormatType::Tsv => Ok(pb::user_stage_info::StageFileFormatType::Tsv),
            mt::StageFileFormatType::Json => Ok(pb::user_stage_info::StageFileFormatType::Json),
            mt::StageFileFormatType::NdJson => Ok(pb::user_stage_info::StageFileFormatType::NdJson),
            mt::StageFileFormatType::Avro => Ok(pb::user_stage_info::StageFileFormatType::Avro),
            mt::StageFileFormatType::Orc => Ok(pb::user_stage_info::StageFileFormatType::Orc),
            mt::StageFileFormatType::Parquet => {
                Ok(pb::user_stage_info::StageFileFormatType::Parquet)
            }
            mt::StageFileFormatType::Xml => Ok(pb::user_stage_info::StageFileFormatType::Xml),
        }
    }
}

impl FromToProto for mt::StageFileCompression {
    type PB = pb::user_stage_info::StageFileCompression;
    fn from_pb(p: pb::user_stage_info::StageFileCompression) -> Result<Self, Incompatible>
    where Self: Sized {
        match p {
            pb::user_stage_info::StageFileCompression::Auto => Ok(mt::StageFileCompression::Auto),
            pb::user_stage_info::StageFileCompression::Gzip => Ok(mt::StageFileCompression::Gzip),
            pb::user_stage_info::StageFileCompression::Bz2 => Ok(mt::StageFileCompression::Bz2),
            pb::user_stage_info::StageFileCompression::Brotli => {
                Ok(mt::StageFileCompression::Brotli)
            }
            pb::user_stage_info::StageFileCompression::Zstd => Ok(mt::StageFileCompression::Zstd),
            pb::user_stage_info::StageFileCompression::Deflate => {
                Ok(mt::StageFileCompression::Deflate)
            }
            pb::user_stage_info::StageFileCompression::RawDeflate => {
                Ok(mt::StageFileCompression::RawDeflate)
            }
            pb::user_stage_info::StageFileCompression::Lzo => Ok(mt::StageFileCompression::Lzo),
            pb::user_stage_info::StageFileCompression::Snappy => {
                Ok(mt::StageFileCompression::Snappy)
            }
            pb::user_stage_info::StageFileCompression::None => Ok(mt::StageFileCompression::None),
            pb::user_stage_info::StageFileCompression::Xz => Ok(mt::StageFileCompression::Xz),
        }
    }

    fn to_pb(&self) -> Result<pb::user_stage_info::StageFileCompression, Incompatible> {
        match *self {
            mt::StageFileCompression::Auto => Ok(pb::user_stage_info::StageFileCompression::Auto),
            mt::StageFileCompression::Gzip => Ok(pb::user_stage_info::StageFileCompression::Gzip),
            mt::StageFileCompression::Bz2 => Ok(pb::user_stage_info::StageFileCompression::Bz2),
            mt::StageFileCompression::Brotli => {
                Ok(pb::user_stage_info::StageFileCompression::Brotli)
            }
            mt::StageFileCompression::Zstd => Ok(pb::user_stage_info::StageFileCompression::Zstd),
            mt::StageFileCompression::Deflate => {
                Ok(pb::user_stage_info::StageFileCompression::Deflate)
            }
            mt::StageFileCompression::RawDeflate => {
                Ok(pb::user_stage_info::StageFileCompression::RawDeflate)
            }
            mt::StageFileCompression::Lzo => Ok(pb::user_stage_info::StageFileCompression::Lzo),
            mt::StageFileCompression::Snappy => {
                Ok(pb::user_stage_info::StageFileCompression::Snappy)
            }
            mt::StageFileCompression::None => Ok(pb::user_stage_info::StageFileCompression::None),
            mt::StageFileCompression::Xz => Ok(pb::user_stage_info::StageFileCompression::Xz),
        }
    }
}

impl FromToProto for mt::StageType {
    type PB = pb::user_stage_info::StageType;
    fn from_pb(p: pb::user_stage_info::StageType) -> Result<Self, Incompatible>
    where Self: Sized {
        match p {
            pb::user_stage_info::StageType::LegacyInternal => Ok(mt::StageType::LegacyInternal),
            pb::user_stage_info::StageType::External => Ok(mt::StageType::External),
            pb::user_stage_info::StageType::Internal => Ok(mt::StageType::Internal),
        }
    }

    fn to_pb(&self) -> Result<pb::user_stage_info::StageType, Incompatible> {
        match *self {
            mt::StageType::LegacyInternal => Ok(pb::user_stage_info::StageType::LegacyInternal),
            mt::StageType::External => Ok(pb::user_stage_info::StageType::External),
            mt::StageType::Internal => Ok(pb::user_stage_info::StageType::Internal),
        }
    }
}

impl FromToProto for StorageParams {
    type PB = pb::user_stage_info::StageStorage;
    fn from_pb(p: pb::user_stage_info::StageStorage) -> Result<Self, Incompatible>
    where Self: Sized {
        match p.storage {
            Some(pb::user_stage_info::stage_storage::Storage::S3(s)) => {
                Ok(StorageParams::S3(StorageS3Config::from_pb(s)?))
            }
            Some(pb::user_stage_info::stage_storage::Storage::Fs(s)) => {
                Ok(StorageParams::Fs(StorageFsConfig::from_pb(s)?))
            }
            Some(pb::user_stage_info::stage_storage::Storage::Gcs(s)) => {
                Ok(StorageParams::Gcs(StorageGcsConfig::from_pb(s)?))
            }
            Some(pb::user_stage_info::stage_storage::Storage::Oss(s)) => {
                Ok(StorageParams::Oss(StorageOssConfig::from_pb(s)?))
            }
            None => Err(Incompatible {
                reason: "StageStorage.storage cannot be None".to_string(),
            }),
        }
    }

    fn to_pb(&self) -> Result<pb::user_stage_info::StageStorage, Incompatible> {
        match self {
            StorageParams::S3(v) => Ok(pb::user_stage_info::StageStorage {
                storage: Some(pb::user_stage_info::stage_storage::Storage::S3(v.to_pb()?)),
            }),
            StorageParams::Fs(v) => Ok(pb::user_stage_info::StageStorage {
                storage: Some(pb::user_stage_info::stage_storage::Storage::Fs(v.to_pb()?)),
            }),
            StorageParams::Gcs(v) => Ok(pb::user_stage_info::StageStorage {
                storage: Some(pb::user_stage_info::stage_storage::Storage::Gcs(v.to_pb()?)),
            }),
            StorageParams::Oss(v) => Ok(pb::user_stage_info::StageStorage {
                storage: Some(pb::user_stage_info::stage_storage::Storage::Oss(v.to_pb()?)),
            }),
            _ => todo!("other stage storage are not supported"),
        }
    }
}

impl FromToProto for mt::StageParams {
    type PB = pb::user_stage_info::StageParams;
    fn from_pb(p: pb::user_stage_info::StageParams) -> Result<Self, Incompatible>
    where Self: Sized {
        Ok(mt::StageParams {
            storage: StorageParams::from_pb(p.storage.ok_or_else(|| Incompatible {
                reason: "pb::user_stage_info::StageParams.storage cannot be None".to_string(),
            })?)?,
        })
    }

    fn to_pb(&self) -> Result<pb::user_stage_info::StageParams, Incompatible> {
        Ok(pb::user_stage_info::StageParams {
            storage: Some(StorageParams::to_pb(&self.storage)?),
        })
    }
}

impl FromToProto for mt::FileFormatOptions {
    type PB = pb::user_stage_info::FileFormatOptions;
    fn from_pb(p: pb::user_stage_info::FileFormatOptions) -> Result<Self, Incompatible>
    where Self: Sized {
        check_ver(p.ver, p.min_compatible)?;

        let format = mt::StageFileFormatType::from_pb(
            FromPrimitive::from_i32(p.format).ok_or_else(|| Incompatible {
                reason: format!("invalid StageFileFormatType: {}", p.format),
            })?,
        )?;

        let compression = mt::StageFileCompression::from_pb(
            FromPrimitive::from_i32(p.compression).ok_or_else(|| Incompatible {
                reason: format!("invalid StageFileCompression: {}", p.compression),
            })?,
        )?;

        Ok(mt::FileFormatOptions {
            format,
            skip_header: p.skip_header,
            field_delimiter: p.field_delimiter.clone(),
            record_delimiter: p.record_delimiter,
            escape: p.escape,
            compression,
        })
    }

    fn to_pb(&self) -> Result<pb::user_stage_info::FileFormatOptions, Incompatible> {
        let format = mt::StageFileFormatType::to_pb(&self.format)? as i32;
        let compression = mt::StageFileCompression::to_pb(&self.compression)? as i32;
        Ok(pb::user_stage_info::FileFormatOptions {
            ver: VER,
            min_compatible: MIN_COMPATIBLE_VER,
            format,
            skip_header: self.skip_header,
            field_delimiter: self.field_delimiter.clone(),
            record_delimiter: self.record_delimiter.clone(),
            compression,
            escape: self.escape.clone(),
        })
    }
}

impl FromToProto for mt::OnErrorMode {
    type PB = pb::user_stage_info::OnErrorMode;
    fn from_pb(p: pb::user_stage_info::OnErrorMode) -> Result<Self, Incompatible>
    where Self: Sized {
        match p.mode {
            Some(pb::user_stage_info::on_error_mode::Mode::None(_)) => Ok(mt::OnErrorMode::None),
            Some(pb::user_stage_info::on_error_mode::Mode::Continue(_)) => {
                Ok(mt::OnErrorMode::Continue)
            }
            Some(pb::user_stage_info::on_error_mode::Mode::SkipFile(_)) => {
                Ok(mt::OnErrorMode::SkipFile)
            }
            Some(pb::user_stage_info::on_error_mode::Mode::SkipFileNum(n)) => {
                Ok(mt::OnErrorMode::SkipFileNum(n))
            }
            Some(pb::user_stage_info::on_error_mode::Mode::AbortStatement(_)) => {
                Ok(mt::OnErrorMode::AbortStatement)
            }
            None => Err(Incompatible {
                reason: "OnErrorMode.mode cannot be None".to_string(),
            }),
        }
    }

    fn to_pb(&self) -> Result<pb::user_stage_info::OnErrorMode, Incompatible> {
        match self {
            mt::OnErrorMode::None => Ok(pb::user_stage_info::OnErrorMode {
                mode: Some(pb::user_stage_info::on_error_mode::Mode::None(pb::Empty {})),
            }),
            mt::OnErrorMode::Continue => Ok(pb::user_stage_info::OnErrorMode {
                mode: Some(pb::user_stage_info::on_error_mode::Mode::Continue(
                    pb::Empty {},
                )),
            }),
            mt::OnErrorMode::SkipFile => Ok(pb::user_stage_info::OnErrorMode {
                mode: Some(pb::user_stage_info::on_error_mode::Mode::SkipFile(
                    pb::Empty {},
                )),
            }),
            mt::OnErrorMode::SkipFileNum(n) => Ok(pb::user_stage_info::OnErrorMode {
                mode: Some(pb::user_stage_info::on_error_mode::Mode::SkipFileNum(*n)),
            }),
            mt::OnErrorMode::AbortStatement => Ok(pb::user_stage_info::OnErrorMode {
                mode: Some(pb::user_stage_info::on_error_mode::Mode::AbortStatement(
                    pb::Empty {},
                )),
            }),
        }
    }
}

impl FromToProto for mt::CopyOptions {
    type PB = pb::user_stage_info::CopyOptions;
    fn from_pb(p: pb::user_stage_info::CopyOptions) -> Result<Self, Incompatible>
    where Self: Sized {
        let on_error = mt::OnErrorMode::from_pb(p.on_error.ok_or_else(|| Incompatible {
            reason: "CopyOptions.on_error cannot be None".to_string(),
        })?)?;
        let size_limit = usize::try_from(p.size_limit).map_err(|err| Incompatible {
            reason: format!("CopyOptions.size_limit cannot be convert to usize: {}", err),
        })?;
        let split_size = usize::try_from(p.split_size).map_err(|err| Incompatible {
            reason: format!("CopyOptions.split_size cannot be convert to usize: {}", err),
        })?;

        let max_file_size = usize::try_from(p.max_file_size).map_err(|err| Incompatible {
            reason: format!(
                "CopyOptions.max_file_size cannot be convert to usize: {}",
                err
            ),
        })?;
        Ok(mt::CopyOptions {
            on_error,
            size_limit,
            split_size,
            purge: p.purge,
            single: p.single,
            max_file_size,
        })
    }

    fn to_pb(&self) -> Result<pb::user_stage_info::CopyOptions, Incompatible> {
        let on_error = mt::OnErrorMode::to_pb(&self.on_error)?;
        let size_limit = u64::try_from(self.size_limit).map_err(|err| Incompatible {
            reason: format!("CopyOptions.size_limit cannot be convert to u64: {}", err),
        })?;
        let split_size = u64::try_from(self.split_size).map_err(|err| Incompatible {
            reason: format!("CopyOptions.split_size cannot be convert to u64: {}", err),
        })?;
        let max_file_size = u64::try_from(self.max_file_size).map_err(|err| Incompatible {
            reason: format!(
                "CopyOptions.max_file_size cannot be convert to u64: {}",
                err
            ),
        })?;
        Ok(pb::user_stage_info::CopyOptions {
            on_error: Some(on_error),
            size_limit,
            split_size,
            purge: self.purge,
            single: self.single,
            max_file_size,
        })
    }
}

impl FromToProto for mt::UserStageInfo {
    type PB = pb::UserStageInfo;
    fn from_pb(p: pb::UserStageInfo) -> Result<Self, Incompatible>
    where Self: Sized {
        check_ver(p.ver, p.min_compatible)?;
        Ok(mt::UserStageInfo {
            stage_name: p.stage_name.clone(),
            stage_type: mt::StageType::from_pb(FromPrimitive::from_i32(p.stage_type).ok_or_else(
                || Incompatible {
                    reason: format!("invalid StageType: {}", p.stage_type),
                },
            )?)?,
            stage_params: mt::StageParams::from_pb(p.stage_params.ok_or_else(|| {
                Incompatible {
                    reason: "UserStageInfo.stage_params cannot be None".to_string(),
                }
            })?)?,
            file_format_options: mt::FileFormatOptions::from_pb(
                p.file_format_options.ok_or_else(|| Incompatible {
                    reason: "UserStageInfo.file_format_options cannot be None".to_string(),
                })?,
            )?,
            copy_options: mt::CopyOptions::from_pb(p.copy_options.ok_or_else(|| {
                Incompatible {
                    reason: "UserStageInfo.copy_options cannot be None".to_string(),
                }
            })?)?,
            comment: p.comment,
            number_of_files: p.number_of_files,
            creator: match p.creator {
                Some(c) => Some(mt::UserIdentity::from_pb(c)?),
                None => None,
            },
        })
    }

    fn to_pb(&self) -> Result<pb::UserStageInfo, Incompatible> {
        Ok(pb::UserStageInfo {
            ver: VER,
            min_compatible: MIN_COMPATIBLE_VER,
            stage_name: self.stage_name.clone(),
            stage_type: mt::StageType::to_pb(&self.stage_type)? as i32,
            stage_params: Some(mt::StageParams::to_pb(&self.stage_params)?),
            file_format_options: Some(mt::FileFormatOptions::to_pb(&self.file_format_options)?),
            copy_options: Some(mt::CopyOptions::to_pb(&self.copy_options)?),
            comment: self.comment.clone(),
            number_of_files: self.number_of_files,
            creator: match &self.creator {
                Some(c) => Some(mt::UserIdentity::to_pb(c)?),
                None => None,
            },
        })
    }
}

impl FromToProto for mt::StageFile {
    type PB = pb::StageFile;
    fn from_pb(p: pb::StageFile) -> Result<Self, Incompatible>
    where Self: Sized {
        check_ver(p.ver, p.min_compatible)?;
        Ok(mt::StageFile {
            path: p.path.clone(),
            size: p.size,
            md5: p.md5.clone(),
            last_modified: DateTime::<Utc>::from_pb(p.last_modified)?,
            creator: match p.creator {
                Some(c) => Some(mt::UserIdentity::from_pb(c)?),
                None => None,
            },
            etag: p.etag.clone(),
        })
    }

    fn to_pb(&self) -> Result<pb::StageFile, Incompatible> {
        Ok(pb::StageFile {
            ver: VER,
            min_compatible: MIN_COMPATIBLE_VER,
            path: self.path.clone(),
            size: self.size,
            md5: self.md5.clone(),
            last_modified: self.last_modified.to_pb()?,
            creator: match &self.creator {
                Some(c) => Some(mt::UserIdentity::to_pb(c)?),
                None => None,
            },
            etag: self.etag.clone(),
        })
    }
}
