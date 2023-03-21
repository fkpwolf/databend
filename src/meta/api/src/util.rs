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

use std::fmt::Display;

use common_meta_app::app_error::AppError;
use common_meta_app::app_error::ShareHasNoGrantedDatabase;
use common_meta_app::app_error::UnknownDatabase;
use common_meta_app::app_error::UnknownShare;
use common_meta_app::app_error::UnknownShareAccounts;
use common_meta_app::app_error::UnknownShareId;
use common_meta_app::app_error::UnknownTable;
use common_meta_app::schema::DatabaseId;
use common_meta_app::schema::DatabaseIdToName;
use common_meta_app::schema::DatabaseMeta;
use common_meta_app::schema::DatabaseNameIdent;
use common_meta_app::schema::TableNameIdent;
use common_meta_app::share::*;
use common_meta_kvapi::kvapi;
use common_meta_kvapi::kvapi::UpsertKVReq;
use common_meta_types::txn_condition::Target;
use common_meta_types::txn_op::Request;
use common_meta_types::ConditionResult;
use common_meta_types::InvalidArgument;
use common_meta_types::InvalidReply;
use common_meta_types::MatchSeq;
use common_meta_types::MetaError;
use common_meta_types::MetaNetworkError;
use common_meta_types::Operation;
use common_meta_types::TxnCondition;
use common_meta_types::TxnDeleteRequest;
use common_meta_types::TxnOp;
use common_meta_types::TxnOpResponse;
use common_meta_types::TxnPutRequest;
use common_meta_types::TxnRequest;
use common_proto_conv::FromToProto;
use enumflags2::BitFlags;
use tracing::debug;
use ConditionResult::Eq;

use crate::kv_app_error::KVAppError;
use crate::reply::txn_reply_to_api_result;
use crate::Id;

pub const TXN_MAX_RETRY_TIMES: u32 = 10;

/// Get value that its type is `u64`.
///
/// It expects the kv-value's type is `u64`, such as:
/// `__fd_table/<db_id>/<table_name> -> (seq, table_id)`, or
/// `__fd_database/<tenant>/<db_name> -> (seq, db_id)`.
///
/// It returns (seq, `u64` value).
/// If not found, (0,0) is returned.
pub async fn get_u64_value<T: kvapi::Key>(
    kv_api: &(impl kvapi::KVApi<Error = MetaError> + ?Sized),
    key: &T,
) -> Result<(u64, u64), KVAppError> {
    let res = kv_api.get_kv(&key.to_string_key()).await?;

    if let Some(seq_v) = res {
        Ok((seq_v.seq, *deserialize_u64(&seq_v.data)?))
    } else {
        Ok((0, 0))
    }
}

/// Get value that are encoded with FromToProto.
///
/// It returns seq number and the data.
pub async fn get_pb_value<K, T>(
    kv_api: &(impl kvapi::KVApi<Error = MetaError> + ?Sized),
    k: &K,
) -> Result<(u64, Option<T>), KVAppError>
where
    K: kvapi::Key,
    T: FromToProto,
    T::PB: common_protos::prost::Message + Default,
{
    let res = kv_api.get_kv(&k.to_string_key()).await?;

    if let Some(seq_v) = res {
        Ok((seq_v.seq, Some(deserialize_struct(&seq_v.data)?)))
    } else {
        Ok((0, None))
    }
}

/// Batch get values that are encoded with FromToProto.
pub async fn mget_pb_values<T>(
    kv_api: &(impl kvapi::KVApi<Error = MetaError> + ?Sized),
    keys: &[String],
) -> Result<Vec<(u64, Option<T>)>, MetaError>
where
    T: FromToProto,
    T::PB: common_protos::prost::Message + Default,
{
    let seq_bytes = kv_api.mget_kv(keys).await?;
    let mut seq_values = Vec::with_capacity(keys.len());
    for seq_v in seq_bytes {
        if let Some(seq_v) = seq_v {
            let seq = seq_v.seq;
            let v = deserialize_struct(&seq_v.data)?;
            seq_values.push((seq, Some(v)))
        } else {
            seq_values.push((0, None));
        }
    }

    Ok(seq_values)
}

/// It returns a vec of structured key(such as DatabaseNameIdent), such as:
/// all the `db_name` with prefix `__fd_database/<tenant>/`.
pub async fn list_keys<K: kvapi::Key>(
    kv_api: &(impl kvapi::KVApi<Error = MetaError> + ?Sized),
    key: &K,
) -> Result<Vec<K>, KVAppError> {
    let res = kv_api.prefix_list_kv(&key.to_string_key()).await?;

    let n = res.len();

    let mut structured_keys = Vec::with_capacity(n);

    for (str_key, _seq_id) in res.iter() {
        let struct_key = K::from_str_key(str_key).map_err(|e| {
            let inv = InvalidReply::new("fail to list_keys", &e);
            MetaNetworkError::InvalidReply(inv)
        })?;
        structured_keys.push(struct_key);
    }

    Ok(structured_keys)
}

/// List kvs whose value's type is `u64`.
///
/// It expects the kv-value' type is `u64`, such as:
/// `__fd_table/<db_id>/<table_name> -> (seq, table_id)`, or
/// `__fd_database/<tenant>/<db_name> -> (seq, db_id)`.
///
/// It returns a vec of structured key(such as DatabaseNameIdent) and a vec of `u64`.
pub async fn list_u64_value<K: kvapi::Key>(
    kv_api: &(impl kvapi::KVApi<Error = MetaError> + ?Sized),
    key: &K,
) -> Result<(Vec<K>, Vec<u64>), MetaError> {
    let res = kv_api.prefix_list_kv(&key.to_string_key()).await?;

    let n = res.len();

    let mut structured_keys = Vec::with_capacity(n);
    let mut values = Vec::with_capacity(n);

    for (str_key, seqv) in res.iter() {
        let id = *deserialize_u64(&seqv.data)?;
        values.push(id);

        // Parse key
        let struct_key = K::from_str_key(str_key).map_err(|e| {
            let inv = InvalidReply::new("list_u64_value", &e);
            MetaNetworkError::InvalidReply(inv)
        })?;
        structured_keys.push(struct_key);
    }

    Ok((structured_keys, values))
}

pub fn serialize_u64(value: impl Into<Id>) -> Result<Vec<u8>, MetaNetworkError> {
    let v = serde_json::to_vec(&*value.into()).map_err(|e| {
        let inv = InvalidArgument::new(e, "");
        MetaNetworkError::InvalidArgument(inv)
    })?;
    Ok(v)
}

pub fn deserialize_u64(v: &[u8]) -> Result<Id, MetaNetworkError> {
    let id = serde_json::from_slice(v).map_err(|e| {
        let inv = InvalidReply::new("", &e);
        MetaNetworkError::InvalidReply(inv)
    })?;
    Ok(Id::new(id))
}

/// Generate an id on metasrv.
///
/// Ids are categorized by generators.
/// Ids may not be consecutive.
pub async fn fetch_id<T: kvapi::Key>(
    kv_api: &impl kvapi::KVApi<Error = MetaError>,
    generator: T,
) -> Result<u64, KVAppError> {
    let res = kv_api
        .upsert_kv(UpsertKVReq {
            key: generator.to_string_key(),
            seq: MatchSeq::GE(0),
            value: Operation::Update(b"".to_vec()),
            value_meta: None,
        })
        .await?;

    // seq: MatchSeq::Any always succeeds
    let seq_v = res.result.unwrap();
    Ok(seq_v.seq)
}

pub fn serialize_struct<T>(value: &T) -> Result<Vec<u8>, MetaNetworkError>
where
    T: FromToProto + 'static,
    T::PB: common_protos::prost::Message,
{
    let p = value.to_pb().map_err(|e| {
        let inv = InvalidArgument::new(e, "");
        MetaNetworkError::InvalidArgument(inv)
    })?;
    let mut buf = vec![];
    common_protos::prost::Message::encode(&p, &mut buf).map_err(|e| {
        let inv = InvalidArgument::new(e, "");
        MetaNetworkError::InvalidArgument(inv)
    })?;
    Ok(buf)
}

pub fn deserialize_struct<T>(buf: &[u8]) -> Result<T, MetaNetworkError>
where
    T: FromToProto,
    T::PB: common_protos::prost::Message + Default,
{
    let p: T::PB = common_protos::prost::Message::decode(buf).map_err(|e| {
        let inv = InvalidReply::new("", &e);
        MetaNetworkError::InvalidReply(inv)
    })?;
    let v: T = FromToProto::from_pb(p).map_err(|e| {
        let inv = InvalidReply::new("", &e);
        MetaNetworkError::InvalidReply(inv)
    })?;

    Ok(v)
}

pub async fn send_txn(
    kv_api: &(impl kvapi::KVApi<Error = MetaError> + ?Sized),
    txn_req: TxnRequest,
) -> Result<(bool, Vec<TxnOpResponse>), KVAppError> {
    let tx_reply = kv_api.transaction(txn_req).await?;
    let (succ, responses) = txn_reply_to_api_result(tx_reply)?;
    Ok((succ, responses))
}

/// Build a TxnCondition that compares the seq of a record.
pub fn txn_cond_seq(key: &impl kvapi::Key, op: ConditionResult, seq: u64) -> TxnCondition {
    TxnCondition {
        key: key.to_string_key(),
        expected: op as i32,
        target: Some(Target::Seq(seq)),
    }
}

/// Build a txn operation that puts a record.
pub fn txn_op_put(key: &impl kvapi::Key, value: Vec<u8>) -> TxnOp {
    TxnOp {
        request: Some(Request::Put(TxnPutRequest {
            key: key.to_string_key(),
            value,
            prev_value: true,
            expire_at: None,
        })),
    }
}

// TODO: replace it with common_meta_types::with::With
pub fn txn_op_put_with_expire(key: &impl kvapi::Key, value: Vec<u8>, expire_at: u64) -> TxnOp {
    TxnOp {
        request: Some(Request::Put(TxnPutRequest {
            key: key.to_string_key(),
            value,
            prev_value: true,
            expire_at: Some(expire_at),
        })),
    }
}

/// Build a txn operation that deletes a record.
pub fn txn_op_del(key: &impl kvapi::Key) -> TxnOp {
    TxnOp {
        request: Some(Request::Delete(TxnDeleteRequest {
            key: key.to_string_key(),
            prev_value: true,
        })),
    }
}

/// Return OK if a db_id or db_meta exists by checking the seq.
///
/// Otherwise returns UnknownDatabase error
pub fn db_has_to_exist(
    seq: u64,
    db_name_ident: &DatabaseNameIdent,
    msg: impl Display,
) -> Result<(), KVAppError> {
    if seq == 0 {
        debug!(seq, ?db_name_ident, "db does not exist");

        Err(KVAppError::AppError(AppError::UnknownDatabase(
            UnknownDatabase::new(
                &db_name_ident.db_name,
                format!("{}: {}", msg, db_name_ident),
            ),
        )))
    } else {
        Ok(())
    }
}

/// Return OK if a table_id or table_meta exists by checking the seq.
///
/// Otherwise returns UnknownTable error
pub fn table_has_to_exist(
    seq: u64,
    name_ident: &TableNameIdent,
    ctx: impl Display,
) -> Result<(), KVAppError> {
    if seq == 0 {
        debug!(seq, ?name_ident, "does not exist");

        Err(KVAppError::AppError(AppError::UnknownTable(
            UnknownTable::new(&name_ident.table_name, format!("{}: {}", ctx, name_ident)),
        )))
    } else {
        Ok(())
    }
}

// Return (share_id_seq, share_id, share_meta_seq, share_meta)
pub async fn get_share_or_err(
    kv_api: &(impl kvapi::KVApi<Error = MetaError> + ?Sized),
    name_key: &ShareNameIdent,
    msg: impl Display,
) -> Result<(u64, u64, u64, ShareMeta), KVAppError> {
    let (share_id_seq, share_id) = get_u64_value(kv_api, name_key).await?;
    share_has_to_exist(share_id_seq, name_key, &msg)?;

    let (share_meta_seq, share_meta) = get_share_meta_by_id_or_err(kv_api, share_id, msg).await?;

    Ok((share_id_seq, share_id, share_meta_seq, share_meta))
}

/// Returns (share_meta_seq, share_meta)
pub async fn get_share_meta_by_id_or_err(
    kv_api: &(impl kvapi::KVApi<Error = MetaError> + ?Sized),
    share_id: u64,
    msg: impl Display,
) -> Result<(u64, ShareMeta), KVAppError> {
    let id_key = ShareId { share_id };

    let (share_meta_seq, share_meta) = get_pb_value(kv_api, &id_key).await?;
    share_meta_has_to_exist(share_meta_seq, share_id, msg)?;

    Ok((share_meta_seq, share_meta.unwrap()))
}

fn share_meta_has_to_exist(seq: u64, share_id: u64, msg: impl Display) -> Result<(), KVAppError> {
    if seq == 0 {
        debug!(seq, ?share_id, "share meta does not exist");

        Err(KVAppError::AppError(AppError::UnknownShareId(
            UnknownShareId::new(share_id, format!("{}: {}", msg, share_id)),
        )))
    } else {
        Ok(())
    }
}

/// Return OK if a share_id or share_meta exists by checking the seq.
///
/// Otherwise returns UnknownShare error
fn share_has_to_exist(
    seq: u64,
    share_name_ident: &ShareNameIdent,
    msg: impl Display,
) -> Result<(), KVAppError> {
    if seq == 0 {
        debug!(seq, ?share_name_ident, "share does not exist");

        Err(KVAppError::AppError(AppError::UnknownShare(
            UnknownShare::new(
                &share_name_ident.share_name,
                format!("{}: {}", msg, share_name_ident),
            ),
        )))
    } else {
        Ok(())
    }
}

/// Returns (share_account_meta_seq, share_account_meta)
pub async fn get_share_account_meta_or_err(
    kv_api: &(impl kvapi::KVApi<Error = MetaError> + ?Sized),
    name_key: &ShareAccountNameIdent,
    msg: impl Display,
) -> Result<(u64, ShareAccountMeta), KVAppError> {
    let (share_account_meta_seq, share_account_meta): (u64, Option<ShareAccountMeta>) =
        get_pb_value(kv_api, name_key).await?;
    share_account_meta_has_to_exist(share_account_meta_seq, name_key, msg)?;

    Ok((
        share_account_meta_seq,
        // Safe unwrap(): share_meta_seq > 0 implies share_meta is not None.
        share_account_meta.unwrap(),
    ))
}

/// Return OK if a share_id or share_account_meta exists by checking the seq.
///
/// Otherwise returns UnknownShareAccounts error
fn share_account_meta_has_to_exist(
    seq: u64,
    name_key: &ShareAccountNameIdent,
    msg: impl Display,
) -> Result<(), KVAppError> {
    if seq == 0 {
        debug!(seq, ?name_key, "share account does not exist");

        Err(KVAppError::AppError(AppError::UnknownShareAccounts(
            UnknownShareAccounts::new(
                &[name_key.account.clone()],
                name_key.share_id,
                format!("{}: {}", msg, name_key),
            ),
        )))
    } else {
        Ok(())
    }
}

/// Returns (share_meta_seq, share_meta)
pub async fn get_share_id_to_name_or_err(
    kv_api: &(impl kvapi::KVApi<Error = MetaError> + ?Sized),
    share_id: u64,
    msg: impl Display,
) -> Result<(u64, ShareNameIdent), KVAppError> {
    let id_key = ShareIdToName { share_id };

    let (share_name_seq, share_name) = get_pb_value(kv_api, &id_key).await?;
    if share_name_seq == 0 {
        debug!(share_name_seq, ?share_id, "share meta does not exist");

        return Err(KVAppError::AppError(AppError::UnknownShareId(
            UnknownShareId::new(share_id, format!("{}: {}", msg, share_id)),
        )));
    }

    Ok((share_name_seq, share_name.unwrap()))
}

pub fn get_share_database_id_and_privilege(
    name_key: &ShareNameIdent,
    share_meta: &ShareMeta,
) -> Result<(u64, BitFlags<ShareGrantObjectPrivilege>), KVAppError> {
    if let Some(entry) = &share_meta.database {
        if let ShareGrantObject::Database(db_id) = entry.object {
            return Ok((db_id, entry.privileges));
        } else {
            unreachable!("database MUST be Database object");
        }
    }

    Err(KVAppError::AppError(AppError::ShareHasNoGrantedDatabase(
        ShareHasNoGrantedDatabase::new(&name_key.tenant, &name_key.share_name),
    )))
}

// Return true if all the database data has been removed.
pub async fn is_all_db_data_removed(
    kv_api: &(impl kvapi::KVApi<Error = MetaError> + ?Sized),
    db_id: u64,
) -> Result<bool, KVAppError> {
    let dbid = DatabaseId { db_id };

    let (db_meta_seq, db_meta): (_, Option<DatabaseMeta>) = get_pb_value(kv_api, &dbid).await?;
    debug_assert_eq!((db_meta_seq == 0), db_meta.is_none());
    if db_meta_seq != 0 {
        return Ok(false);
    }

    let id_to_name = DatabaseIdToName { db_id };
    let (name_ident_seq, name_ident): (_, Option<DatabaseNameIdent>) =
        get_pb_value(kv_api, &id_to_name).await?;
    debug_assert_eq!((name_ident_seq == 0), name_ident.is_none());
    if name_ident_seq != 0 {
        return Ok(false);
    }

    Ok(true)
}

// Return (true, `DataBaseMeta.from_share`) if the database needs to be removed, otherwise return (false, None).
// f: the predict function whether or not the database needs to be removed
//    base on the database meta passed by the user.
// When the database needs to be removed, add `TxnCondition` into `condition`
//    and `TxnOp` into the `if_then`.
pub async fn is_db_need_to_be_remove<F>(
    kv_api: &(impl kvapi::KVApi<Error = MetaError> + ?Sized),
    db_id: u64,
    mut f: F,
    condition: &mut Vec<TxnCondition>,
    if_then: &mut Vec<TxnOp>,
) -> Result<(bool, Option<ShareNameIdent>), KVAppError>
where
    F: FnMut(&DatabaseMeta) -> bool,
{
    let dbid = DatabaseId { db_id };

    let (db_meta_seq, db_meta): (_, Option<DatabaseMeta>) = get_pb_value(kv_api, &dbid).await?;
    if db_meta_seq == 0 {
        return Ok((false, None));
    }

    let id_to_name = DatabaseIdToName { db_id };
    let (name_ident_seq, _name_ident): (_, Option<DatabaseNameIdent>) =
        get_pb_value(kv_api, &id_to_name).await?;
    if name_ident_seq == 0 {
        return Ok((false, None));
    }

    if let Some(db_meta) = db_meta {
        if f(&db_meta) {
            condition.push(txn_cond_seq(&dbid, Eq, db_meta_seq));
            if_then.push(txn_op_del(&dbid));
            condition.push(txn_cond_seq(&id_to_name, Eq, name_ident_seq));
            if_then.push(txn_op_del(&id_to_name));

            return Ok((true, db_meta.from_share));
        }
    }
    Ok((false, None))
}

pub async fn get_object_shared_by_share_ids(
    kv_api: &(impl kvapi::KVApi<Error = MetaError> + ?Sized),
    object: &ShareGrantObject,
) -> Result<(u64, ObjectSharedByShareIds), KVAppError> {
    let (seq, share_ids): (u64, Option<ObjectSharedByShareIds>) =
        get_pb_value(kv_api, object).await?;

    match share_ids {
        Some(share_ids) => Ok((seq, share_ids)),
        None => Ok((0, ObjectSharedByShareIds::default())),
    }
}
