/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */

#include "redis_hash.h"

#include <rocksdb/status.h>

#include <algorithm>
#include <cctype>
#include <cmath>
#include <random>
#include <utility>

#include "db_util.h"
#include "parse_util.h"
#include "sample_helper.h"
#include "time_util.h"

namespace redis {

rocksdb::Status Hash::GetMetadata(engine::Context &ctx, const Slice &ns_key, HashMetadata *metadata) {
  return Database::GetMetadata(ctx, {kRedisHash}, ns_key, metadata);
}

rocksdb::Status Hash::Size(engine::Context &ctx, const Slice &user_key, uint64_t *size) {
  *size = 0;

  std::string ns_key = AppendNamespacePrefix(user_key);
  HashMetadata metadata(false);
  rocksdb::Status s = GetMetadata(ctx, ns_key, &metadata);
  if (!s.ok()) return s;
  *size = metadata.size;
  return rocksdb::Status::OK();
}

rocksdb::Status Hash::Get(engine::Context &ctx, const Slice &user_key, const Slice &field, std::string *value) {
  std::string ns_key = AppendNamespacePrefix(user_key);
  HashMetadata metadata(false);
  rocksdb::Status s = GetMetadata(ctx, ns_key, &metadata);
  if (!s.ok()) return s;
  if (metadata.generation > 0 && field == "__gen") {
    *value = std::to_string(metadata.generation - 1);
    return rocksdb::Status::OK();
  }
  rocksdb::ReadOptions read_options;
  std::string sub_key = InternalKey(ns_key, field, metadata.version, storage_->IsSlotIdEncoded()).Encode();
  return storage_->Get(ctx, ctx.GetReadOptions(), sub_key, value);
}

rocksdb::Status Hash::IncrBy(engine::Context &ctx, const Slice &user_key, const Slice &field, int64_t increment,
                             int64_t *new_value) {
  bool exists = false;
  int64_t old_value = 0;

  std::string ns_key = AppendNamespacePrefix(user_key);

  LockGuard guard(storage_->GetLockManager(), ns_key);
  HashMetadata metadata;
  rocksdb::Status s = GetMetadata(ctx, ns_key, &metadata);
  if (!s.ok() && !s.IsNotFound()) return s;

  std::string sub_key = InternalKey(ns_key, field, metadata.version, storage_->IsSlotIdEncoded()).Encode();
  if (s.ok()) {
    std::string value_bytes;
    s = storage_->Get(ctx, ctx.GetReadOptions(), sub_key, &value_bytes);
    if (!s.ok() && !s.IsNotFound()) return s;
    if (s.ok()) {
      auto parse_result = ParseInt<int64_t>(value_bytes, 10);
      if (!parse_result) {
        return rocksdb::Status::InvalidArgument(parse_result.Msg());
      }
      if (isspace(value_bytes[0])) {
        return rocksdb::Status::InvalidArgument("value is not an integer");
      }
      old_value = *parse_result;
      exists = true;
    }
  }
  if ((increment < 0 && old_value < 0 && increment < (LLONG_MIN - old_value)) ||
      (increment > 0 && old_value > 0 && increment > (LLONG_MAX - old_value))) {
    return rocksdb::Status::InvalidArgument("increment or decrement would overflow");
  }

  *new_value = old_value + increment;
  auto batch = storage_->GetWriteBatchBase();
  WriteBatchLogData log_data(kRedisHash);
  s = batch->PutLogData(log_data.Encode());
  if (!s.ok()) return s;
  s = batch->Put(sub_key, std::to_string(*new_value));
  if (!s.ok()) return s;
  if (!exists) {
    metadata.size += 1;
    std::string bytes;
    metadata.Encode(&bytes);
    s = batch->Put(metadata_cf_handle_, ns_key, bytes);
    if (!s.ok()) return s;
  }
  return storage_->Write(ctx, storage_->DefaultWriteOptions(), batch->GetWriteBatch());
}

rocksdb::Status Hash::IncrByFloat(engine::Context &ctx, const Slice &user_key, const Slice &field, double increment,
                                  double *new_value) {
  bool exists = false;
  double old_value = 0;

  std::string ns_key = AppendNamespacePrefix(user_key);

  LockGuard guard(storage_->GetLockManager(), ns_key);
  HashMetadata metadata;
  rocksdb::Status s = GetMetadata(ctx, ns_key, &metadata);
  if (!s.ok() && !s.IsNotFound()) return s;

  std::string sub_key = InternalKey(ns_key, field, metadata.version, storage_->IsSlotIdEncoded()).Encode();
  if (s.ok()) {
    std::string value_bytes;
    s = storage_->Get(ctx, ctx.GetReadOptions(), sub_key, &value_bytes);
    if (!s.ok() && !s.IsNotFound()) return s;
    if (s.ok()) {
      auto value_stat = ParseFloat(value_bytes);
      if (!value_stat || isspace(value_bytes[0])) {
        return rocksdb::Status::InvalidArgument("value is not a number");
      }
      old_value = *value_stat;
      exists = true;
    }
  }
  double n = old_value + increment;
  if (std::isinf(n) || std::isnan(n)) {
    return rocksdb::Status::InvalidArgument("increment would produce NaN or Infinity");
  }

  *new_value = n;
  auto batch = storage_->GetWriteBatchBase();
  WriteBatchLogData log_data(kRedisHash);
  s = batch->PutLogData(log_data.Encode());
  if (!s.ok()) return s;
  s = batch->Put(sub_key, std::to_string(*new_value));
  if (!s.ok()) return s;
  if (!exists) {
    metadata.size += 1;
    std::string bytes;
    metadata.Encode(&bytes);
    s = batch->Put(metadata_cf_handle_, ns_key, bytes);
    if (!s.ok()) return s;
  }
  return storage_->Write(ctx, storage_->DefaultWriteOptions(), batch->GetWriteBatch());
}

rocksdb::Status Hash::MGet(engine::Context &ctx, const Slice &user_key, const std::vector<Slice> &fields,
                           std::vector<std::string> *values, std::vector<rocksdb::Status> *statuses) {
  values->clear();
  statuses->clear();

  std::string ns_key = AppendNamespacePrefix(user_key);
  HashMetadata metadata(false);
  rocksdb::Status s = GetMetadata(ctx, ns_key, &metadata);
  if (!s.ok()) {
    return s;
  }

  rocksdb::ReadOptions read_options = ctx.DefaultMultiGetOptions();
  std::vector<rocksdb::Slice> keys;

  keys.reserve(fields.size());
  std::vector<std::string> sub_keys;
  sub_keys.resize(fields.size());
  std::vector<size_t> key_indices;
  key_indices.reserve(fields.size());
  for (size_t i = 0; i < fields.size(); i++) {
    auto &field = fields[i];
    if (metadata.generation > 0 && field == "__gen") {
      continue;
    }
    sub_keys[i] = InternalKey(ns_key, field, metadata.version, storage_->IsSlotIdEncoded()).Encode();
    keys.emplace_back(sub_keys[i]);
    key_indices.push_back(i);
  }

  std::vector<rocksdb::PinnableSlice> values_vector;
  values_vector.resize(keys.size());
  std::vector<rocksdb::Status> statuses_vector;
  statuses_vector.resize(keys.size());
  if (!keys.empty()) {
    storage_->MultiGet(ctx, read_options, storage_->GetDB()->DefaultColumnFamily(), keys.size(), keys.data(),
                       values_vector.data(), statuses_vector.data());
  }

  values->resize(fields.size());
  statuses->resize(fields.size(), rocksdb::Status::OK());
  if (metadata.generation > 0) {
    std::string gen_str = std::to_string(metadata.generation - 1);
    for (size_t i = 0; i < fields.size(); i++) {
      if (fields[i] == "__gen") {
        (*values)[i] = gen_str;
        (*statuses)[i] = rocksdb::Status::OK();
      }
    }
  }
  for (size_t k = 0; k < keys.size(); k++) {
    if (!statuses_vector[k].ok() && !statuses_vector[k].IsNotFound()) return statuses_vector[k];
    size_t idx = key_indices[k];
    (*values)[idx] = values_vector[k].ToString();
    (*statuses)[idx] = statuses_vector[k];
  }
  return rocksdb::Status::OK();
}

rocksdb::Status Hash::Set(engine::Context &ctx, const Slice &user_key, const Slice &field, const Slice &value,
                          uint64_t *added_cnt) {
  return MSet(ctx, user_key, {{field.ToString(), value.ToString()}}, false, added_cnt);
}

rocksdb::Status Hash::Delete(engine::Context &ctx, const Slice &user_key, const std::vector<Slice> &fields,
                             uint64_t *deleted_cnt) {
  *deleted_cnt = 0;
  std::string ns_key = AppendNamespacePrefix(user_key);

  HashMetadata metadata(false);
  auto batch = storage_->GetWriteBatchBase();
  WriteBatchLogData log_data(kRedisHash);
  auto s = batch->PutLogData(log_data.Encode());
  if (!s.ok()) return s;
  LockGuard guard(storage_->GetLockManager(), ns_key);
  s = GetMetadata(ctx, ns_key, &metadata);
  if (!s.ok()) return s.IsNotFound() ? rocksdb::Status::OK() : s;

  std::string value;
  std::unordered_set<std::string_view> field_set;
  for (const auto &field : fields) {
    if (!field_set.emplace(field.ToStringView()).second) {
      continue;
    }
    std::string sub_key = InternalKey(ns_key, field, metadata.version, storage_->IsSlotIdEncoded()).Encode();
    s = storage_->Get(ctx, ctx.GetReadOptions(), sub_key, &value);
    if (s.ok()) {
      *deleted_cnt += 1;
      s = batch->Delete(sub_key);
      if (!s.ok()) return s;
    }
  }
  if (*deleted_cnt == 0) {
    return rocksdb::Status::OK();
  }
  metadata.size -= *deleted_cnt;
  std::string bytes;
  metadata.Encode(&bytes);
  s = batch->Put(metadata_cf_handle_, ns_key, bytes);
  if (!s.ok()) return s;
  return storage_->Write(ctx, storage_->DefaultWriteOptions(), batch->GetWriteBatch());
}

rocksdb::Status Hash::MSet(engine::Context &ctx, const Slice &user_key, const std::vector<FieldValue> &field_values,
                           bool nx, uint64_t *added_cnt) {
  *added_cnt = 0;
  std::string ns_key = AppendNamespacePrefix(user_key);

  LockGuard guard(storage_->GetLockManager(), ns_key);
  HashMetadata metadata;
  rocksdb::Status s = GetMetadata(ctx, ns_key, &metadata);
  if (!s.ok() && !s.IsNotFound()) return s;

  int added = 0;
  bool meta_updated = false;
  auto batch = storage_->GetWriteBatchBase();
  WriteBatchLogData log_data(kRedisHash);
  s = batch->PutLogData(log_data.Encode());
  if (!s.ok()) return s;
  std::unordered_set<std::string_view> field_set;
  for (auto it = field_values.rbegin(); it != field_values.rend(); it++) {
    if (!field_set.insert(it->field).second) {
      continue;
    }

    if (it->field == "__gen") {
      auto parsed = ParseInt<int64_t>(it->value, 10);
      if (parsed && *parsed >= 0) {
        uint64_t new_gen = static_cast<uint64_t>(*parsed) + 1;
        if (metadata.generation == 0) {
          added++;
          metadata.generation = new_gen;
          meta_updated = true;
        } else if (!nx && metadata.generation != new_gen) {
          metadata.generation = new_gen;
          meta_updated = true;
        }
        continue;
      }
    }

    bool exists = false;
    std::string sub_key = InternalKey(ns_key, it->field, metadata.version, storage_->IsSlotIdEncoded()).Encode();

    if (metadata.size > 0) {
      std::string field_value;
      s = storage_->Get(ctx, ctx.GetReadOptions(), sub_key, &field_value);
      if (!s.ok() && !s.IsNotFound()) return s;

      if (s.ok()) {
        if (nx || field_value == it->value) continue;

        exists = true;
      }
    }

    if (!exists) added++;

    s = batch->Put(sub_key, it->value);
    if (!s.ok()) return s;
  }

  if (added > 0 || meta_updated) {
    *added_cnt = added;
    metadata.size += added;
    std::string bytes;
    metadata.Encode(&bytes);
    s = batch->Put(metadata_cf_handle_, ns_key, bytes);
    if (!s.ok()) return s;
  }

  return storage_->Write(ctx, storage_->DefaultWriteOptions(), batch->GetWriteBatch());
}

rocksdb::Status Hash::MSetCAS(engine::Context &ctx, const Slice &user_key, const std::vector<FieldValue> &field_values,
                              HashCasMode mode, int64_t expect_gen, uint64_t ttl_ms, int64_t *ret_gen) {
  *ret_gen = -1;
  std::string ns_key = AppendNamespacePrefix(user_key);

  LockGuard guard(storage_->GetLockManager(), ns_key);
  HashMetadata metadata(false);
  rocksdb::Status s = GetMetadata(ctx, ns_key, &metadata);
  if (!s.ok() && !s.IsNotFound()) return s;

  bool has_old = s.ok();
  if (mode == HashCasMode::kCreateOnly && has_old) {
    *ret_gen = -3;
    return rocksdb::Status::OK();
  }
  if (mode == HashCasMode::kUpdateOnly && !has_old) {
    *ret_gen = -2;
    return rocksdb::Status::OK();
  }

  int64_t cur_gen = 0;
  if (has_old) {
    if (metadata.generation > 0) {
      cur_gen = static_cast<int64_t>(metadata.generation - 1);
    } else {
      std::string gen_bytes;
      std::string gen_sub_key = InternalKey(ns_key, "__gen", metadata.version, storage_->IsSlotIdEncoded()).Encode();
      auto gs = storage_->Get(ctx, ctx.GetReadOptions(), gen_sub_key, &gen_bytes);
      if (gs.ok()) {
        auto parsed = ParseInt<int64_t>(gen_bytes, 10);
        if (parsed) cur_gen = *parsed;
      }
    }
  }

  if (expect_gen >= 0 && (!has_old || cur_gen != expect_gen)) {
    *ret_gen = -1;
    return rocksdb::Status::OK();
  }

  bool wipe_existing = (!has_old || mode == HashCasMode::kCreateOnly || mode == HashCasMode::kReplace ||
                        mode == HashCasMode::kInsertReplace);
  int64_t base_gen = 0;
  if (has_old && mode != HashCasMode::kInsertReplace && mode != HashCasMode::kCreateOnly) {
    base_gen = cur_gen;
  }
  int64_t final_gen = (expect_gen >= 0) ? (base_gen + 1) : base_gen;

  if (wipe_existing) {
    metadata = HashMetadata(true);
  }

  int added = 0;
  if (metadata.generation == 0) {
    added++;
  }
  metadata.generation = static_cast<uint64_t>(final_gen) + 1;
  if (ttl_ms > 0) {
    metadata.expire = util::GetTimeStampMS() + ttl_ms;
  }

  auto batch = storage_->GetWriteBatchBase();
  WriteBatchLogData log_data(kRedisHash);
  s = batch->PutLogData(log_data.Encode());
  if (!s.ok()) return s;

  std::unordered_set<std::string_view> field_set;
  for (auto it = field_values.rbegin(); it != field_values.rend(); it++) {
    if (it->field == "__gen" || !field_set.insert(it->field).second) {
      continue;
    }

    bool exists = false;
    std::string sub_key = InternalKey(ns_key, it->field, metadata.version, storage_->IsSlotIdEncoded()).Encode();

    if (!wipe_existing && metadata.size > 0) {
      std::string field_value;
      s = storage_->Get(ctx, ctx.GetReadOptions(), sub_key, &field_value);
      if (!s.ok() && !s.IsNotFound()) return s;
      if (s.ok()) {
        if (field_value == it->value) continue;
        exists = true;
      }
    }

    if (!exists) added++;

    s = batch->Put(sub_key, it->value);
    if (!s.ok()) return s;
  }

  metadata.size += added;
  std::string bytes;
  metadata.Encode(&bytes);
  s = batch->Put(metadata_cf_handle_, ns_key, bytes);
  if (!s.ok()) return s;

  s = storage_->Write(ctx, storage_->DefaultWriteOptions(), batch->GetWriteBatch());
  if (!s.ok()) return s;

  *ret_gen = final_gen;
  return rocksdb::Status::OK();
}

rocksdb::Status Hash::RangeByLex(engine::Context &ctx, const Slice &user_key, const RangeLexSpec &spec,
                                 std::vector<FieldValue> *field_values) {
  field_values->clear();
  if (spec.count == 0) {
    return rocksdb::Status::OK();
  }
  std::string ns_key = AppendNamespacePrefix(user_key);
  HashMetadata metadata(false);
  rocksdb::Status s = GetMetadata(ctx, ns_key, &metadata);
  if (!s.ok()) return s.IsNotFound() ? rocksdb::Status::OK() : s;

  std::string start_member = spec.reversed ? spec.max : spec.min;
  std::string start_key = InternalKey(ns_key, start_member, metadata.version, storage_->IsSlotIdEncoded()).Encode();
  std::string prefix_key = InternalKey(ns_key, "", metadata.version, storage_->IsSlotIdEncoded()).Encode();
  std::string next_version_prefix_key =
      InternalKey(ns_key, "", metadata.version + 1, storage_->IsSlotIdEncoded()).Encode();
  rocksdb::ReadOptions read_options = ctx.DefaultScanOptions();
  rocksdb::Slice upper_bound(next_version_prefix_key);
  read_options.iterate_upper_bound = &upper_bound;
  rocksdb::Slice lower_bound(prefix_key);
  read_options.iterate_lower_bound = &lower_bound;

  auto iter = util::UniqueIterator(ctx, read_options);
  if (!spec.reversed) {
    iter->Seek(start_key);
  } else {
    if (spec.max_infinite) {
      iter->SeekToLast();
    } else {
      iter->SeekForPrev(start_key);
    }
  }
  int64_t pos = 0;
  for (; iter->Valid() && iter->key().starts_with(prefix_key); (!spec.reversed ? iter->Next() : iter->Prev())) {
    InternalKey ikey(iter->key(), storage_->IsSlotIdEncoded());
    if (spec.reversed) {
      if (ikey.GetSubKey().ToString() < spec.min || (spec.minex && ikey.GetSubKey().ToString() == spec.min)) {
        break;
      }
      if ((spec.maxex && ikey.GetSubKey().ToString() == spec.max) ||
          (!spec.max_infinite && ikey.GetSubKey().ToString() > spec.max)) {
        continue;
      }
    } else {
      if (spec.minex && ikey.GetSubKey().ToString() == spec.min) continue;  // the min member was exclusive
      if ((spec.maxex && ikey.GetSubKey().ToString() == spec.max) ||
          (!spec.max_infinite && ikey.GetSubKey().ToString() > spec.max))
        break;
    }
    if (spec.offset >= 0 && pos++ < spec.offset) continue;

    field_values->emplace_back(ikey.GetSubKey().ToString(), iter->value().ToString());
    if (spec.count > 0 && field_values->size() >= static_cast<unsigned>(spec.count)) break;
  }
  return rocksdb::Status::OK();
}

rocksdb::Status Hash::GetAll(engine::Context &ctx, const Slice &user_key, std::vector<FieldValue> *field_values,
                             HashFetchType type) {
  field_values->clear();

  std::string ns_key = AppendNamespacePrefix(user_key);
  HashMetadata metadata(false);
  rocksdb::Status s = GetMetadata(ctx, ns_key, &metadata);
  if (!s.ok()) return s.IsNotFound() ? rocksdb::Status::OK() : s;

  if (metadata.generation > 0) {
    if (type == HashFetchType::kOnlyKey) {
      field_values->emplace_back("__gen", "");
    } else if (type == HashFetchType::kOnlyValue) {
      field_values->emplace_back("", std::to_string(metadata.generation - 1));
    } else {
      field_values->emplace_back("__gen", std::to_string(metadata.generation - 1));
    }
  }

  std::string prefix_key = InternalKey(ns_key, "", metadata.version, storage_->IsSlotIdEncoded()).Encode();
  std::string next_version_prefix_key =
      InternalKey(ns_key, "", metadata.version + 1, storage_->IsSlotIdEncoded()).Encode();

  rocksdb::ReadOptions read_options = ctx.DefaultScanOptions();
  rocksdb::Slice upper_bound(next_version_prefix_key);
  read_options.iterate_upper_bound = &upper_bound;

  auto iter = util::UniqueIterator(ctx, read_options);
  for (iter->Seek(prefix_key); iter->Valid() && iter->key().starts_with(prefix_key); iter->Next()) {
    InternalKey ikey(iter->key(), storage_->IsSlotIdEncoded());
    if (metadata.generation > 0 && ikey.GetSubKey() == "__gen") {
      continue;
    }
    if (type == HashFetchType::kOnlyKey) {
      field_values->emplace_back(ikey.GetSubKey().ToString(), "");
    } else if (type == HashFetchType::kOnlyValue) {
      field_values->emplace_back("", iter->value().ToString());
    } else {
      field_values->emplace_back(ikey.GetSubKey().ToString(), iter->value().ToString());
    }
  }
  return rocksdb::Status::OK();
}

rocksdb::Status Hash::Scan(engine::Context &ctx, const Slice &user_key, const std::string &cursor, uint64_t limit,
                           const std::string &field_prefix, std::vector<std::string> *fields,
                           std::vector<std::string> *values) {
  return SubKeyScanner::Scan(ctx, kRedisHash, user_key, cursor, limit, field_prefix, fields, values);
}

rocksdb::Status Hash::RandField(engine::Context &ctx, const Slice &user_key, int64_t command_count,
                                std::vector<FieldValue> *field_values, HashFetchType type) {
  uint64_t count = (command_count >= 0) ? static_cast<uint64_t>(command_count) : static_cast<uint64_t>(-command_count);
  bool unique = (command_count >= 0);

  std::string ns_key = AppendNamespacePrefix(user_key);
  HashMetadata metadata(/*generate_version=*/false);
  rocksdb::Status s = GetMetadata(ctx, ns_key, &metadata);
  if (!s.ok()) return s;

  std::vector<FieldValue> samples;
  // TODO: Getting all values in Hash might be heavy, consider lazy-loading these values later
  if (count == 0) return rocksdb::Status::OK();
  s = ExtractRandMemberFromSet<FieldValue>(
      unique, count,
      [this, user_key, type, &ctx](std::vector<FieldValue> *elements) {
        return this->GetAll(ctx, user_key, elements, type);
      },
      field_values);
  if (!s.ok()) {
    return s;
  }
  switch (type) {
    case HashFetchType::kAll:
      break;
    case HashFetchType::kOnlyKey: {
      // GetAll should only fetching the key, checking all the values is empty
      for (const FieldValue &value : *field_values) {
        DCHECK(value.value.empty());
      }
      break;
    }
    case HashFetchType::kOnlyValue:
      // Unreachable.
      DCHECK(false);
      break;
  }
  return rocksdb::Status::OK();
}

}  // namespace redis
