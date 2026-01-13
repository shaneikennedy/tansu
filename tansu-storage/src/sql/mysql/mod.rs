// Copyright ⓒ 2024-2025 Peter Morgan <peter.james.morgan@gmail.com>
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::{collections::BTreeMap, sync::LazyLock};

use crate::sql::{Cache, remove_comments};

macro_rules! include_sql {
    ($e: expr) => {
        remove_comments(include_str!($e))
    };
}

pub(crate) static SQL: LazyLock<Cache> = LazyLock::new(|| {
    let mapping = [
        ("cluster_insert", include_sql!("cluster_insert.sql")),
        (
            "consumer_group_delete",
            include_sql!("consumer_group_delete.sql"),
        ),
        (
            "consumer_group_detail_delete",
            include_sql!("consumer_group_detail_delete.sql"),
        ),
        (
            "consumer_group_insert",
            include_sql!("consumer_group_insert.sql"),
        ),
        (
            "consumer_group_select_all",
            include_sql!("consumer_group_select_all.sql"),
        ),
        (
            "consumer_group_select_for_update",
            include_sql!("consumer_group_select_for_update.sql"),
        ),
        (
            "consumer_group_select",
            include_sql!("consumer_group_select.sql"),
        ),
        (
            "consumer_group_update",
            include_sql!("consumer_group_update.sql"),
        ),
        (
            "consumer_offset_delete_by_group",
            include_sql!("consumer_offset_delete_by_group.sql"),
        ),
        (
            "consumer_offset_insert_from_txn",
            include_sql!("consumer_offset_insert_from_txn.sql"),
        ),
        (
            "consumer_offset_insert",
            include_sql!("consumer_offset_insert.sql"),
        ),
        (
            "consumer_offset_select_by_group",
            include_sql!("consumer_offset_select_by_group.sql"),
        ),
        (
            "consumer_offset_select",
            include_sql!("consumer_offset_select.sql"),
        ),
        (
            "header_delete_by_topic",
            include_sql!("header_delete_by_topic.sql"),
        ),
        ("header_fetch", include_sql!("header_fetch.sql")),
        ("header_insert", include_sql!("header_insert.sql")),
        (
            "producer_detail_insert",
            include_sql!("producer_detail_insert.sql"),
        ),
        (
            "producer_detail_select_for_update",
            include_sql!("producer_detail_select_for_update.sql"),
        ),
        (
            "producer_epoch_insert",
            include_sql!("producer_epoch_insert.sql"),
        ),
        (
            "producer_epoch_select_current",
            include_sql!("producer_epoch_select_current.sql"),
        ),
        ("producer_insert", include_sql!("producer_insert.sql")),
        (
            "record_delete_by_topic",
            include_sql!("record_delete_by_topic.sql"),
        ),
        (
            "record_delete_from_offset",
            include_sql!("record_delete_from_offset.sql"),
        ),
        (
            "record_fetch_by_timestamp",
            include_sql!("record_fetch_by_timestamp.sql"),
        ),
        ("record_fetch", include_sql!("record_fetch.sql")),
        ("record_insert", include_sql!("record_insert.sql")),
        ("record_min_offset", include_sql!("record_min_offset.sql")),
        (
            "topic_configuration_delete_by_topic",
            include_sql!("topic_configuration_delete_by_topic.sql"),
        ),
        (
            "topic_configuration_delete",
            include_sql!("topic_configuration_delete.sql"),
        ),
        (
            "topic_configuration_select",
            include_sql!("topic_configuration_select.sql"),
        ),
        (
            "topic_configuration_upsert",
            include_sql!("topic_configuration_upsert.sql"),
        ),
        ("topic_delete", include_sql!("topic_delete.sql")),
        ("topic_insert", include_sql!("topic_insert.sql")),
        ("topic_select_all", include_sql!("topic_select_all.sql")),
        (
            "topic_select_by_name",
            include_sql!("topic_select_by_name.sql"),
        ),
        (
            "topic_select_full_by_uuid",
            include_sql!("topic_select_full_by_uuid.sql"),
        ),
        ("topic_select_full", include_sql!("topic_select_full.sql")),
        (
            "topic_select_name_partitions",
            include_sql!("topic_select_name_partitions.sql"),
        ),
        ("topic_select_uuid", include_sql!("topic_select_uuid.sql")),
        (
            "topition_delete_by_topic",
            include_sql!("topition_delete_by_topic.sql"),
        ),
        ("topition_exists", include_sql!("topition_exists.sql")),
        ("topition_insert", include_sql!("topition_insert.sql")),
        ("topition_select_id", include_sql!("topition_select_id.sql")),
        ("txn_insert", include_sql!("txn_insert.sql")),
        ("txn_offset_add", include_sql!("txn_offset_add.sql")),
        (
            "txn_offset_commit_delete",
            include_sql!("txn_offset_commit_delete.sql"),
        ),
        (
            "txn_offset_commit_insert",
            include_sql!("txn_offset_commit_insert.sql"),
        ),
        (
            "txn_offset_commit_tp_delete",
            include_sql!("txn_offset_commit_tp_delete.sql"),
        ),
        (
            "txn_offset_commit_tp_insert",
            include_sql!("txn_offset_commit_tp_insert.sql"),
        ),
        (
            "txn_produce_offset_delete",
            include_sql!("txn_produce_offset_delete.sql"),
        ),
        (
            "txn_produce_offset_insert",
            include_sql!("txn_produce_offset_insert.sql"),
        ),
        (
            "txn_produce_offset_select_overlapping",
            include_sql!("txn_produce_offset_select_overlapping.sql"),
        ),
        (
            "txn_produce_offset_select_range",
            include_sql!("txn_produce_offset_select_range.sql"),
        ),
        (
            "txn_select_for_offset",
            include_sql!("txn_select_for_offset.sql"),
        ),
        ("txn_status_update", include_sql!("txn_status_update.sql")),
        (
            "txn_topition_delete",
            include_sql!("txn_topition_delete.sql"),
        ),
        (
            "txn_topition_insert",
            include_sql!("txn_topition_insert.sql"),
        ),
        (
            "txn_topition_select",
            include_sql!("txn_topition_select.sql"),
        ),
        (
            "watermark_delete_by_topic",
            include_sql!("watermark_delete_by_topic.sql"),
        ),
        ("watermark_insert", include_sql!("watermark_insert.sql")),
        (
            "watermark_select_for_update",
            include_sql!("watermark_select_for_update.sql"),
        ),
        ("watermark_select", include_sql!("watermark_select.sql")),
        ("watermark_update", include_sql!("watermark_update.sql")),
    ];

    Cache::new(BTreeMap::from(mapping))
});
