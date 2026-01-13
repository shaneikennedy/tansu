// Copyright ⓒ 2024-2026 Peter Morgan <peter.james.morgan@gmail.com>
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

use std::{collections::BTreeMap, time::Duration};

use chrono::NaiveDateTime;

use async_trait::async_trait;
use bytes::Bytes;
use mysql_async::TxOpts;
use rand::{prelude::*, rng};
use serde_json::Value;
use tansu_sans_io::{
    ConfigResource, ConfigSource, ConfigType, ErrorCode, IsolationLevel, ListOffset, NULL_TOPIC_ID,
    OpType,
    add_partitions_to_txn_response::{
        AddPartitionsToTxnPartitionResult, AddPartitionsToTxnTopicResult,
    },
    create_topics_request::CreatableTopic,
    delete_groups_response::DeletableGroupResult,
    delete_records_request::DeleteRecordsTopic,
    delete_records_response::{DeleteRecordsPartitionResult, DeleteRecordsTopicResult},
    describe_cluster_response::DescribeClusterBroker,
    describe_configs_response::{DescribeConfigsResourceResult, DescribeConfigsResult},
    describe_topic_partitions_response::{
        DescribeTopicPartitionsResponsePartition, DescribeTopicPartitionsResponseTopic,
    },
    incremental_alter_configs_request::AlterConfigsResource,
    incremental_alter_configs_response::AlterConfigsResourceResponse,
    list_groups_response::ListedGroup,
    metadata_response::{MetadataResponseBroker, MetadataResponsePartition, MetadataResponseTopic},
    record::{Header, Record, deflated, inflated::Batch},
    txn_offset_commit_response::{TxnOffsetCommitResponsePartition, TxnOffsetCommitResponseTopic},
};
use tracing::{debug, error, instrument};
use url::Url;
use uuid::Uuid;

use crate::{
    BrokerRegistrationRequest, Error, GroupDetail, ListOffsetResponse, MetadataResponse,
    NamedGroupDetail, OffsetCommitRequest, OffsetStage, ProducerIdResponse, Result, Storage,
    TopicId, Topition, TxnAddPartitionsRequest, TxnAddPartitionsResponse, TxnOffsetCommitRequest,
    TxnState, UpdateError, Version,
};

use crate::mysql::Mysql;

#[async_trait]
impl Storage for Mysql {
    #[instrument(skip_all)]
    async fn register_broker(&self, broker_registration: BrokerRegistrationRequest) -> Result<()> {
        debug!(cluster = self.cluster, ?broker_registration);

        let mut conn = self.connection().await?;

        let _ = self
            .prepare_execute(
                &mut conn,
                "cluster_insert",
                (broker_registration.cluster_id.as_str(),),
            )
            .await?;

        Ok(())
    }

    #[instrument(skip_all)]
    async fn brokers(&self) -> Result<Vec<DescribeClusterBroker>> {
        debug!(cluster = self.cluster);

        let broker_id = self.node;
        let host = self
            .advertised_listener
            .host_str()
            .unwrap_or("0.0.0.0")
            .into();
        let port = self.advertised_listener.port().unwrap_or(9092).into();
        let rack = None;

        Ok(vec![
            DescribeClusterBroker::default()
                .broker_id(broker_id)
                .host(host)
                .port(port)
                .rack(rack),
        ])
    }

    #[instrument(skip_all)]
    async fn create_topic(&self, topic: CreatableTopic, validate_only: bool) -> Result<Uuid> {
        debug!(cluster = self.cluster, ?topic, validate_only);

        let mut conn = self.connection().await?;
        let mut tx = conn.start_transaction(TxOpts::default()).await?;

        let uuid = Uuid::new_v4();

        let existing: Option<String> = self
            .tx_prepare_query_opt(
                &mut tx,
                "topic_select_by_name",
                (self.cluster.as_str(), topic.name.as_str()),
            )
            .await?;

        if existing.is_some() {
            return Err(Error::Api(ErrorCode::TopicAlreadyExists));
        }

        let _ = self
            .tx_prepare_execute(
                &mut tx,
                "topic_insert",
                (
                    self.cluster.as_str(),
                    topic.name.as_str(),
                    uuid.to_string(),
                    topic.num_partitions,
                    topic.replication_factor as i32,
                ),
            )
            .await
            .inspect_err(|err| error!(?err, ?topic, ?validate_only))?;

        debug!(?uuid, cluster = self.cluster, ?topic);

        for partition in 0..topic.num_partitions {
            let _ = self
                .tx_prepare_execute(
                    &mut tx,
                    "topition_insert",
                    (self.cluster.as_str(), topic.name.as_str(), partition),
                )
                .await?;

            let _ = self
                .tx_prepare_execute(
                    &mut tx,
                    "watermark_insert",
                    (self.cluster.as_str(), topic.name.as_str(), partition),
                )
                .await?;
        }

        if let Some(configs) = topic.configs {
            for config in configs {
                debug!(?config);

                let _ = self
                    .tx_prepare_execute(
                        &mut tx,
                        "topic_configuration_upsert",
                        (
                            self.cluster.as_str(),
                            topic.name.as_str(),
                            config.name.as_str(),
                            config.value.as_deref(),
                        ),
                    )
                    .await
                    .inspect_err(|err| error!(?err, ?config))?;
            }
        }

        tx.commit().await.inspect_err(|err| error!(?err))?;

        Ok(uuid)
    }

    #[instrument(skip_all)]
    async fn delete_records(
        &self,
        topics: &[DeleteRecordsTopic],
    ) -> Result<Vec<DeleteRecordsTopicResult>> {
        debug!(cluster = self.cluster, ?topics);

        let mut conn = self.connection().await?;

        let mut responses = vec![];

        for topic in topics {
            let mut partition_responses = vec![];

            if let Some(ref partitions) = topic.partitions {
                for partition in partitions {
                    let _ = self
                        .prepare_execute(
                            &mut conn,
                            "record_delete_from_offset",
                            (
                                self.cluster.as_str(),
                                topic.name.as_str(),
                                partition.partition_index,
                                partition.offset,
                            ),
                        )
                        .await
                        .inspect_err(|err| {
                            let cluster = self.cluster.as_str();
                            let topic = topic.name.as_str();
                            let partition_index = partition.partition_index;
                            let offset = partition.offset;

                            error!(?err, ?cluster, ?topic, ?partition_index, ?offset)
                        })?;

                    let low_watermark: Option<i64> = self
                        .prepare_query_opt(
                            &mut conn,
                            "record_min_offset",
                            (
                                self.cluster.as_str(),
                                topic.name.as_str(),
                                partition.partition_index,
                            ),
                        )
                        .await?;

                    partition_responses.push(
                        DeleteRecordsPartitionResult::default()
                            .partition_index(partition.partition_index)
                            .low_watermark(low_watermark.unwrap_or(0))
                            .error_code(ErrorCode::None.into()),
                    );
                }
            }

            responses.push(
                DeleteRecordsTopicResult::default()
                    .name(topic.name.clone())
                    .partitions(Some(partition_responses)),
            );
        }

        Ok(responses)
    }

    #[instrument(skip_all)]
    async fn delete_topic(&self, topic: &TopicId) -> Result<ErrorCode> {
        debug!(cluster = self.cluster, ?topic);

        let mut conn = self.connection().await?;

        let topic_name = match topic {
            TopicId::Name(name) => name.clone(),
            TopicId::Id(uuid) => {
                let name: Option<String> = self
                    .prepare_query_opt(
                        &mut conn,
                        "topic_select_uuid",
                        (self.cluster.as_str(), uuid.to_string()),
                    )
                    .await?;
                name.ok_or(Error::Api(ErrorCode::UnknownTopicOrPartition))?
            }
        };

        let _ = self
            .prepare_execute(
                &mut conn,
                "header_delete_by_topic",
                (self.cluster.as_str(), topic_name.as_str()),
            )
            .await?;

        let _ = self
            .prepare_execute(
                &mut conn,
                "record_delete_by_topic",
                (self.cluster.as_str(), topic_name.as_str()),
            )
            .await?;

        let _ = self
            .prepare_execute(
                &mut conn,
                "watermark_delete_by_topic",
                (self.cluster.as_str(), topic_name.as_str()),
            )
            .await?;

        let _ = self
            .prepare_execute(
                &mut conn,
                "topition_delete_by_topic",
                (self.cluster.as_str(), topic_name.as_str()),
            )
            .await?;

        let _ = self
            .prepare_execute(
                &mut conn,
                "topic_configuration_delete_by_topic",
                (self.cluster.as_str(), topic_name.as_str()),
            )
            .await?;

        let rows = self
            .prepare_execute(
                &mut conn,
                "topic_delete",
                (self.cluster.as_str(), topic_name.as_str()),
            )
            .await?;

        if rows == 0 {
            Ok(ErrorCode::UnknownTopicOrPartition)
        } else {
            Ok(ErrorCode::None)
        }
    }

    #[instrument(skip_all)]
    async fn produce(
        &self,
        transaction_id: Option<&str>,
        topition: &Topition,
        batch: deflated::Batch,
    ) -> Result<i64> {
        debug!(cluster = self.cluster, ?transaction_id, ?topition, ?batch);

        let mut conn = self.connection().await?;
        let mut tx = conn.start_transaction(TxOpts::default()).await?;

        let offset = self
            .produce_in_tx(transaction_id, topition, batch, &mut tx)
            .await?;

        tx.commit().await?;

        Ok(offset)
    }

    #[instrument(skip_all)]
    async fn fetch(
        &self,
        topition: &'_ Topition,
        offset: i64,
        min_bytes: u32,
        max_bytes: u32,
        isolation: IsolationLevel,
    ) -> Result<Vec<deflated::Batch>> {
        debug!(
            cluster = self.cluster,
            ?topition,
            offset,
            min_bytes,
            max_bytes,
            ?isolation
        );

        let mut conn = self.connection().await?;

        let rows: Vec<(
            i64,
            i16,
            i64,
            i16,
            NaiveDateTime,
            Option<Vec<u8>>,
            Option<Vec<u8>>,
        )> = self
            .prepare_query(
                &mut conn,
                "record_fetch",
                (
                    self.cluster.as_str(),
                    topition.topic(),
                    topition.partition(),
                    offset,
                ),
            )
            .await?;

        let mut batches = vec![];

        for (record_offset, attributes, producer_id, producer_epoch, timestamp, key, value) in rows
        {
            let timestamp_ms = timestamp.and_utc().timestamp_millis();
            let headers: Vec<(Option<Vec<u8>>, Option<Vec<u8>>)> = self
                .prepare_query(
                    &mut conn,
                    "header_fetch",
                    (
                        self.cluster.as_str(),
                        topition.topic(),
                        topition.partition(),
                        record_offset,
                    ),
                )
                .await?;

            let mut record_builder = Record::builder()
                .key(key.map(Bytes::from))
                .value(value.map(Bytes::from));

            for (hk, hv) in headers {
                let mut header_builder = Header::builder();
                if let Some(k) = hk {
                    header_builder = header_builder.key(Bytes::from(k));
                }
                if let Some(v) = hv {
                    header_builder = header_builder.value(Bytes::from(v));
                }
                record_builder = record_builder.header(header_builder);
            }

            let batch = Batch::builder()
                .base_offset(record_offset)
                .record(record_builder)
                .attributes(attributes)
                .producer_id(producer_id)
                .producer_epoch(producer_epoch)
                .base_timestamp(timestamp_ms)
                .build()
                .and_then(TryInto::try_into)?;

            batches.push(batch);
        }

        Ok(batches)
    }

    #[instrument(skip_all)]
    async fn offset_stage(&self, topition: &Topition) -> Result<OffsetStage> {
        debug!(cluster = self.cluster, ?topition);

        let mut conn = self.connection().await?;

        let row: Option<(Option<i64>, Option<i64>)> = self
            .prepare_query_opt(
                &mut conn,
                "watermark_select",
                (
                    self.cluster.as_str(),
                    topition.topic(),
                    topition.partition(),
                ),
            )
            .await?;

        if let Some((low, high)) = row {
            Ok(OffsetStage {
                last_stable: high.unwrap_or(0),
                high_watermark: high.unwrap_or(0),
                log_start: low.unwrap_or(0),
            })
        } else {
            Err(Error::Api(ErrorCode::UnknownTopicOrPartition))
        }
    }

    #[instrument(skip_all)]
    async fn list_offsets(
        &self,
        isolation_level: IsolationLevel,
        offsets: &[(Topition, ListOffset)],
    ) -> Result<Vec<(Topition, ListOffsetResponse)>> {
        debug!(cluster = self.cluster, ?isolation_level, ?offsets);

        let mut conn = self.connection().await?;

        let mut responses = vec![];

        for (topition, list_offset) in offsets {
            let row: Option<(Option<i64>, Option<i64>)> = self
                .prepare_query_opt(
                    &mut conn,
                    "watermark_select",
                    (
                        self.cluster.as_str(),
                        topition.topic(),
                        topition.partition(),
                    ),
                )
                .await?;

            if let Some((low, high)) = row {
                let offset = match list_offset {
                    ListOffset::Earliest => low.unwrap_or(0),
                    ListOffset::Latest => high.unwrap_or(0),
                    ListOffset::Timestamp(ts) => {
                        let ts_millis = ts
                            .duration_since(std::time::UNIX_EPOCH)
                            .map(|d| d.as_millis() as i64)
                            .unwrap_or(0);
                        let ts_offset: Option<i64> = self
                            .prepare_query_opt(
                                &mut conn,
                                "record_fetch_by_timestamp",
                                (
                                    self.cluster.as_str(),
                                    topition.topic(),
                                    topition.partition(),
                                    ts_millis,
                                ),
                            )
                            .await?;
                        ts_offset.unwrap_or(high.unwrap_or(0))
                    }
                };

                responses.push((
                    topition.clone(),
                    ListOffsetResponse {
                        error_code: ErrorCode::None,
                        timestamp: None,
                        offset: Some(offset),
                    },
                ));
            } else {
                responses.push((
                    topition.clone(),
                    ListOffsetResponse {
                        error_code: ErrorCode::UnknownTopicOrPartition,
                        timestamp: None,
                        offset: None,
                    },
                ));
            }
        }

        Ok(responses)
    }

    #[instrument(skip_all)]
    async fn offset_commit(
        &self,
        group_id: &str,
        retention_time_ms: Option<Duration>,
        offsets: &[(Topition, OffsetCommitRequest)],
    ) -> Result<Vec<(Topition, ErrorCode)>> {
        debug!(
            cluster = self.cluster,
            group_id,
            ?retention_time_ms,
            ?offsets
        );

        let mut conn = self.connection().await?;

        let mut responses = vec![];

        for (topition, offset_commit) in offsets {
            let _ = self
                .prepare_execute(
                    &mut conn,
                    "consumer_offset_insert",
                    (
                        self.cluster.as_str(),
                        group_id,
                        topition.topic(),
                        topition.partition(),
                        offset_commit.offset,
                    ),
                )
                .await?;

            responses.push((topition.clone(), ErrorCode::None));
        }

        Ok(responses)
    }

    #[instrument(skip_all)]
    async fn offset_fetch(
        &self,
        group_id: Option<&str>,
        topics: &[Topition],
        require_stable: Option<bool>,
    ) -> Result<BTreeMap<Topition, i64>> {
        debug!(cluster = self.cluster, group_id, ?topics, ?require_stable);

        let mut conn = self.connection().await?;

        let mut offsets = BTreeMap::new();

        if let Some(group_id) = group_id {
            for topition in topics {
                let offset: Option<i64> = self
                    .prepare_query_opt(
                        &mut conn,
                        "consumer_offset_select",
                        (
                            self.cluster.as_str(),
                            group_id,
                            topition.topic(),
                            topition.partition(),
                        ),
                    )
                    .await?;

                if let Some(offset) = offset {
                    let _ = offsets.insert(topition.clone(), offset);
                }
            }
        }

        Ok(offsets)
    }

    #[instrument(skip_all)]
    async fn committed_offset_topitions(&self, group_id: &str) -> Result<BTreeMap<Topition, i64>> {
        debug!(cluster = self.cluster, group_id);

        let mut conn = self.connection().await?;

        let rows: Vec<(String, i32, i64)> = self
            .prepare_query(
                &mut conn,
                "consumer_offset_select_by_group",
                (self.cluster.as_str(), group_id),
            )
            .await?;

        let mut offsets = BTreeMap::new();
        for (topic, partition, offset) in rows {
            let _ = offsets.insert(Topition::new(topic, partition), offset);
        }

        Ok(offsets)
    }

    #[instrument(skip_all)]
    async fn metadata(&self, topics: Option<&[TopicId]>) -> Result<MetadataResponse> {
        debug!(cluster = self.cluster, ?topics);

        let mut conn = self.connection().await?;

        let brokers = self.brokers().await?;

        let mut topic_responses = vec![];

        if let Some(topics) = topics {
            for topic in topics {
                let (name, partitions, uuid) = match topic {
                    TopicId::Name(name) => {
                        let row: Option<(i32, String)> = self
                            .prepare_query_opt(
                                &mut conn,
                                "topic_select_name_partitions",
                                (self.cluster.as_str(), name.as_str()),
                            )
                            .await?;

                        if let Some((partitions, uuid)) = row {
                            (name.clone(), partitions, uuid)
                        } else {
                            topic_responses.push(
                                MetadataResponseTopic::default()
                                    .error_code(ErrorCode::UnknownTopicOrPartition.into())
                                    .name(Some(name.into()))
                                    .topic_id(Some(NULL_TOPIC_ID))
                                    .is_internal(Some(false))
                                    .partitions(Some([].into())),
                            );
                            continue;
                        }
                    }
                    TopicId::Id(uuid) => {
                        let row: Option<(String, i32)> = self
                            .prepare_query_opt(
                                &mut conn,
                                "topic_select_name_partitions",
                                (self.cluster.as_str(), uuid.to_string()),
                            )
                            .await?;

                        if let Some((name, partitions)) = row {
                            (name, partitions, uuid.to_string())
                        } else {
                            topic_responses.push(
                                MetadataResponseTopic::default()
                                    .error_code(ErrorCode::UnknownTopicOrPartition.into())
                                    .name(None)
                                    .topic_id(Some(uuid.into_bytes()))
                                    .is_internal(Some(false))
                                    .partitions(Some([].into())),
                            );
                            continue;
                        }
                    }
                };

                let topic_uuid = Uuid::parse_str(&uuid).unwrap_or(Uuid::nil());

                let partition_responses: Vec<MetadataResponsePartition> = (0..partitions)
                    .map(|partition_index| {
                        MetadataResponsePartition::default()
                            .error_code(ErrorCode::None.into())
                            .partition_index(partition_index)
                            .leader_id(self.node)
                            .leader_epoch(Some(0))
                            .replica_nodes(Some(vec![self.node]))
                            .isr_nodes(Some(vec![self.node]))
                            .offline_replicas(Some(vec![]))
                    })
                    .collect();

                topic_responses.push(
                    MetadataResponseTopic::default()
                        .error_code(ErrorCode::None.into())
                        .name(Some(name.into()))
                        .topic_id(Some(topic_uuid.into_bytes()))
                        .is_internal(Some(false))
                        .partitions(Some(partition_responses)),
                );
            }
        } else {
            let rows: Vec<(String, i32, String)> = self
                .prepare_query(&mut conn, "topic_select_all", (self.cluster.as_str(),))
                .await?;

            for (name, partitions, uuid) in rows {
                let topic_uuid = Uuid::parse_str(&uuid).unwrap_or(Uuid::nil());

                let partition_responses: Vec<MetadataResponsePartition> = (0..partitions)
                    .map(|partition_index| {
                        MetadataResponsePartition::default()
                            .error_code(ErrorCode::None.into())
                            .partition_index(partition_index)
                            .leader_id(self.node)
                            .leader_epoch(Some(0))
                            .replica_nodes(Some(vec![self.node]))
                            .isr_nodes(Some(vec![self.node]))
                            .offline_replicas(Some(vec![]))
                    })
                    .collect();

                topic_responses.push(
                    MetadataResponseTopic::default()
                        .error_code(ErrorCode::None.into())
                        .name(Some(name.into()))
                        .topic_id(Some(topic_uuid.into_bytes()))
                        .is_internal(Some(false))
                        .partitions(Some(partition_responses)),
                );
            }
        }

        Ok(MetadataResponse {
            cluster: Some(self.cluster.clone()),
            controller: Some(self.node),
            brokers: brokers
                .into_iter()
                .map(|b| {
                    MetadataResponseBroker::default()
                        .node_id(b.broker_id)
                        .host(b.host)
                        .port(b.port)
                        .rack(b.rack)
                })
                .collect(),
            topics: topic_responses,
        })
    }

    #[instrument(skip_all)]
    async fn describe_config(
        &self,
        name: &str,
        resource: ConfigResource,
        keys: Option<&[String]>,
    ) -> Result<DescribeConfigsResult> {
        debug!(cluster = self.cluster, name, ?resource, ?keys);

        let mut conn = self.connection().await?;

        let exists: Option<i32> = match resource {
            ConfigResource::Topic => {
                self.prepare_query_opt(
                    &mut conn,
                    "topic_select_by_name",
                    (self.cluster.as_str(), name),
                )
                .await?
            }
            _ => Some(1),
        };

        if exists.is_some() {
            let rows: Vec<(String, Option<String>)> = self
                .prepare_query(
                    &mut conn,
                    "topic_configuration_select",
                    (self.cluster.as_str(), name),
                )
                .await?;

            let mut configs = vec![];

            for (config_name, config_value) in rows {
                if keys.is_none() || keys.unwrap().contains(&config_name) {
                    configs.push(
                        DescribeConfigsResourceResult::default()
                            .name(config_name)
                            .value(config_value)
                            .read_only(false)
                            .is_default(Some(false))
                            .is_sensitive(false)
                            .config_source(Some(i8::from(ConfigSource::DynamicTopicConfig)))
                            .config_type(Some(i8::from(ConfigType::String)))
                            .documentation(Some("".into())),
                    );
                }
            }

            let error_code = ErrorCode::None;

            Ok(DescribeConfigsResult::default()
                .error_code(error_code.into())
                .error_message(Some(error_code.to_string()))
                .resource_type(i8::from(resource))
                .resource_name(name.into())
                .configs(Some(configs)))
        } else {
            let error_code = ErrorCode::UnknownTopicOrPartition;

            Ok(DescribeConfigsResult::default()
                .error_code(error_code.into())
                .error_message(Some(error_code.to_string()))
                .resource_type(i8::from(resource))
                .resource_name(name.into())
                .configs(Some([].into())))
        }
    }

    #[instrument(skip_all)]
    async fn describe_topic_partitions(
        &self,
        topics: Option<&[TopicId]>,
        partition_limit: i32,
        cursor: Option<Topition>,
    ) -> Result<Vec<DescribeTopicPartitionsResponseTopic>> {
        let _ = (topics, partition_limit, cursor);

        let mut conn = self.connection().await.inspect_err(|err| error!(?err))?;

        let mut responses =
            Vec::with_capacity(topics.map(|topics| topics.len()).unwrap_or_default());

        for topic in topics.unwrap_or_default() {
            responses.push(match topic {
                TopicId::Name(name) => {
                    let row: Option<(String, String, bool, i32, i32)> = self
                        .prepare_query_opt(
                            &mut conn,
                            "topic_select_full",
                            (self.cluster.as_str(), name.as_str()),
                        )
                        .await
                        .inspect_err(|err| error!(?err))?;

                    if let Some((uuid, name, is_internal, partitions, replication_factor)) = row {
                        let topic_uuid = Uuid::parse_str(&uuid).unwrap_or(Uuid::nil());

                        DescribeTopicPartitionsResponseTopic::default()
                            .error_code(ErrorCode::None.into())
                            .name(Some(name))
                            .topic_id(topic_uuid.into_bytes())
                            .is_internal(is_internal)
                            .partitions(Some(
                                (0..partitions)
                                    .map(|partition_index| {
                                        DescribeTopicPartitionsResponsePartition::default()
                                            .error_code(ErrorCode::None.into())
                                            .partition_index(partition_index)
                                            .leader_id(self.node)
                                            .leader_epoch(0)
                                            .replica_nodes(Some(vec![
                                                self.node;
                                                replication_factor as usize
                                            ]))
                                            .isr_nodes(Some(vec![
                                                self.node;
                                                replication_factor as usize
                                            ]))
                                            .eligible_leader_replicas(Some(vec![]))
                                            .last_known_elr(Some(vec![]))
                                            .offline_replicas(Some(vec![]))
                                    })
                                    .collect(),
                            ))
                            .topic_authorized_operations(-2147483648)
                    } else {
                        DescribeTopicPartitionsResponseTopic::default()
                            .error_code(ErrorCode::UnknownTopicOrPartition.into())
                            .name(Some(name.into()))
                            .topic_id(NULL_TOPIC_ID)
                            .is_internal(false)
                            .partitions(Some([].into()))
                            .topic_authorized_operations(-2147483648)
                    }
                }
                TopicId::Id(id) => {
                    let row: Option<(String, String, bool, i32, i32)> = self
                        .prepare_query_opt(
                            &mut conn,
                            "topic_select_full_by_uuid",
                            (self.cluster.as_str(), id.to_string()),
                        )
                        .await?;

                    if let Some((uuid, name, is_internal, partitions, replication_factor)) = row {
                        let topic_uuid = Uuid::parse_str(&uuid).unwrap_or(Uuid::nil());

                        DescribeTopicPartitionsResponseTopic::default()
                            .error_code(ErrorCode::None.into())
                            .name(Some(name))
                            .topic_id(topic_uuid.into_bytes())
                            .is_internal(is_internal)
                            .partitions(Some(
                                (0..partitions)
                                    .map(|partition_index| {
                                        DescribeTopicPartitionsResponsePartition::default()
                                            .error_code(ErrorCode::None.into())
                                            .partition_index(partition_index)
                                            .leader_id(self.node)
                                            .leader_epoch(0)
                                            .replica_nodes(Some(vec![
                                                self.node;
                                                replication_factor as usize
                                            ]))
                                            .isr_nodes(Some(vec![
                                                self.node;
                                                replication_factor as usize
                                            ]))
                                            .eligible_leader_replicas(Some(vec![]))
                                            .last_known_elr(Some(vec![]))
                                            .offline_replicas(Some(vec![]))
                                    })
                                    .collect(),
                            ))
                            .topic_authorized_operations(-2147483648)
                    } else {
                        DescribeTopicPartitionsResponseTopic::default()
                            .error_code(ErrorCode::UnknownTopicOrPartition.into())
                            .name(None)
                            .topic_id(id.into_bytes())
                            .is_internal(false)
                            .partitions(Some([].into()))
                            .topic_authorized_operations(-2147483648)
                    }
                }
            });
        }

        Ok(responses)
    }

    #[instrument(skip_all)]
    async fn list_groups(&self, states_filter: Option<&[String]>) -> Result<Vec<ListedGroup>> {
        debug!(?states_filter);

        let mut conn = self.connection().await.inspect_err(|err| error!(?err))?;

        let rows: Vec<String> = self
            .prepare_query(
                &mut conn,
                "consumer_group_select_all",
                (self.cluster.as_str(),),
            )
            .await
            .inspect_err(|err| error!(?err))?;

        let listed_groups = rows
            .into_iter()
            .map(|group_id| {
                ListedGroup::default()
                    .group_id(group_id)
                    .protocol_type("consumer".into())
                    .group_state(Some("unknown".into()))
                    .group_type(Some("classic".into()))
            })
            .collect();

        Ok(listed_groups)
    }

    #[instrument(skip_all)]
    async fn delete_groups(
        &self,
        group_ids: Option<&[String]>,
    ) -> Result<Vec<DeletableGroupResult>> {
        debug!(?group_ids);

        let mut results = vec![];

        if let Some(group_ids) = group_ids {
            let mut conn = self.connection().await?;

            for group_id in group_ids {
                let _ = self
                    .prepare_execute(
                        &mut conn,
                        "consumer_offset_delete_by_group",
                        (self.cluster.as_str(), group_id.as_str()),
                    )
                    .await
                    .inspect_err(|err| error!(?err))?;

                let _ = self
                    .prepare_execute(
                        &mut conn,
                        "consumer_group_detail_delete",
                        (self.cluster.as_str(), group_id.as_str()),
                    )
                    .await
                    .inspect_err(|err| error!(?err))?;

                let rows = self
                    .prepare_execute(
                        &mut conn,
                        "consumer_group_delete",
                        (self.cluster.as_str(), group_id.as_str()),
                    )
                    .await
                    .inspect_err(|err| error!(?err))?;

                results.push(
                    DeletableGroupResult::default()
                        .group_id(group_id.into())
                        .error_code(
                            if rows == 0 {
                                ErrorCode::GroupIdNotFound
                            } else {
                                ErrorCode::None
                            }
                            .into(),
                        ),
                );
            }
        }

        Ok(results)
    }

    #[instrument(skip_all)]
    async fn describe_groups(
        &self,
        group_ids: Option<&[String]>,
        include_authorized_operations: bool,
    ) -> Result<Vec<NamedGroupDetail>> {
        debug!(?group_ids, include_authorized_operations);

        let mut results = vec![];
        let mut conn = self.connection().await.inspect_err(|err| error!(?err))?;

        if let Some(group_ids) = group_ids {
            for group_id in group_ids {
                let row: Option<(String, String)> = self
                    .prepare_query_opt(
                        &mut conn,
                        "consumer_group_select",
                        (self.cluster.as_str(), group_id.as_str()),
                    )
                    .await
                    .inspect_err(|err| error!(?err, group_id))?;

                if let Some((_, detail)) = row {
                    let value: Value = serde_json::from_str(&detail)?;

                    let current = serde_json::from_value::<GroupDetail>(value)
                        .inspect(|current| debug!(?current))?;

                    results.push(NamedGroupDetail::found(group_id.into(), current));
                } else {
                    results.push(NamedGroupDetail::error_code(
                        group_id.into(),
                        ErrorCode::GroupIdNotFound,
                    ));
                }
            }
        }

        Ok(results)
    }

    #[instrument(skip_all)]
    async fn update_group(
        &self,
        group_id: &str,
        detail: GroupDetail,
        version: Option<Version>,
    ) -> Result<Version, UpdateError<GroupDetail>> {
        debug!(cluster = self.cluster, group_id, ?detail, ?version);

        let mut conn = self.connection().await?;
        let mut tx = conn.start_transaction(TxOpts::default()).await?;

        let _ = self
            .tx_prepare_execute(
                &mut tx,
                "consumer_group_insert",
                (self.cluster.as_str(), group_id),
            )
            .await?;

        let detail_json = serde_json::to_string(&detail)?;
        let new_version = Uuid::new_v4();

        if let Some(expected_version) = version {
            let current: Option<(String, String)> = self
                .tx_prepare_query_opt(
                    &mut tx,
                    "consumer_group_select_for_update",
                    (self.cluster.as_str(), group_id),
                )
                .await?;

            if let Some((current_detail, current_version)) = current {
                if Some(current_version.clone()) != expected_version.e_tag.clone() {
                    let current_detail: GroupDetail = serde_json::from_str(&current_detail)?;
                    return Err(UpdateError::Outdated {
                        current: current_detail,
                        version: Version {
                            e_tag: Some(current_version),
                            version: None,
                        },
                    });
                }
            }
        }

        let _ = self
            .tx_prepare_execute(
                &mut tx,
                "consumer_group_update",
                (
                    detail_json,
                    new_version.to_string(),
                    self.cluster.as_str(),
                    group_id,
                ),
            )
            .await?;

        tx.commit().await?;

        Ok(Version {
            e_tag: Some(new_version.to_string()),
            version: None,
        })
    }

    #[instrument(skip_all)]
    async fn init_producer(
        &self,
        transaction_id: Option<&str>,
        transaction_timeout_ms: i32,
        producer_id: Option<i64>,
        producer_epoch: Option<i16>,
    ) -> Result<ProducerIdResponse> {
        debug!(
            cluster = self.cluster,
            ?transaction_id,
            transaction_timeout_ms,
            ?producer_id,
            ?producer_epoch
        );

        let mut conn = self.connection().await?;
        let mut tx = conn.start_transaction(TxOpts::default()).await?;

        let (pid, pep) = if let Some(producer_id) = producer_id {
            let epoch = producer_epoch.unwrap_or(0) + 1;
            (producer_id, epoch)
        } else {
            let new_pid: i64 = rng().random();
            (new_pid.abs(), 0i16)
        };

        let _ = self
            .tx_prepare_execute(&mut tx, "producer_insert", (self.cluster.as_str(), pid))
            .await?;

        let _ = self
            .tx_prepare_execute(
                &mut tx,
                "producer_epoch_insert",
                (self.cluster.as_str(), pid, pep),
            )
            .await?;

        if let Some(txn_id) = transaction_id {
            let _ = self
                .tx_prepare_execute(
                    &mut tx,
                    "txn_insert",
                    (
                        self.cluster.as_str(),
                        txn_id,
                        pid,
                        pep,
                        transaction_timeout_ms,
                        String::from(TxnState::Begin),
                    ),
                )
                .await?;
        }

        tx.commit().await?;

        Ok(ProducerIdResponse {
            error: ErrorCode::None,
            id: pid,
            epoch: pep,
        })
    }

    #[instrument(skip_all)]
    async fn txn_add_offsets(
        &self,
        transaction_id: &str,
        producer_id: i64,
        producer_epoch: i16,
        group_id: &str,
    ) -> Result<ErrorCode> {
        debug!(
            cluster = self.cluster,
            transaction_id, producer_id, producer_epoch, group_id
        );

        let mut conn = self.connection().await?;

        let _ = self
            .prepare_execute(
                &mut conn,
                "txn_offset_add",
                (
                    self.cluster.as_str(),
                    transaction_id,
                    producer_id,
                    producer_epoch,
                    group_id,
                ),
            )
            .await?;

        Ok(ErrorCode::None)
    }

    #[instrument(skip_all)]
    async fn txn_add_partitions(
        &self,
        partitions: TxnAddPartitionsRequest,
    ) -> Result<TxnAddPartitionsResponse> {
        debug!(cluster = self.cluster, ?partitions);

        match partitions {
            TxnAddPartitionsRequest::VersionZeroToThree {
                transaction_id,
                producer_id,
                producer_epoch,
                topics,
            } => {
                let mut conn = self.connection().await?;
                let mut results = vec![];

                for topic in topics {
                    let mut results_by_partition = vec![];

                    for partition_index in topic.partitions.unwrap_or(vec![]) {
                        let exists: Option<i32> = self
                            .prepare_query_opt(
                                &mut conn,
                                "topition_exists",
                                (self.cluster.as_str(), topic.name.as_str(), partition_index),
                            )
                            .await?;

                        if exists.is_none() {
                            results_by_partition.push(
                                AddPartitionsToTxnPartitionResult::default()
                                    .partition_index(partition_index)
                                    .partition_error_code(
                                        ErrorCode::UnknownTopicOrPartition.into(),
                                    ),
                            );
                            continue;
                        }

                        let _ = self
                            .prepare_execute(
                                &mut conn,
                                "txn_topition_insert",
                                (
                                    self.cluster.as_str(),
                                    transaction_id.as_str(),
                                    producer_id,
                                    producer_epoch,
                                    topic.name.as_str(),
                                    partition_index,
                                ),
                            )
                            .await?;

                        results_by_partition.push(
                            AddPartitionsToTxnPartitionResult::default()
                                .partition_index(partition_index)
                                .partition_error_code(i16::from(ErrorCode::None)),
                        );
                    }

                    results.push(
                        AddPartitionsToTxnTopicResult::default()
                            .name(topic.name)
                            .results_by_partition(Some(results_by_partition)),
                    );
                }

                Ok(TxnAddPartitionsResponse::VersionZeroToThree(results))
            }

            TxnAddPartitionsRequest::VersionFourPlus { transactions } => {
                let mut conn = self.connection().await?;
                let mut results = vec![];

                for txn in transactions {
                    let mut topic_results = vec![];

                    for topic in txn.topics.unwrap_or(vec![]) {
                        let mut partition_results = vec![];

                        for partition_index in topic.partitions.unwrap_or(vec![]) {
                            let exists: Option<i32> = self
                                .prepare_query_opt(
                                    &mut conn,
                                    "topition_exists",
                                    (self.cluster.as_str(), topic.name.as_str(), partition_index),
                                )
                                .await?;

                            if exists.is_none() {
                                partition_results.push(
                                    AddPartitionsToTxnPartitionResult::default()
                                        .partition_index(partition_index)
                                        .partition_error_code(
                                            ErrorCode::UnknownTopicOrPartition.into(),
                                        ),
                                );
                                continue;
                            }

                            let _ = self
                                .prepare_execute(
                                    &mut conn,
                                    "txn_topition_insert",
                                    (
                                        self.cluster.as_str(),
                                        txn.transactional_id.as_str(),
                                        txn.producer_id,
                                        txn.producer_epoch,
                                        topic.name.as_str(),
                                        partition_index,
                                    ),
                                )
                                .await?;

                            partition_results.push(
                                AddPartitionsToTxnPartitionResult::default()
                                    .partition_index(partition_index)
                                    .partition_error_code(i16::from(ErrorCode::None)),
                            );
                        }

                        topic_results.push(
                            AddPartitionsToTxnTopicResult::default()
                                .name(topic.name)
                                .results_by_partition(Some(partition_results)),
                        );
                    }

                    results.push(
                        tansu_sans_io::add_partitions_to_txn_response::AddPartitionsToTxnResult::default()
                            .transactional_id(txn.transactional_id)
                            .topic_results(Some(topic_results)),
                    );
                }

                Ok(TxnAddPartitionsResponse::VersionFourPlus(results))
            }
        }
    }

    #[instrument(skip_all)]
    async fn txn_offset_commit(
        &self,
        offsets: TxnOffsetCommitRequest,
    ) -> Result<Vec<TxnOffsetCommitResponseTopic>> {
        debug!(cluster = self.cluster, ?offsets);

        let mut conn = self.connection().await?;
        let mut tx = conn.start_transaction(TxOpts::default()).await?;

        let (producer_id, producer_epoch): (Option<i64>, Option<i16>) = self
            .tx_prepare_query_opt(
                &mut tx,
                "txn_select_for_offset",
                (self.cluster.as_str(), &offsets.transaction_id),
            )
            .await?
            .unwrap_or((None, None));

        let _ = self
            .tx_prepare_execute(
                &mut tx,
                "consumer_group_insert",
                (self.cluster.as_str(), &offsets.group_id),
            )
            .await?;

        debug!(?producer_id, ?producer_epoch);

        let _ = self
            .tx_prepare_execute(
                &mut tx,
                "txn_offset_commit_insert",
                (
                    self.cluster.as_str(),
                    &offsets.transaction_id,
                    &offsets.group_id,
                    offsets.producer_id,
                    offsets.producer_epoch,
                    offsets.generation_id,
                    &offsets.member_id,
                ),
            )
            .await?;

        let mut topics = vec![];

        for topic in offsets.topics {
            let mut partitions = vec![];

            for partition in topic.partitions.unwrap_or(vec![]) {
                if producer_id.is_some_and(|pid| pid == offsets.producer_id) {
                    if producer_epoch.is_some_and(|pep| pep == offsets.producer_epoch) {
                        let _ = self
                            .tx_prepare_execute(
                                &mut tx,
                                "txn_offset_commit_tp_insert",
                                (
                                    self.cluster.as_str(),
                                    &offsets.transaction_id,
                                    &offsets.group_id,
                                    offsets.producer_id,
                                    offsets.producer_epoch,
                                    &topic.name,
                                    partition.partition_index,
                                    partition.committed_offset,
                                    partition.committed_leader_epoch,
                                    &partition.committed_metadata,
                                ),
                            )
                            .await?;

                        partitions.push(
                            TxnOffsetCommitResponsePartition::default()
                                .partition_index(partition.partition_index)
                                .error_code(i16::from(ErrorCode::None)),
                        );
                    } else {
                        partitions.push(
                            TxnOffsetCommitResponsePartition::default()
                                .partition_index(partition.partition_index)
                                .error_code(i16::from(ErrorCode::InvalidProducerEpoch)),
                        );
                    }
                } else {
                    partitions.push(
                        TxnOffsetCommitResponsePartition::default()
                            .partition_index(partition.partition_index)
                            .error_code(i16::from(ErrorCode::UnknownProducerId)),
                    );
                }
            }

            topics.push(
                TxnOffsetCommitResponseTopic::default()
                    .name(topic.name)
                    .partitions(Some(partitions)),
            );
        }

        tx.commit().await?;

        Ok(topics)
    }

    #[instrument(skip_all)]
    async fn txn_end(
        &self,
        transaction_id: &str,
        producer_id: i64,
        producer_epoch: i16,
        committed: bool,
    ) -> Result<ErrorCode> {
        debug!(
            cluster = self.cluster,
            transaction_id, producer_id, producer_epoch, committed
        );

        let mut conn = self.connection().await?;
        let mut tx = conn.start_transaction(TxOpts::default()).await?;

        let result = self
            .end_txn_in_tx(
                transaction_id,
                producer_id,
                producer_epoch,
                committed,
                &mut tx,
            )
            .await?;

        tx.commit().await?;

        Ok(result)
    }

    #[instrument(skip_all)]
    async fn incremental_alter_resource(
        &self,
        resource: AlterConfigsResource,
    ) -> Result<AlterConfigsResourceResponse> {
        debug!(cluster = self.cluster, ?resource);

        let mut conn = self.connection().await?;

        if let Some(configs) = resource.configs {
            for config in configs {
                match OpType::try_from(config.config_operation)? {
                    OpType::Set => {
                        let _ = self
                            .prepare_execute(
                                &mut conn,
                                "topic_configuration_upsert",
                                (
                                    self.cluster.as_str(),
                                    resource.resource_name.as_str(),
                                    config.name.as_str(),
                                    config.value.as_deref(),
                                ),
                            )
                            .await?;
                    }
                    OpType::Delete => {
                        let _ = self
                            .prepare_execute(
                                &mut conn,
                                "topic_configuration_delete",
                                (
                                    self.cluster.as_str(),
                                    resource.resource_name.as_str(),
                                    config.name.as_str(),
                                ),
                            )
                            .await?;
                    }
                    _ => {}
                }
            }
        }

        Ok(AlterConfigsResourceResponse::default()
            .error_code(ErrorCode::None.into())
            .error_message(None)
            .resource_type(resource.resource_type)
            .resource_name(resource.resource_name))
    }

    async fn cluster_id(&self) -> Result<String> {
        Ok(self.cluster.clone())
    }

    async fn node(&self) -> Result<i32> {
        Ok(self.node)
    }

    async fn advertised_listener(&self) -> Result<Url> {
        Ok(self.advertised_listener.clone())
    }

    async fn ping(&self) -> Result<()> {
        let mut conn = self.connection().await?;
        let _: (i32,) = self.query_one(&mut conn, "SELECT 1 + 1", ()).await?;
        Ok(())
    }
}
