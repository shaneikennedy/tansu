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

use std::{fmt::Debug, marker::PhantomData, sync::LazyLock, time::SystemTime};

use chrono::{DateTime, Utc};

use bytes::Bytes;
use deadpool::managed::Pool;
use mysql_async::{Conn, Transaction, prelude::*};
use opentelemetry::metrics::Histogram;
use opentelemetry::{KeyValue, metrics::Counter};
use tansu_sans_io::{
    BatchAttribute, ConfigResource, ControlBatch, EndTransactionMarker, ErrorCode,
    record::{Record, deflated, inflated::Batch},
    to_system_time,
};
use tansu_schema::{
    Registry,
    lake::{House, LakeHouse as _},
};
use tracing::{debug, error, instrument};
use url::Url;

use crate::Topition;
use crate::mysql::manager::MysqlManager;
use crate::mysql::txn::Txn;
use crate::sql::mysql::SQL;
use crate::{Error, METER, Result, Storage, TxnState, sql::idempotent_sequence_check};

static SQL_DURATION: LazyLock<Histogram<u64>> = LazyLock::new(|| {
    METER
        .u64_histogram("tansu_mysql_sql_duration")
        .with_unit("ms")
        .with_description("The MySQL SQL request latencies in milliseconds")
        .build()
});

static SQL_REQUESTS: LazyLock<Counter<u64>> = LazyLock::new(|| {
    METER
        .u64_counter("tansu_mysql_sql_requests")
        .with_description("The number of MySQL SQL requests made")
        .build()
});

static SQL_ERROR: LazyLock<Counter<u64>> = LazyLock::new(|| {
    METER
        .u64_counter("tansu_mysql_sql_error")
        .with_description("The MySQL SQL error count")
        .build()
});

pub(crate) type MysqlPool = Pool<MysqlManager>;

/// MySQL Storage Engine
#[derive(Clone, Debug)]
pub struct Mysql {
    pub(crate) cluster: String,
    pub(crate) node: i32,
    pub(crate) advertised_listener: Url,
    pub(crate) pool: MysqlPool,
    pub(crate) schemas: Option<Registry>,
    pub(crate) lake: Option<House>,
}

/// MySQL Storage Builder
#[derive(Clone, Debug)]
pub struct Builder<C, N, L, P> {
    cluster: C,
    node: N,
    advertised_listener: L,
    pool: P,
    schemas: Option<Registry>,
    lake: Option<House>,
}

impl<C: Default, N: Default, L: Default, P: Default> Default for Builder<C, N, L, P> {
    fn default() -> Self {
        Self {
            cluster: C::default(),
            node: N::default(),
            advertised_listener: L::default(),
            pool: P::default(),
            schemas: None,
            lake: None,
        }
    }
}

impl<C, N, L, P> Builder<C, N, L, P> {
    pub fn cluster(self, cluster: impl Into<String>) -> Builder<String, N, L, P> {
        Builder {
            cluster: cluster.into(),
            node: self.node,
            advertised_listener: self.advertised_listener,
            pool: self.pool,
            schemas: self.schemas,
            lake: self.lake,
        }
    }

    pub fn node(self, node: i32) -> Builder<C, i32, L, P> {
        Builder {
            cluster: self.cluster,
            node,
            advertised_listener: self.advertised_listener,
            pool: self.pool,
            schemas: self.schemas,
            lake: self.lake,
        }
    }

    pub fn advertised_listener(self, advertised_listener: Url) -> Builder<C, N, Url, P> {
        Builder {
            cluster: self.cluster,
            node: self.node,
            advertised_listener,
            pool: self.pool,
            schemas: self.schemas,
            lake: self.lake,
        }
    }

    pub fn pool(self, pool: MysqlPool) -> Builder<C, N, L, MysqlPool> {
        Builder {
            cluster: self.cluster,
            node: self.node,
            advertised_listener: self.advertised_listener,
            pool,
            schemas: self.schemas,
            lake: self.lake,
        }
    }

    pub fn schemas(self, schemas: Option<Registry>) -> Builder<C, N, L, P> {
        Self { schemas, ..self }
    }

    pub fn lake(self, lake: Option<House>) -> Self {
        Self { lake, ..self }
    }
}

impl Builder<String, i32, Url, MysqlPool> {
    pub fn build(self) -> Mysql {
        Mysql {
            cluster: self.cluster,
            node: self.node,
            advertised_listener: self.advertised_listener,
            pool: self.pool,
            schemas: self.schemas,
            lake: self.lake,
        }
    }
}

impl Mysql {
    pub fn builder(
        connection: &str,
    ) -> Result<Builder<PhantomData<String>, PhantomData<i32>, Url, MysqlPool>> {
        debug!(connection);

        let manager =
            MysqlManager::from_url(connection).map_err(|e| Error::Message(e.to_string()))?;

        let pool = Pool::builder(manager)
            .max_size(16)
            .build()
            .map_err(|e| Error::Message(e.to_string()))?;

        let advertised_listener = Url::parse("tcp://127.0.0.1/")?;

        Ok(Builder {
            pool,
            advertised_listener,
            node: PhantomData,
            cluster: PhantomData,
            schemas: None,
            lake: None,
        })
    }

    pub(crate) async fn connection(&self) -> Result<deadpool::managed::Object<MysqlManager>> {
        self.pool.get().await.map_err(Into::into)
    }

    fn sql_lookup(&self, key: &str) -> Result<&str> {
        SQL.get(key)
    }

    fn attributes_for_sql(&self, sql: &str) -> Vec<KeyValue> {
        vec![
            KeyValue::new("sql", sql.to_owned()),
            KeyValue::new("cluster_id", self.cluster.clone()),
        ]
    }

    #[instrument(skip(self, conn, params))]
    pub(crate) async fn execute<P>(&self, conn: &mut Conn, sql: &str, params: P) -> Result<u64>
    where
        P: Into<mysql_async::Params> + Send,
    {
        let execute_start = SystemTime::now();

        let result = conn.exec_drop(sql, params).await;

        SQL_DURATION.record(
            execute_start
                .elapsed()
                .map_or(0, |duration| duration.as_millis() as u64),
            &self.attributes_for_sql(sql),
        );

        match result {
            Ok(_) => {
                SQL_REQUESTS.add(1, &self.attributes_for_sql(sql));
                Ok(conn.affected_rows())
            }
            Err(err) => {
                SQL_ERROR.add(1, &self.attributes_for_sql(sql));
                Err(Error::MysqlAsync(err.into()))
            }
        }
    }

    #[instrument(skip(self, conn, params))]
    pub(crate) async fn query<T, P>(&self, conn: &mut Conn, sql: &str, params: P) -> Result<Vec<T>>
    where
        T: FromRow + Send + 'static,
        P: Into<mysql_async::Params> + Send,
    {
        let execute_start = SystemTime::now();

        let result: std::result::Result<Vec<T>, _> = conn.exec(sql, params).await;

        SQL_DURATION.record(
            execute_start
                .elapsed()
                .map_or(0, |duration| duration.as_millis() as u64),
            &self.attributes_for_sql(sql),
        );

        match result {
            Ok(rows) => {
                SQL_REQUESTS.add(1, &self.attributes_for_sql(sql));
                Ok(rows)
            }
            Err(err) => {
                SQL_ERROR.add(1, &self.attributes_for_sql(sql));
                Err(Error::MysqlAsync(err.into()))
            }
        }
    }

    #[instrument(skip(self, conn, params))]
    pub(crate) async fn query_one<T, P>(&self, conn: &mut Conn, sql: &str, params: P) -> Result<T>
    where
        T: FromRow + Send + 'static,
        P: Into<mysql_async::Params> + Send,
    {
        let execute_start = SystemTime::now();

        let result: std::result::Result<Option<T>, _> = conn.exec_first(sql, params).await;

        SQL_DURATION.record(
            execute_start
                .elapsed()
                .map_or(0, |duration| duration.as_millis() as u64),
            &self.attributes_for_sql(sql),
        );

        match result {
            Ok(Some(row)) => {
                SQL_REQUESTS.add(1, &self.attributes_for_sql(sql));
                Ok(row)
            }
            Ok(None) => {
                SQL_REQUESTS.add(1, &self.attributes_for_sql(sql));
                Err(Error::Message("No rows returned".into()))
            }
            Err(err) => {
                SQL_ERROR.add(1, &self.attributes_for_sql(sql));
                Err(Error::MysqlAsync(err.into()))
            }
        }
    }

    #[instrument(skip(self, conn, params))]
    pub(crate) async fn query_opt<T, P>(
        &self,
        conn: &mut Conn,
        sql: &str,
        params: P,
    ) -> Result<Option<T>>
    where
        T: FromRow + Send + 'static,
        P: Into<mysql_async::Params> + Send,
    {
        let execute_start = SystemTime::now();

        let result: std::result::Result<Option<T>, _> = conn.exec_first(sql, params).await;

        SQL_DURATION.record(
            execute_start
                .elapsed()
                .map_or(0, |duration| duration.as_millis() as u64),
            &self.attributes_for_sql(sql),
        );

        match result {
            Ok(row) => {
                SQL_REQUESTS.add(1, &self.attributes_for_sql(sql));
                Ok(row)
            }
            Err(err) => {
                SQL_ERROR.add(1, &self.attributes_for_sql(sql));
                Err(Error::MysqlAsync(err.into()))
            }
        }
    }

    #[instrument(skip(self, tx, params))]
    pub(crate) async fn tx_execute<'t, P>(
        &self,
        tx: &mut Transaction<'t>,
        sql: &str,
        params: P,
    ) -> Result<u64>
    where
        P: Into<mysql_async::Params> + Send,
    {
        let execute_start = SystemTime::now();

        let result = tx.exec_drop(sql, params).await;

        SQL_DURATION.record(
            execute_start
                .elapsed()
                .map_or(0, |duration| duration.as_millis() as u64),
            &self.attributes_for_sql(sql),
        );

        match result {
            Ok(_) => {
                SQL_REQUESTS.add(1, &self.attributes_for_sql(sql));
                Ok(tx.affected_rows())
            }
            Err(err) => {
                SQL_ERROR.add(1, &self.attributes_for_sql(sql));
                Err(Error::MysqlAsync(err.into()))
            }
        }
    }

    #[instrument(skip(self, tx, params))]
    pub(crate) async fn tx_query<'t, T, P>(
        &self,
        tx: &mut Transaction<'t>,
        sql: &str,
        params: P,
    ) -> Result<Vec<T>>
    where
        T: FromRow + Send + 'static,
        P: Into<mysql_async::Params> + Send,
    {
        let execute_start = SystemTime::now();

        let result: std::result::Result<Vec<T>, _> = tx.exec(sql, params).await;

        SQL_DURATION.record(
            execute_start
                .elapsed()
                .map_or(0, |duration| duration.as_millis() as u64),
            &self.attributes_for_sql(sql),
        );

        match result {
            Ok(rows) => {
                SQL_REQUESTS.add(1, &self.attributes_for_sql(sql));
                Ok(rows)
            }
            Err(err) => {
                SQL_ERROR.add(1, &self.attributes_for_sql(sql));
                Err(Error::MysqlAsync(err.into()))
            }
        }
    }

    #[instrument(skip(self, tx, params))]
    pub(crate) async fn tx_query_one<'t, T, P>(
        &self,
        tx: &mut Transaction<'t>,
        sql: &str,
        params: P,
    ) -> Result<T>
    where
        T: FromRow + Send + 'static,
        P: Into<mysql_async::Params> + Send,
    {
        let execute_start = SystemTime::now();

        let result: std::result::Result<Option<T>, _> = tx.exec_first(sql, params).await;

        SQL_DURATION.record(
            execute_start
                .elapsed()
                .map_or(0, |duration| duration.as_millis() as u64),
            &self.attributes_for_sql(sql),
        );

        match result {
            Ok(Some(row)) => {
                SQL_REQUESTS.add(1, &self.attributes_for_sql(sql));
                Ok(row)
            }
            Ok(None) => {
                SQL_REQUESTS.add(1, &self.attributes_for_sql(sql));
                Err(Error::Message("No rows returned".into()))
            }
            Err(err) => {
                SQL_ERROR.add(1, &self.attributes_for_sql(sql));
                Err(Error::MysqlAsync(err.into()))
            }
        }
    }

    #[instrument(skip(self, tx, params))]
    pub(crate) async fn tx_query_opt<'t, T, P>(
        &self,
        tx: &mut Transaction<'t>,
        sql: &str,
        params: P,
    ) -> Result<Option<T>>
    where
        T: FromRow + Send + 'static,
        P: Into<mysql_async::Params> + Send,
    {
        let execute_start = SystemTime::now();

        let result: std::result::Result<Option<T>, _> = tx.exec_first(sql, params).await;

        SQL_DURATION.record(
            execute_start
                .elapsed()
                .map_or(0, |duration| duration.as_millis() as u64),
            &self.attributes_for_sql(sql),
        );

        match result {
            Ok(row) => {
                SQL_REQUESTS.add(1, &self.attributes_for_sql(sql));
                Ok(row)
            }
            Err(err) => {
                SQL_ERROR.add(1, &self.attributes_for_sql(sql));
                Err(Error::MysqlAsync(err.into()))
            }
        }
    }

    #[instrument(skip(self, conn, params))]
    pub(crate) async fn prepare_execute<P>(
        &self,
        conn: &mut Conn,
        sql_key: &str,
        params: P,
    ) -> Result<u64>
    where
        P: Into<mysql_async::Params> + Send,
    {
        let sql = self.sql_lookup(sql_key)?;
        self.execute(conn, sql, params).await
    }

    #[instrument(skip(self, conn, params))]
    pub(crate) async fn prepare_query<T, P>(
        &self,
        conn: &mut Conn,
        sql_key: &str,
        params: P,
    ) -> Result<Vec<T>>
    where
        T: FromRow + Send + 'static,
        P: Into<mysql_async::Params> + Send,
    {
        let sql = self.sql_lookup(sql_key)?;
        self.query(conn, sql, params).await
    }

    #[instrument(skip(self, conn, params))]
    pub(crate) async fn prepare_query_opt<T, P>(
        &self,
        conn: &mut Conn,
        sql_key: &str,
        params: P,
    ) -> Result<Option<T>>
    where
        T: FromRow + Send + 'static,
        P: Into<mysql_async::Params> + Send,
    {
        let sql = self.sql_lookup(sql_key)?;
        self.query_opt(conn, sql, params).await
    }

    #[instrument(skip(self, tx, params))]
    pub(crate) async fn tx_prepare_execute<'t, P>(
        &self,
        tx: &mut Transaction<'t>,
        sql_key: &str,
        params: P,
    ) -> Result<u64>
    where
        P: Into<mysql_async::Params> + Send,
    {
        let sql = self.sql_lookup(sql_key)?;
        self.tx_execute(tx, sql, params).await
    }

    #[instrument(skip(self, tx, params))]
    pub(crate) async fn tx_prepare_query<'t, T, P>(
        &self,
        tx: &mut Transaction<'t>,
        sql_key: &str,
        params: P,
    ) -> Result<Vec<T>>
    where
        T: FromRow + Send + 'static,
        P: Into<mysql_async::Params> + Send,
    {
        let sql = self.sql_lookup(sql_key)?;
        self.tx_query(tx, sql, params).await
    }

    #[instrument(skip(self, tx, params))]
    pub(crate) async fn tx_prepare_query_one<'t, T, P>(
        &self,
        tx: &mut Transaction<'t>,
        sql_key: &str,
        params: P,
    ) -> Result<T>
    where
        T: FromRow + Send + 'static,
        P: Into<mysql_async::Params> + Send,
    {
        let sql = self.sql_lookup(sql_key)?;
        self.tx_query_one(tx, sql, params).await
    }

    #[instrument(skip(self, tx, params))]
    pub(crate) async fn tx_prepare_query_opt<'t, T, P>(
        &self,
        tx: &mut Transaction<'t>,
        sql_key: &str,
        params: P,
    ) -> Result<Option<T>>
    where
        T: FromRow + Send + 'static,
        P: Into<mysql_async::Params> + Send,
    {
        let sql = self.sql_lookup(sql_key)?;
        self.tx_query_opt(tx, sql, params).await
    }

    async fn idempotent_message_check<'t>(
        &self,
        transaction_id: Option<&str>,
        topition: &Topition,
        deflated: &deflated::Batch,
        tx: &mut Transaction<'t>,
    ) -> Result<()> {
        debug!(transaction_id, ?deflated);

        let current_epoch: Option<i16> = self
            .tx_prepare_query_opt(
                tx,
                "producer_epoch_select_current",
                (self.cluster.as_str(), deflated.producer_id),
            )
            .await
            .inspect_err(|err| error!(?err))?;

        if let Some(current_epoch) = current_epoch {
            let sequence: i32 = self
                .tx_prepare_query_one(
                    tx,
                    "producer_detail_select_for_update",
                    (
                        self.cluster.as_str(),
                        topition.topic(),
                        topition.partition(),
                        deflated.producer_id,
                        deflated.producer_epoch,
                    ),
                )
                .await
                .inspect_err(|err| {
                    error!(
                        self.cluster,
                        ?topition,
                        deflated.producer_id,
                        deflated.producer_epoch,
                        ?err
                    )
                })?;

            debug!(
                self.cluster,
                ?topition,
                deflated.producer_id,
                deflated.producer_epoch,
                current_epoch,
                sequence,
            );

            let increment = idempotent_sequence_check(&current_epoch, &sequence, deflated)?;

            debug!(increment);

            let _ = self
                .tx_prepare_execute(
                    tx,
                    "producer_detail_insert",
                    (
                        self.cluster.as_str(),
                        topition.topic(),
                        topition.partition(),
                        deflated.producer_id,
                        deflated.producer_epoch,
                        increment,
                        increment,
                    ),
                )
                .await?;

            Ok(())
        } else {
            Err(Error::Api(ErrorCode::UnknownProducerId))
        }
    }

    async fn watermark_select_for_update<'t>(
        &self,
        topition: &Topition,
        tx: &mut Transaction<'t>,
    ) -> Result<(Option<i64>, Option<i64>)> {
        let row: Option<(Option<i64>, Option<i64>)> = self
            .tx_prepare_query_opt(
                tx,
                "watermark_select_for_update",
                (
                    self.cluster.as_str(),
                    topition.topic(),
                    topition.partition(),
                ),
            )
            .await
            .inspect_err(|err| error!(?err, cluster = ?self.cluster, ?topition))?;

        if let Some((low, high)) = row {
            Ok((low, high))
        } else {
            Err(Error::Api(ErrorCode::UnknownTopicOrPartition))
        }
    }

    #[instrument(skip_all)]
    pub(crate) async fn produce_in_tx<'t>(
        &self,
        transaction_id: Option<&str>,
        topition: &Topition,
        deflated: deflated::Batch,
        tx: &mut Transaction<'t>,
    ) -> Result<i64> {
        debug!(cluster = ?self.cluster, ?transaction_id, ?topition, ?deflated);

        let topic = topition.topic();
        let partition = topition.partition();

        let topition_id: Option<i32> = self
            .tx_prepare_query_opt(
                tx,
                "topition_select_id",
                (self.cluster.as_str(), topic, partition),
            )
            .await
            .inspect_err(|err| debug!(?err))?;

        let Some(topition_id) = topition_id else {
            return Err(Error::Api(ErrorCode::UnknownTopicOrPartition));
        };

        debug!(topition_id);

        if deflated.is_idempotent() {
            self.idempotent_message_check(transaction_id, topition, &deflated, tx)
                .await
                .inspect_err(|err| error!(?err))?;
        }

        let (low, high) = self.watermark_select_for_update(topition, tx).await?;

        debug!(?low, ?high);

        let inflated = Batch::try_from(deflated).inspect_err(|err| error!(?err))?;

        let attributes = BatchAttribute::try_from(inflated.attributes)?;

        if !attributes.control
            && let Some(ref schemas) = self.schemas
        {
            schemas.validate(topition.topic(), &inflated).await?;
        }

        let last_offset_delta = i64::from(inflated.last_offset_delta);

        for (delta, record) in inflated.records.iter().enumerate() {
            let delta = i64::try_from(delta)?;
            let offset = high.unwrap_or_default() + delta;
            let record_attributes = inflated.attributes;
            let timestamp = to_system_time(inflated.base_timestamp + record.timestamp_delta)?;
            let chrono_timestamp: DateTime<Utc> = timestamp.into();

            let _ = self
                .tx_prepare_execute(
                    tx,
                    "record_insert",
                    (
                        topition_id,
                        offset,
                        record_attributes,
                        inflated.producer_id,
                        inflated.producer_epoch,
                        chrono_timestamp.naive_utc(),
                        record.key.as_ref().map(|b| b.to_vec()),
                        record.value.as_ref().map(|b| b.to_vec()),
                    ),
                )
                .await?;

            for header in record.headers.iter() {
                let _ = self
                    .tx_prepare_execute(
                        tx,
                        "header_insert",
                        (
                            topition_id,
                            offset,
                            header.key.as_ref().map(|b| b.to_vec()),
                            header.value.as_ref().map(|b| b.to_vec()),
                        ),
                    )
                    .await?;
            }
        }

        let new_high = high.unwrap_or_default() + last_offset_delta + 1;

        let _ = self
            .tx_prepare_execute(
                tx,
                "watermark_update",
                (new_high, self.cluster.as_str(), topic, partition),
            )
            .await?;

        if let Some(transaction_id) = transaction_id {
            let _ = self
                .tx_prepare_execute(
                    tx,
                    "txn_produce_offset_insert",
                    (
                        self.cluster.as_str(),
                        transaction_id,
                        inflated.producer_id,
                        inflated.producer_epoch,
                        topic,
                        partition,
                        high.unwrap_or_default(),
                        new_high - 1,
                    ),
                )
                .await?;
        }

        self.lake_store(&attributes, topition, high, &inflated)
            .await?;

        Ok(high.unwrap_or_default())
    }

    #[instrument(skip_all)]
    pub(crate) async fn end_txn_in_tx<'t>(
        &self,
        transaction_id: &str,
        producer_id: i64,
        producer_epoch: i16,
        committed: bool,
        tx: &mut Transaction<'t>,
    ) -> Result<ErrorCode> {
        debug!(
            cluster = self.cluster,
            transaction_id, producer_id, producer_epoch, committed
        );

        let rows: Vec<(String, i32)> = self
            .tx_prepare_query(
                tx,
                "txn_topition_select",
                (
                    self.cluster.as_str(),
                    transaction_id,
                    producer_id,
                    producer_epoch,
                ),
            )
            .await?;

        let mut overlaps = vec![];

        for (topic, partition) in rows {
            let topition = Topition::new(topic.clone(), partition);

            debug!(?topition);

            let control_batch: Bytes = if committed {
                ControlBatch::default().commit().try_into()?
            } else {
                ControlBatch::default().abort().try_into()?
            };
            let end_transaction_marker: Bytes = EndTransactionMarker::default().try_into()?;

            let batch = Batch::builder()
                .record(
                    Record::builder()
                        .key(control_batch.into())
                        .value(end_transaction_marker.into()),
                )
                .attributes(
                    BatchAttribute::default()
                        .control(true)
                        .transaction(true)
                        .into(),
                )
                .producer_id(producer_id)
                .producer_epoch(producer_epoch)
                .base_sequence(-1)
                .build()
                .and_then(TryInto::try_into)
                .inspect(|deflated| debug!(?deflated))?;

            let offset = self
                .produce_in_tx(Some(transaction_id), &topition, batch, tx)
                .await?;

            debug!(offset, ?topition);

            let (offset_start, offset_end): (i64, i64) = self
                .tx_prepare_query_one(
                    tx,
                    "txn_produce_offset_select_range",
                    (
                        self.cluster.as_str(),
                        transaction_id,
                        producer_id,
                        producer_epoch,
                        &topic,
                        partition,
                    ),
                )
                .await?;

            debug!(offset_start, offset_end);

            let txn_rows: Vec<(String, i64, i16, Option<String>)> = self
                .tx_prepare_query(
                    tx,
                    "txn_produce_offset_select_overlapping",
                    (
                        self.cluster.as_str(),
                        transaction_id,
                        producer_id,
                        producer_epoch,
                        &topic,
                        partition,
                        offset_end,
                    ),
                )
                .await?;

            for (name, pid, pep, status) in txn_rows {
                let status = status
                    .map_or(Ok(TxnState::Begin), TxnState::try_from)
                    .inspect(|txn| debug!(?txn))?;

                overlaps.push(Txn {
                    name,
                    producer_id: pid,
                    producer_epoch: pep,
                    status,
                });
            }
        }

        if overlaps.iter().all(|txn| txn.status.is_prepared()) {
            let txns = {
                let mut txns = Vec::with_capacity(overlaps.len() + 1);

                txns.append(&mut overlaps);

                txns.push(Txn {
                    name: transaction_id.into(),
                    producer_id,
                    producer_epoch,
                    status: if committed {
                        TxnState::PrepareCommit
                    } else {
                        TxnState::PrepareAbort
                    },
                });

                txns
            };

            debug!(?txns);

            for txn in txns {
                debug!(?txn);

                let _ = self
                    .tx_prepare_execute(
                        tx,
                        "txn_produce_offset_delete",
                        (
                            self.cluster.as_str(),
                            &txn.name,
                            txn.producer_id,
                            txn.producer_epoch,
                        ),
                    )
                    .await?;

                let _ = self
                    .tx_prepare_execute(
                        tx,
                        "txn_topition_delete",
                        (
                            self.cluster.as_str(),
                            &txn.name,
                            txn.producer_id,
                            txn.producer_epoch,
                        ),
                    )
                    .await?;

                if txn.status == TxnState::PrepareCommit {
                    let _ = self
                        .tx_prepare_execute(
                            tx,
                            "consumer_offset_insert_from_txn",
                            (
                                self.cluster.as_str(),
                                &txn.name,
                                txn.producer_id,
                                txn.producer_epoch,
                            ),
                        )
                        .await?;
                }

                let _ = self
                    .tx_prepare_execute(
                        tx,
                        "txn_offset_commit_tp_delete",
                        (
                            self.cluster.as_str(),
                            &txn.name,
                            txn.producer_id,
                            txn.producer_epoch,
                        ),
                    )
                    .await?;

                let _ = self
                    .tx_prepare_execute(
                        tx,
                        "txn_offset_commit_delete",
                        (
                            self.cluster.as_str(),
                            &txn.name,
                            txn.producer_id,
                            txn.producer_epoch,
                        ),
                    )
                    .await?;

                let outcome = if txn.status == TxnState::PrepareCommit {
                    String::from(TxnState::Committed)
                } else if txn.status == TxnState::PrepareAbort {
                    String::from(TxnState::Aborted)
                } else {
                    String::from(txn.status)
                };

                let _ = self
                    .tx_prepare_execute(
                        tx,
                        "txn_status_update",
                        (
                            &outcome,
                            self.cluster.as_str(),
                            &txn.name,
                            txn.producer_id,
                            txn.producer_epoch,
                        ),
                    )
                    .await?;
            }
        } else {
            debug!(?overlaps);

            let outcome = if committed {
                String::from(TxnState::PrepareCommit)
            } else {
                String::from(TxnState::PrepareAbort)
            };

            let _ = self
                .tx_prepare_execute(
                    tx,
                    "txn_status_update",
                    (
                        &outcome,
                        self.cluster.as_str(),
                        transaction_id,
                        producer_id,
                        producer_epoch,
                    ),
                )
                .await?;
        }

        Ok(ErrorCode::None)
    }

    #[instrument(skip_all)]
    async fn lake_store(
        &self,
        attributes: &BatchAttribute,
        topition: &Topition,
        high: Option<i64>,
        inflated: &Batch,
    ) -> Result<()> {
        if !attributes.control
            && let Some(ref lake) = self.lake
        {
            let config = self
                .describe_config(topition.topic(), ConfigResource::Topic, None)
                .await?;

            lake.store(
                topition.topic(),
                topition.partition(),
                high.unwrap_or_default(),
                inflated,
                config,
            )
            .await?;
        }

        Ok(())
    }
}
