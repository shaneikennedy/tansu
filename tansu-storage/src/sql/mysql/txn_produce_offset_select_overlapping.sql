-- -*- mode: sql; sql-product: mysql; -*-
-- Copyright ⓒ 2024-2025 Peter Morgan <peter.james.morgan@gmail.com>
--
-- Licensed under the Apache License, Version 2.0 (the "License");
-- you may not use this file except in compliance with the License.
-- You may obtain a copy of the License at
--
-- http://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing, software
-- distributed under the License is distributed on an "AS IS" BASIS,
-- WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
-- See the License for the specific language governing permissions and
-- limitations under the License.

SELECT txn, producer_id, producer_epoch, status FROM txn_produce_offset tpo JOIN txn t ON tpo.cluster = t.cluster AND tpo.txn = t.name WHERE tpo.cluster = ? AND NOT (tpo.txn = ? AND tpo.producer_id = ? AND tpo.producer_epoch = ?) AND tpo.topic = ? AND tpo.partition = ? AND tpo.offset_start <= ?
