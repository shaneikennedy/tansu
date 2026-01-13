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

use crate::Error;
use crate::TxnState;
use mysql_async::Row;
use tracing::error;

#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub(crate) struct Txn {
    pub(crate) name: String,
    pub(crate) producer_id: i64,
    pub(crate) producer_epoch: i16,
    pub(crate) status: TxnState,
}

impl TryFrom<Row> for Txn {
    type Error = Error;

    fn try_from(row: Row) -> Result<Self, Self::Error> {
        use mysql_async::prelude::FromRow;

        let (name, producer_id, producer_epoch, status): (String, i64, i16, Option<String>) =
            FromRow::from_row_opt(row).map_err(|e| Error::Message(e.to_string()))?;

        let status = status
            .map_or(Ok(TxnState::Begin), TxnState::try_from)
            .inspect_err(|err| error!(?err))?;

        Ok(Self {
            name,
            producer_id,
            producer_epoch,
            status,
        })
    }
}
