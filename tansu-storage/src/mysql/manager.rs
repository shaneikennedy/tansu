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

//! Deadpool Manager implementation for mysql_async

use deadpool::managed::{Manager, Metrics, RecycleError, RecycleResult};
use mysql_async::{Conn, Opts, Pool as MysqlPool, prelude::*};

#[derive(Debug)]
pub struct MysqlManager {
    pool: MysqlPool,
}

impl MysqlManager {
    pub fn new(opts: Opts) -> Self {
        Self {
            pool: MysqlPool::new(opts),
        }
    }

    pub fn from_url(url: &str) -> Result<Self, mysql_async::UrlError> {
        let opts = Opts::from_url(url)?;
        Ok(Self::new(opts))
    }
}

impl Manager for MysqlManager {
    type Type = Conn;
    type Error = mysql_async::Error;

    async fn create(&self) -> Result<Conn, Self::Error> {
        self.pool.get_conn().await
    }

    async fn recycle(&self, conn: &mut Conn, _metrics: &Metrics) -> RecycleResult<Self::Error> {
        conn.query_drop("SELECT 1")
            .await
            .map_err(|_| RecycleError::message("Connection health check failed"))
    }
}

impl Drop for MysqlManager {
    fn drop(&mut self) {
        let pool = self.pool.clone();
        let _ = tokio::spawn(async move {
            let _ = pool.disconnect().await;
        });
    }
}
