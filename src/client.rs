use std::io::Read;

use reqwest::{Client, Response, StatusCode};
use serde_json;
use serde_json::de::IoRead as SerdeIoRead;

use {ChunkedQuery, error, Node, Point, Points, Precision, Query, serialization};

use url::Url;

/// The client to influxdb
#[derive(Debug)]
pub struct InfluxClient {
    host: String,
    db: String,
    authentication: Option<(String, String)>,
    client: Client,
}

unsafe impl Send for InfluxClient {}

impl InfluxClient {
    /// Create a new influxdb client with http
    pub fn new<T>(host: T, db: T) -> Self
        where
            T: ToString,
    {
        let client = Client::builder().build().expect("Could not build client");

        InfluxClient {
            host: host.to_string(),
            db: db.to_string(),
            authentication: None,
            client,
        }
    }

    /// Change the client's database
    pub fn switch_database<T>(&mut self, database: T)
        where
            T: ToString,
    {
        self.db = database.to_string();
    }

    /// Change the client's user
    pub fn set_authentication<T>(mut self, user: T, passwd: T) -> Self
        where
            T: Into<String>,
    {
        self.authentication = Some((user.into(), passwd.into()));
        self
    }

    /// View the current db name
    pub fn get_db(&self) -> String {
        self.db.to_owned()
    }

    /// Query whether the corresponding database exists, return bool
    pub fn ping(&self) -> bool {
        let url = self.build_url("ping", None);
        if let Ok(res) = self.client.get(url).send() {
            match res.status() {
                StatusCode::OK => true,
                _ => false,
            }
        } else {
            false
        }
    }

    /// Write a point to the database
    pub fn write_point(
        &self,
        point: Point,
        precision: Option<Precision>,
        rp: Option<&str>,
    ) -> Result<(), error::Error> {
        let points = Points::new(point);
        self.write_points(points, precision, rp)
    }

    /// Write multiple points to the database
    pub fn write_points<T: Iterator<Item=Point>>(
        &self,
        points: T,
        precision: Option<Precision>,
        rp: Option<&str>,
    ) -> Result<(), error::Error> {
        let line = serialization::line_serialization(points);

        let mut param = vec![("db", self.db.as_str())];

        match precision {
            Some(ref t) => param.push(("precision", t.to_str())),
            None => param.push(("precision", "s")),
        };

        if let Some(t) = rp {
            param.push(("rp", t))
        }

        let url = self.build_url("write", Some(param));

        let mut res = self.client.post(url)
            .body(line)
            .send()?;
        let mut err = String::new();
        let _ = res.read_to_string(&mut err);

        match res.status() {
            StatusCode::OK | StatusCode::NO_CONTENT => Ok(()),
            StatusCode::BAD_REQUEST => Err(error::Error::SyntaxError(serialization::conversion(err.as_str()))),
            StatusCode::UNAUTHORIZED | StatusCode::FORBIDDEN => Err(error::Error::InvalidCredentials(
                "Invalid authentication credentials.".to_string(),
            )),
            StatusCode::NOT_FOUND => Err(error::Error::DataBaseDoesNotExist(
                serialization::conversion(err.as_str()),
            )),
            StatusCode::INTERNAL_SERVER_ERROR => Err(error::Error::RetentionPolicyDoesNotExist(err)),
            _ => Err(error::Error::Unknow("There is something wrong".to_string())),
        }
    }

    /// Query and return data, the data type is `Option<Vec<Node>>`
    pub fn query(
        &self,
        q: &str,
        epoch: Option<Precision>,
    ) -> Result<Option<Vec<Node>>, error::Error> {
        match self.query_raw(q, epoch) {
            Ok(t) => Ok(t.results),
            Err(e) => Err(e),
        }
    }

    /// Query and return data, the data type is `Option<Vec<Node>>`
    pub fn query_chunked(
        &self,
        q: &str,
        epoch: Option<Precision>,
    ) -> Result<ChunkedQuery<SerdeIoRead<Response>>, error::Error> {
        self.query_raw_chunked(q, epoch)
    }

    /// Drop measurement
    pub fn drop_measurement(&self, measurement: &str) -> Result<(), error::Error> {
        let sql = format!(
            "Drop measurement {}",
            serialization::quote_ident(measurement)
        );

        match self.query_raw(sql.as_str(), None) {
            Ok(_) => Ok(()),
            Err(e) => Err(e),
        }
    }

    /// Create a new database in InfluxDB.
    pub fn create_database(&self, dbname: &str) -> Result<(), error::Error> {
        let sql = format!("Create database {}", serialization::quote_ident(dbname));

        match self.query_raw(sql.as_str(), None) {
            Ok(_) => Ok(()),
            Err(e) => Err(e),
        }
    }

    /// Drop a database from InfluxDB.
    pub fn drop_database(&self, dbname: &str) -> Result<(), error::Error> {
        let sql = format!("Drop database {}", serialization::quote_ident(dbname));

        match self.query_raw(sql.as_str(), None) {
            Ok(_) => Ok(()),
            Err(e) => Err(e),
        }
    }

    /// Create a new user in InfluxDB.
    pub fn create_user(&self, user: &str, passwd: &str, admin: bool) -> Result<(), error::Error> {
        let sql: String = {
            if admin {
                format!(
                    "Create user {0} with password {1} with all privileges",
                    serialization::quote_ident(user),
                    serialization::quote_literal(passwd)
                )
            } else {
                format!(
                    "Create user {0} WITH password {1}",
                    serialization::quote_ident(user),
                    serialization::quote_literal(passwd)
                )
            }
        };

        match self.query_raw(sql.as_str(), None) {
            Ok(_) => Ok(()),
            Err(e) => Err(e),
        }
    }

    /// Drop a user from InfluxDB.
    pub fn drop_user(&self, user: &str) -> Result<(), error::Error> {
        let sql = format!("Drop user {}", serialization::quote_ident(user));

        match self.query_raw(sql.as_str(), None) {
            Ok(_) => Ok(()),
            Err(e) => Err(e),
        }
    }

    /// Change the password of an existing user.
    pub fn set_user_password(&self, user: &str, passwd: &str) -> Result<(), error::Error> {
        let sql = format!(
            "Set password for {}={}",
            serialization::quote_ident(user),
            serialization::quote_literal(passwd)
        );

        match self.query_raw(sql.as_str(), None) {
            Ok(_) => Ok(()),
            Err(e) => Err(e),
        }
    }

    /// Grant cluster administration privileges to a user.
    pub fn grant_admin_privileges(&self, user: &str) -> Result<(), error::Error> {
        let sql = format!(
            "Grant all privileges to {}",
            serialization::quote_ident(user)
        );

        match self.query_raw(sql.as_str(), None) {
            Ok(_) => Ok(()),
            Err(e) => Err(e),
        }
    }

    /// Revoke cluster administration privileges from a user.
    pub fn revoke_admin_privileges(&self, user: &str) -> Result<(), error::Error> {
        let sql = format!(
            "Revoke all privileges from {}",
            serialization::quote_ident(user)
        );

        match self.query_raw(sql.as_str(), None) {
            Ok(_) => Ok(()),
            Err(e) => Err(e),
        }
    }

    /// Grant a privilege on a database to a user.
    /// :param privilege: the privilege to grant, one of 'read', 'write'
    /// or 'all'. The string is case-insensitive
    pub fn grant_privilege(
        &self,
        user: &str,
        db: &str,
        privilege: &str,
    ) -> Result<(), error::Error> {
        let sql = format!(
            "Grant {} on {} to {}",
            privilege,
            serialization::quote_ident(db),
            serialization::quote_ident(user)
        );

        match self.query_raw(sql.as_str(), None) {
            Ok(_) => Ok(()),
            Err(e) => Err(e),
        }
    }

    /// Revoke a privilege on a database from a user.
    /// :param privilege: the privilege to grant, one of 'read', 'write'
    /// or 'all'. The string is case-insensitive
    pub fn revoke_privilege(
        &self,
        user: &str,
        db: &str,
        privilege: &str,
    ) -> Result<(), error::Error> {
        let sql = format!(
            "Revoke {0} on {1} from {2}",
            privilege,
            serialization::quote_ident(db),
            serialization::quote_ident(user)
        );

        match self.query_raw(sql.as_str(), None) {
            Ok(_) => Ok(()),
            Err(e) => Err(e),
        }
    }

    /// Create a retention policy for a database.
    /// :param duration: the duration of the new retention policy.
    ///  Durations such as 1h, 90m, 12h, 7d, and 4w, are all supported
    ///  and mean 1 hour, 90 minutes, 12 hours, 7 day, and 4 weeks,
    ///  respectively. For infinite retention – meaning the data will
    ///  never be deleted – use 'INF' for duration.
    ///  The minimum retention period is 1 hour.
    pub fn create_retention_policy(
        &self,
        name: &str,
        duration: &str,
        replication: &str,
        default: bool,
        db: Option<&str>,
    ) -> Result<(), error::Error> {
        let database = {
            if let Some(t) = db {
                t
            } else {
                &self.db
            }
        };

        let sql: String = {
            if default {
                format!(
                    "Create retention policy {} on {} duration {} replication {} default",
                    serialization::quote_ident(name),
                    serialization::quote_ident(database),
                    duration,
                    replication
                )
            } else {
                format!(
                    "Create retention policy {} on {} duration {} replication {}",
                    serialization::quote_ident(name),
                    serialization::quote_ident(database),
                    duration,
                    replication
                )
            }
        };

        match self.query_raw(sql.as_str(), None) {
            Ok(_) => Ok(()),
            Err(e) => Err(e),
        }
    }

    /// Drop an existing retention policy for a database.
    pub fn drop_retention_policy(&self, name: &str, db: Option<&str>) -> Result<(), error::Error> {
        let database = {
            if let Some(t) = db {
                t
            } else {
                &self.db
            }
        };

        let sql = format!(
            "Drop retention policy {} on {}",
            serialization::quote_ident(name),
            serialization::quote_ident(database)
        );

        match self.query_raw(sql.as_str(), None) {
            Ok(_) => Ok(()),
            Err(e) => Err(e),
        }
    }

    fn send_request(
        &self,
        q: &str,
        epoch: Option<Precision>,
        chunked: bool,
    ) -> Result<Response, error::Error> {
        let mut param = vec![("db", self.db.as_str()), ("q", q)];

        if let Some(ref t) = epoch {
            param.push(("epoch", t.to_str()))
        }

        if chunked {
            param.push(("chunked", "true"));
        }

        let url = self.build_url("query", Some(param));

        let q_lower = q.to_lowercase();
        let mut res = {
            if q_lower.starts_with("select") && !q_lower.contains("into")
                || q_lower.starts_with("show")
            {
                self.client.get(url).send()?
            } else {
                self.client.post(url).send()?
            }
        };

        println!("Status is: {:?}", res.status());
        match res.status() {
            StatusCode::OK | StatusCode::NO_CONTENT => Ok(res),
            StatusCode::BAD_REQUEST => {
                let mut context = String::new();
                let _ = res.read_to_string(&mut context);
                let json_data: Query = serde_json::from_str(context.as_str()).unwrap();

                Err(error::Error::SyntaxError(serialization::conversion(
                    json_data.error.unwrap().as_str(),
                )))
            }
            StatusCode::UNAUTHORIZED | StatusCode::FORBIDDEN => Err(error::Error::InvalidCredentials(
                "Invalid authentication credentials.".to_string(),
            )),
            _ => Err(error::Error::Unknow("There is something wrong".to_string())),
        }
    }

    /// Query and return to the native json structure
    fn query_raw(&self, q: &str, epoch: Option<Precision>) -> Result<Query, error::Error> {
        let mut response = self.send_request(q, epoch, false)?;

        let mut context = String::new();
        let _ = response.read_to_string(&mut context);

        let json_data: Query = serde_json::from_str(context.as_str()).unwrap();
        Ok(json_data)
    }

    /// Query and return to the native json structure
    fn query_raw_chunked(
        &self,
        q: &str,
        epoch: Option<Precision>,
    ) -> Result<ChunkedQuery<SerdeIoRead<Response>>, error::Error> {
        let response = self.send_request(q, epoch, true)?;
        let stream = serde_json::Deserializer::from_reader(response).into_iter::<Query>();
        Ok(stream)
    }

    /// Constructs the full URL for an API call.
    fn build_url(&self, key: &str, param: Option<Vec<(&str, &str)>>) -> Url {
        let url = Url::parse(&self.host).unwrap().join(key).unwrap();

        let mut authentication = Vec::new();

        if let Some(ref t) = self.authentication {
            authentication.push(("u", &t.0));
            authentication.push(("p", &t.1));
        }

        let url = Url::parse_with_params(url.as_str(), authentication).unwrap();

        if param.is_some() {
            Url::parse_with_params(url.as_str(), param.unwrap()).unwrap()
        } else {
            url
        }
    }
}

impl Default for InfluxClient {
    /// connecting for default database `test` and host `http://localhost:8086`
    fn default() -> Self {
        InfluxClient::new("http://localhost:8086", "test")
    }
}
