use anyhow::{anyhow, bail, Result};

pub struct SetParams {
    pub(crate) key: String,
    pub(crate) value: String,
    pub(crate) expiry: Option<i64>,
}

pub enum Command {
    Ping,
    Echo(String),
    Get(String),
    Set(SetParams),
}

impl Command {
    pub(crate) fn parse_redis_format(incoming_str: &str) -> Result<Self> {
        let parts = incoming_str.split("\r\n").collect::<Vec<&str>>();
        let cmd = parts.get(2).ok_or_else(|| anyhow!("Command not found"))?;


        match cmd.to_uppercase().as_str() {
            "PING" => Ok(Self::Ping),
            "ECHO" => {
                let msg = parts.get(4).ok_or_else(|| anyhow!("Missing echo value"))?;
                Ok(Self::Echo(msg.to_string()))
            }
            "GET" => {
                let key = parts.get(4).ok_or_else(|| anyhow!("Missing key"))?;
                Ok(Self::Get(key.to_string()))
            }
            "SET" => {
                let key = parts.get(4).ok_or_else(|| anyhow!("Missing key"))?.to_string();
                let value = parts.get(6).ok_or_else(|| anyhow!("Missing value"))?.to_string();
                let expiry = parts.get(10).and_then(|s| s.parse::<i64>().ok());
                Ok(Self::Set(SetParams { key, value, expiry }))
            }
            _ => bail!("Incorrect RESP message")
        }
    }
}
