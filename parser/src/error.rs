use serde::Serialize;
use std::fmt;
use thiserror::Error;

#[derive(Debug, PartialEq, Serialize)]
pub struct ParseError {
    pub message: Option<String>,
    pub funcname: Option<String>,
    pub filename: Option<String>,
    pub lineno: i32,
    pub cursorpos: i32,
    pub context: Option<String>,
}

impl fmt::Display for ParseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let e = if let Some(m) = &self.message {
            &m
        } else {
            "no message returned from parse"
        };

        write!(f, "{}", e)
    }
}

#[derive(Debug, Error, PartialEq, Serialize)]
pub enum PgQueryError {
    #[error("could not parse C string into Rust UTF-8 string")]
    ParsingCString,
    #[error("could not parse JSON from libpg_query: {0}\n{1}")]
    JsonParse(String, String),
    #[error("could not convert query string to C string")]
    QueryToCString,
    #[error("libpg_query returned an error from parsing the string: {0}")]
    PgParseError(ParseError),
}

impl std::convert::From<std::ffi::NulError> for PgQueryError {
    fn from(_: std::ffi::NulError) -> Self {
        Self::QueryToCString
    }
}

impl std::convert::From<serde_json::error::Error> for PgQueryError {
    fn from(e: serde_json::error::Error) -> Self {
        Self::JsonParse("{}".to_string(), e.to_string())
    }
}

impl std::convert::From<std::ffi::IntoStringError> for PgQueryError {
    fn from(_: std::ffi::IntoStringError) -> Self {
        Self::ParsingCString
    }
}

impl std::convert::From<std::str::Utf8Error> for PgQueryError {
    fn from(_: std::str::Utf8Error) -> Self {
        Self::ParsingCString
    }
}
