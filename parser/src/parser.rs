// The code in this module is a very lightly edited version of the code in the
// squawk-parser crate.
use crate::ast;
use crate::error::{PGQueryError, ParseError};
use libpg_query::{pg_query_free_parse_result, pg_query_parse};
use std::ffi::{CStr, CString};

pub fn parse_sql(sql: &str) -> Result<Vec<ast::Root>, PGQueryError> {
    let c_str = CString::new(sql)?;
    let pg_parse_result = unsafe { pg_query_parse(c_str.as_ptr()) };

    if !pg_parse_result.error.is_null() {
        let err = unsafe { *pg_parse_result.error };
        let parse_error = ParseError {
            message: c_ptr_to_string(err.message),
            funcname: c_ptr_to_string(err.funcname),
            filename: c_ptr_to_string(err.filename),
            lineno: err.lineno,
            cursorpos: err.cursorpos,
            context: c_ptr_to_string(err.context),
        };
        return Err(PGQueryError::PGParseError(parse_error));
    }

    // not sure if this is ever null, but might as well check
    if pg_parse_result.parse_tree.is_null() {
        return Err(PGQueryError::ParsingCString);
    }

    let parse_tree = unsafe { CStr::from_ptr(pg_parse_result.parse_tree) }.to_str()?;
    let output =
        serde_json::from_str(parse_tree).map_err(|e| PGQueryError::JsonParse(e.to_string()));

    unsafe {
        pg_query_free_parse_result(pg_parse_result);
    };

    output
}

fn c_ptr_to_string(str_ptr: *mut ::std::os::raw::c_char) -> Option<String> {
    if str_ptr.is_null() {
        None
    } else {
        unsafe { CStr::from_ptr(str_ptr) }
            .to_str()
            .ok()
            .map(|s| s.to_owned())
    }
}
