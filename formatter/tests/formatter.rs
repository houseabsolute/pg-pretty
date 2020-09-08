#[macro_use]
extern crate lazy_static;

use anyhow::{anyhow, Error};
use pg_pretty_formatter::Formatter;
use pg_pretty_parser::parser;
use prettydiff::diff_lines;
use regex::Regex;
use spectral::prelude::*;
use std::{env, fs, path};

#[test]
fn test_all_cases() -> Result<(), Error> {
    let mut case_dir = env::current_dir()?;
    case_dir.push("test-cases");
    let mut file_count = 0;
    for entry in fs::read_dir(case_dir)? {
        let path = entry?.path();
        if path.is_file() {
            run_tests_from(path)?;
            file_count += 1;
        }
    }
    assert_that(&file_count)
        .named("number of files read")
        .is_not_equal_to(0);
    Ok(())
}

fn run_tests_from(p: path::PathBuf) -> Result<(), Error> {
    let cases = String::from_utf8(fs::read(p.clone())?)?;
    lazy_static! {
        static ref RE: Regex = Regex::new(
            r"(?msx)
\+{4}\n
(?P<name>.+?)\n
-{4}\n
(?P<input>.+?)\n
-{4}\n
(?P<expect>.+?\n)
-{4}\n
\n*
",
        )
        .expect("no error compiling regex");
    }

    let file = p
        .file_name()
        .expect("valid string in file name")
        .to_str()
        .expect("file name is valid utf-8");

    let mut case_count = 0;
    for caps in RE.captures_iter(&cases) {
        test_one_case(&file, &caps["name"], &caps["input"], &caps["expect"])?;
        case_count += 1;
    }

    assert_that(&case_count)
        .named(&format!("number of cases in {}", file))
        .is_not_equal_to(0);
    Ok(())
}

fn test_one_case(file: &str, name: &str, input: &str, expect: &str) -> Result<(), Error> {
    let mut f = Formatter::new();
    match parser::parse_sql(input) {
        Err(e) => {
            return Err(anyhow!("Could not parse `{}` ({})", input, e));
        }
        Ok(p) => {
            let formatted = &f.format_root_stmt(&p[0])?;
            if expect != formatted {
                let diff = diff_lines(expect, formatted);
                diff.names("expect", "got").prettytable();
                return Err(anyhow!("{}|{} failed", file, name));
            }
            Ok(())
        }
    }
}
