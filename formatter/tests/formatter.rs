#[macro_use]
extern crate lazy_static;

use anyhow::{anyhow, Error};
use k9::assert_greater_than;
use pg_pretty_formatter::Formatter;
use pg_pretty_parser::parser;
use prettydiff::diff_lines;
use regex::Regex;
use std::{env, fs, path};

#[test]
fn test_all_cases() -> Result<(), Error> {
    let mut case_dir = env::current_dir()?;
    case_dir.push("tests");
    case_dir.push("cases");
    let mut file_count = 0;
    for entry in fs::read_dir(case_dir)? {
        let path = entry?.path();
        if path.is_file() {
            match path.extension() {
                Some(e) => {
                    if e != "tests" {
                        continue;
                    }
                }
                None => continue,
            };
            run_tests_from(path)?;
            file_count += 1;
        }
    }
    assert_greater_than!(file_count, 0, "found files to read");
    Ok(())
}

fn run_tests_from(p: path::PathBuf) -> Result<(), Error> {
    let mut only = String::new();
    if let Some(o) = env::var_os("PG_PRETTY_TEST_ONLY") {
        only = o.into_string().expect("env var is valid UTF-8");
    }

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
        case_count += 1;
        if !only.is_empty() && only != caps["name"] {
            continue;
        }
        test_one_case(&file, &caps["name"], &caps["input"], &caps["expect"])?;
    }

    assert_greater_than!(case_count, 0, format!("{} contains cases", file));
    Ok(())
}

fn test_one_case(file: &str, name: &str, input: &str, expect: &str) -> Result<(), Error> {
    let mut f = Formatter::new();
    match parser::parse_sql(input) {
        Err(e) => Err(anyhow!("Could not parse `{}` ({})", input, e)),
        Ok(p) => {
            let got = &f
                .format_root_stmt(&p[0])
                .map_err(|e| anyhow!("{}\n{:#?}", e, &p[0]))?;

            if expect != got {
                if env::var_os("PG_PRETTY_DEBUG_PARSE").is_some() {
                    println!("{:#?}", p);
                }

                let diff = diff_lines(expect, got);
                diff.names("expect", "got").prettytable();

                if env::var_os("PG_PRETTY_DEBUG_WS").is_some() {
                    println!("Expect");
                    println!(
                        "{}",
                        expect
                            .chars()
                            .map(|c| c.escape_debug().to_string())
                            .collect::<Vec<String>>()
                            .join("")
                    );
                    println!();
                    println!("Got");
                    println!(
                        "{}",
                        got.chars()
                            .map(|c| c.escape_debug().to_string())
                            .collect::<Vec<String>>()
                            .join("")
                    );
                }

                return Err(anyhow!("{}|{} failed", file, name));
            }
            Ok(())
        }
    }
}
