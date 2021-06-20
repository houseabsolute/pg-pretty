use eyre::Result;
//use pg_pretty_formatter::Transformer;
use pg_pretty_parser::{ast::Root, parser};

fn main() -> Result<()> {
    let sql = r#"
SELECT *
  FROM a 
       JOIN b USING (a_id)
       JOIN c USING (b_id)
"#;
    let res = parser::parse_sql(sql)?;
    let Root::RawStmt(stmt) = &res[0];
    //    let mut f = Transformer::new();
    println!("{:#?}", stmt);
    // for stmt in &res {
    //     println!("{}", f.format_root_stmt(&stmt)?)
    // }
    Ok(())
}
