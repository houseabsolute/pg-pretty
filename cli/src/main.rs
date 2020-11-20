use eyre::Result;
use pg_pretty_formatter::Formatter;
use pg_pretty_parser::{ast::Root, parser};

fn main() -> Result<()> {
    let sql = r#"
SELECT p."CapName" AS product_name,
p.something,
order_qty * unit_price AS non_discount_sales,
order_qty * unit_price / arbitrary AS discounts
FROM "Foo"."Production"."Product" AS p 
  INNER JOIN sales.sales_order_detail AS sod
    ON p.product_id = sod.product_id 
    AND p.foo_id = sod.foo_id
  LEFT OUTER JOIN something USING (something_id)
  JOIN ( SELECT x FROM y ) AS yx ON yx.x = p.x
WHERE (((( (x = 1 AND y = 2) OR ( ( x = 3 OR  x = 4 ) AND y = 5 )))))
ORDER BY product_name DESC, size
LIMIT 10 OFFSET 1
"#;
    let res = parser::parse_sql(sql)?;
    let Root::RawStmt(stmt) = &res[0];
    let mut f = Formatter::new();
    println!("{:#?}", stmt);
    for stmt in &res {
        println!("{}", f.format_root_stmt(&stmt)?)
    }
    Ok(())
}
