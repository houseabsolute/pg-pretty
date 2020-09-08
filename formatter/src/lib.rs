use pg_pretty_parser::ast::*;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
    #[error("unexpected node {:?} in {}", .node, .func)]
    UnexpectedNode { node: String, func: String },
    #[error("no target list for select")]
    NoTargetListForSelect,
    #[error("infix expression is {}", e)]
    MalformedInfixExpression { e: String },
    #[error("join type is {} but there is no Pg keyword for this", jt)]
    InexpressibleJoinType { jt: String },
    #[error(
        "cannot mix join strategies but found {} and {} in the FROM clause",
        strat1,
        strat2
    )]
    CannotMixJoinStrategies { strat1: String, strat2: String },
    #[error("order by clause had a USING without an op")]
    OrderByUsingWithoutOp,
}

struct Context {
    indent: usize,
}

pub struct Formatter {
    a_expr_depth: u8,
    bool_expr_depth: u8,
    max_line_length: usize,
    indent_width: u8,
    contexts: Vec<Context>,
}

type R = Result<String, Error>;

impl Formatter {
    pub fn new() -> Self {
        Formatter {
            a_expr_depth: 0,
            bool_expr_depth: 0,
            max_line_length: 100,
            indent_width: 4,
            contexts: vec![Context { indent: 0 }],
        }
    }

    pub fn format_root_stmt(&mut self, root: &Root) -> R {
        match root {
            Root::RawStmt(raw) => Ok(self.format_node(&raw.stmt)?),
        }
    }

    fn format_node(&mut self, node: &Node) -> R {
        match &node {
            Node::SelectStmt(s) => Ok(self.format_select_stmt(&s)?),
            Node::ColumnRef(c) => Ok(self.format_column_ref(&c)?),
            Node::AConst(a) => Ok(self.format_a_const(&a)?),
            Node::AExpr(a) => Ok(self.format_a_expr(&a)?),
            Node::BoolExpr(b) => Ok(self.format_bool_expr(&b)?),
            Node::FuncCall(f) => Ok(self.format_func_call(&f)?),
            Node::StringStruct(s) => Ok(self.quote_string(&s.str)),
            _ => Err(Error::UnexpectedNode {
                node: node.to_string(),
                func: "format_node".to_string(),
            }),
        }
    }

    fn format_select_stmt(&mut self, s: &SelectStmt) -> R {
        let t = match &s.target_list {
            Some(tl) => tl,
            None => return Err(Error::NoTargetListForSelect),
        };
        let mut select = self.format_select_clause(t)?;
        if let Some(f) = &s.from_clause {
            select.push_str(&self.format_from_clause(f)?);
        }
        if let Some(w) = &s.where_clause {
            select.push_str(&self.format_where_clause(w)?);
        }
        // if let Some(g) = &s.group_clause {
        //     select.push_str(&self.format_group_by_clause(g)?);
        // }
        // if let Some(h) = &s.having_clause {
        //     select.push_str(&self.format_having_clause(h)?);
        // }
        if let Some(o) = &s.sort_clause {
            select.push_str(&self.format_order_by_clause(o)?);
        }

        Ok(select)
    }

    fn format_select_clause(&mut self, tl: &[Node]) -> R {
        let mut targets: Vec<String> = vec![];
        let mut can_be_one_line = true;
        for t in tl {
            if self.is_complex_target_element(t)? {
                can_be_one_line = false;
            }
            targets.push(self.format_target_element(t)?);
        }

        let mut select = self.indent_str("SELECT ");
        self.push_context_from_str(&select);
        let mut is_one_line = false;
        if can_be_one_line {
            let one_line = targets.join(", ");
            if self.fits_on_one_line(&one_line) {
                select.push_str(&one_line);
                select.push_str("\n");
                is_one_line = true;
            }
        }

        if !is_one_line {
            let last = targets.len();
            for (n, t) in targets.iter().enumerate() {
                if n != 0 {
                    select.push_str(&" ".repeat(self.current_indent()));
                }
                select.push_str(&t);
                if n < last - 1 {
                    select.push_str(",");
                }
                select.push_str("\n");
            }
        }

        self.pop_context();

        Ok(select)
    }

    fn is_complex_target_element(&mut self, t: &Node) -> Result<bool, Error> {
        match &t {
            Node::ResTarget(rt) => match *rt.val {
                Node::SubLink(_) => Ok(true),
                _ => Ok(false),
            },
            _ => Err(Error::UnexpectedNode {
                node: t.to_string(),
                func: "is_complex_target_element".to_string(),
            }),
        }
    }

    fn format_target_element(&mut self, t: &Node) -> R {
        match &t {
            Node::ResTarget(rt) => self.format_res_target(rt),
            _ => Err(Error::UnexpectedNode {
                node: t.to_string(),
                func: "format_target_element".to_string(),
            }),
        }
    }

    fn format_res_target(&mut self, rt: &ResTarget) -> R {
        let mut f = String::new();
        f.push_str(&self.format_node(&rt.val)?);
        if let Some(n) = &rt.name {
            f.push_str(&Self::alias_name(&n));
        }

        Ok(f)
    }

    fn format_where_clause(&mut self, w: &Node) -> R {
        let mut wh = self.indent_str("WHERE ");

        self.push_context_from_str(&wh);
        wh.push_str(&self.format_node(w)?);
        self.pop_context();

        wh.push_str("\n");

        Ok(wh)
    }

    fn format_column_ref(&mut self, c: &ColumnRef) -> R {
        let mut cols: Vec<String> = vec![];
        for f in &c.fields {
            match f {
                ColumnRefField::StringStruct(s) => cols.push(Self::maybe_quote(&s.str)),
                ColumnRefField::AStar(_) => cols.push("*".to_string()),
            }
        }

        Ok(cols.join("."))
    }

    fn format_a_const(&mut self, a: &AConst) -> R {
        match &a.val {
            Value::StringStruct(s) => Ok(self.quote_string(&s.str)),
            Value::BitString(s) => Ok(self.quote_string(&s.str)),
            Value::Integer(i) => Ok(i.ival.to_string()),
            Value::Float(f) => Ok(f.str.clone()),
            Value::Null(_) => Ok("NULL".to_string()),
        }
    }

    fn format_a_expr(&mut self, a: &AExpr) -> R {
        self.a_expr_depth += 1;
        let e = match a.kind {
            AExprKind::AExprOp => self.format_infix_expr(&a.lexpr, a.name.as_ref(), &a.rexpr)?,
            AExprKind::AExprIn => format!("{} IN ( {} )", self.format_node(&a.lexpr)?, "?"),
            _ => "aexpr".to_string(),
        };
        self.a_expr_depth -= 1;
        Ok(e)
    }

    // If a bool expr is just a series of "AND" clauses, then we get one
    // BoolExpr with many args. If there is a mix of "AND" and "OR", then the
    // args will contain nested BoolExpr clauses. So if our depth is > 1 then
    // we need to add wrapping parens.
    fn format_bool_expr(&mut self, b: &BoolExpr) -> R {
        let op = match b.boolop {
            BoolExprType::AndExpr => "AND",
            BoolExprType::OrExpr => "OR",
        };

        self.bool_expr_depth += 1;

        let mut formatted = b
            .args
            .iter()
            .map(|n| self.format_node(&n))
            .collect::<Result<Vec<_>, _>>()?;

        let mut expr = String::new();
        // If we're inside a nested boolean expression we start adding parens
        // everywhere rather than trying to be clever about it. The end result
        // is more consistent and readable that way, and it makes the
        // formatter code much simpler too.
        if self.bool_expr_depth > 1 {
            expr.push_str("(\n");
            // If we're multiple levels in we need to indent once per level,
            // not just once total. For example, if we are two levels deep
            // "((x OR y)...)", we need to indent twice to get the result we
            // want.
            for _ in 1..self.bool_expr_depth {
                self.push_context_one_level();
            }
        };

        // The first level of bool expr will be immediately after a "WHERE" or
        // an "ON", and so does not need additional indentation.
        if self.bool_expr_depth == 1 {
            expr.push_str(&formatted.remove(0));
        } else {
            expr.push_str(&self.indent_str(&formatted.remove(0)));
        }
        expr.push_str("\n");

        let last = formatted.len();
        for (n, f) in formatted.iter().enumerate() {
            expr.push_str(&self.indent_str(&op));
            expr.push_str(" ");
            expr.push_str(&f);
            if n < last - 1 {
                expr.push_str("\n");
            }
        }

        // If we're closing out a nested boolean, we want to ...
        if self.bool_expr_depth > 1 {
            // add a newline
            expr.push_str("\n");
            // outdent one level
            self.pop_context();
            // add our closing delimiter
            expr.push_str(&self.indent_str(")"));
            self.bool_expr_depth -= 1;
            // outdent back to 1 to match what we did earlier.
            for _ in 1..self.bool_expr_depth {
                self.pop_context();
            }
        } else {
            self.bool_expr_depth -= 1;
        }

        Ok(expr)
    }

    fn format_infix_expr(&mut self, left: &Node, op: Option<&List>, right: &OneOrManyNodes) -> R {
        let mut e: Vec<String> = vec![];
        e.push(self.format_node(&left)?);
        match op {
            Some(o) => match o.len() {
                1 => match &o[0] {
                    Node::StringStruct(s) => e.push(s.str.clone()),
                    _ => {
                        return Err(Error::MalformedInfixExpression {
                            e: "infix op is not a string".to_string(),
                        })
                    }
                },
                _ => {
                    return Err(Error::MalformedInfixExpression {
                        e: format!("infix op is a list with {} elements", o.len()),
                    })
                }
            },
            None => {
                return Err(Error::MalformedInfixExpression {
                    e: "infix op is empty".to_string(),
                })
            }
        }
        match right {
            OneOrManyNodes::One(r) => e.push(self.format_node(&r)?),
            // XXX - Is this right? The right side could be the right side of
            // something like "foo IN (1, 2)", but could it also be something
            // else that shouldn't be joined by commas?
            OneOrManyNodes::Many(r) => {
                e.push("(".to_string());
                e.push(
                    r.iter()
                        .map(|n| self.format_node(&n))
                        .collect::<Result<Vec<_>, _>>()?
                        .join(", "),
                );
                e.push(")".to_string());
            }
        }
        if self.a_expr_depth > 1 {
            e.insert(0, "(".to_string());
            e.push(")".to_string());
        }
        Ok(e.join(" "))
    }

    fn format_func_call(&mut self, f: &FuncCall) -> R {
        // There are a number of special case "functions" like "thing AT TIME
        // ZONE ..." and "thing LIKE foo ESCAPE bar" that need to be handled
        // differently.

        // XXX - are multiple list elements always going to be catalog and
        // schema?
        let mut func = f
            .funcname
            .iter()
            .map(|n| match n {
                // We don't want to quote a string here.
                Node::StringStruct(s) => Ok(s.str.to_uppercase().to_owned()),
                _ => self.format_node(&n),
            })
            .collect::<Result<Vec<_>, _>>()?
            .join(" ");
        func.push_str("(");

        let mut arg_is_simple = true;
        let mut args = String::new();
        if f.agg_distinct {
            arg_is_simple = false;
            args.push_str("DISTINCT ");
        }

        // We'll ignore f.func_variadic since it's optional.
        if f.agg_star {
            args.push_str("*");
        }

        match &f.args {
            Some(a) => {
                let f = a
                    .iter()
                    .map(|n| self.format_node(n))
                    .collect::<Result<Vec<_>, _>>()?
                    .join(", ");
                if f.contains(&[' ', '('][..]) {
                    arg_is_simple = false;
                }
                args.push_str(&f);
            }
            None => (),
        }

        if let Some(o) = &f.agg_order {
            arg_is_simple = false;
            args.push_str(&self.format_order_by_clause(o)?);
        }

        if let Some(fil) = &f.agg_filter {
            arg_is_simple = false;
            args.push_str(" FILTER ");
            args.push_str(&self.format_node(fil)?);
        }

        if let Some(w) = &f.over {
            arg_is_simple = false;
            args.push_str(" OVER ");
            args.push_str(&self.format_window_def(&w)?);
        }

        // XXX need to handle f.agg_within_group

        if !arg_is_simple {
            func.push_str(" ");
            func.push_str(&args);
            func.push_str(" ");
        } else {
            func.push_str(&args);
        }

        func.push_str(")");

        Ok(func)
    }

    // XXX - to be implemented
    fn format_window_def(&mut self, w: &WindowDefWrapper) -> R {
        Ok("WINDOW".to_string())
    }

    fn format_from_clause(&mut self, fc: &[Node]) -> R {
        let mut from = self.indent_str("FROM ");

        self.push_context_from_str(&from);
        for (n, f) in fc.iter().enumerate() {
            from.push_str(&self.format_from_element(&f, n == 0)?);
        }
        self.pop_context();

        from.push_str("\n");

        Ok(from)
    }

    fn format_from_element(&mut self, f: &Node, is_first: bool) -> R {
        match f {
            Node::JoinExpr(j) => Ok(self.format_join_expr(&j, is_first)?),
            Node::RangeVar(r) => Ok(self.format_range_var(&r)),
            Node::RangeSubselect(sub) => Ok(self.format_subselect(&sub)?),
            _ => Ok("from_element".to_string()),
        }
    }

    fn format_join_expr(&mut self, j: &JoinExpr, is_first: bool) -> R {
        let mut e = String::new();
        if !is_first {
            e.push_str(&" ".repeat(self.current_indent()));
        }

        if j.is_natural {
            if j.quals.is_some() {
                return Err(Error::CannotMixJoinStrategies {
                    strat1: "NATURAL".to_string(),
                    strat2: "ON".to_string(),
                });
            }
            if j.using_clause.is_some() {
                return Err(Error::CannotMixJoinStrategies {
                    strat1: "NATURAL".to_string(),
                    strat2: "USING".to_string(),
                });
            }
        }
        if j.quals.is_some() && j.using_clause.is_some() {
            return Err(Error::CannotMixJoinStrategies {
                strat1: "ON".to_string(),
                strat2: "USING".to_string(),
            });
        }

        e.push_str(&self.format_from_element(&j.larg, is_first)?);
        if j.is_natural {
            e.push_str("NATURAL");
        }
        e.push_str("\n");
        e.push_str(&" ".repeat(self.current_indent()));
        e.push_str(self.join_type(&j.jointype)?);
        e.push_str(" ");
        e.push_str(&self.format_from_element(&j.rarg, is_first)?);

        if let Some(q) = &j.quals {
            // For now, we'll just always put a newline before the "ON ..."
            // clause. This simplifies the formatting and makes it consistent
            // with how we format WHERE clauses.
            e.push_str("\n");
            self.push_context_one_level();
            e.push_str(&self.indent_str("ON "));
            e.push_str(&self.format_node(q)?);
            self.pop_context();
        }
        if let Some(u) = &j.using_clause {
            let using = format!("USING {}", self.format_using_clause(u));
            // + 1 for space before "USING"
            if self.len_after_nl(&e) + using.len() + 1 > self.max_line_length {
                e.push_str("\n");
                self.push_context_one_level();
                e.push_str(&self.indent_str(&using));
                self.pop_context();
            } else {
                e.push_str(" ");
                e.push_str(&using);
            }
        }

        Ok(e)
    }

    fn join_type(&self, jt: &JoinType) -> Result<&str, Error> {
        match jt {
            JoinType::JoinInner => Ok("JOIN"),
            JoinType::JoinLeft => Ok("LEFT OUTER JOIN"),
            JoinType::JoinRight => Ok("RIGHT OUTER JOIN"),
            JoinType::JoinFull => Ok("FULL OUTER JOIN"),
            _ => Err(Error::InexpressibleJoinType { jt: jt.to_string() }),
        }
    }

    fn format_using_clause(&self, using: &[StringStructWrapper]) -> String {
        let mut c = "(".to_string();
        if using.len() > 1 {
            c.push_str(" ");
        }
        c.push_str(
            using
                .iter()
                .map(|StringStructWrapper::StringStruct(u)| u.str.clone())
                .collect::<Vec<String>>()
                .join(", ")
                .as_str(),
        );
        if using.len() > 1 {
            c.push_str(" ");
        }
        c.push_str(")");
        c
    }

    fn format_range_var(&self, r: &RangeVar) -> String {
        let mut names: Vec<String> = vec![];
        if let Some(c) = &r.catalogname {
            names.push(c.clone());
        }
        if let Some(s) = &r.schemaname {
            names.push(s.clone());
        }
        names.push(r.relname.clone());

        let mut e = names
            .iter()
            .map(Self::maybe_quote)
            .collect::<Vec<String>>()
            .join(".");

        if let Some(AliasWrapper::Alias(a)) = &r.alias {
            e.push_str(&Self::alias_name(&a.aliasname));
            // XXX - do something with colnames here?
        }

        e
    }

    fn format_order_by_clause(&mut self, order: &Vec<SortByWrapper>) -> R {
        let mut ob: Vec<String> = vec![];
        for SortByWrapper::SortBy(s) in order {
            let mut el = self.format_node(&s.node)?;
            match s.sortby_dir {
                SortByDir::SortbyDefault => (),
                SortByDir::SortbyAsc => el.push_str(" ASC"),
                SortByDir::SortbyDesc => el.push_str(" DESC"),
                SortByDir::SortbyUsing => {
                    el.push_str(" USING ");
                    match &s.use_op {
                        Some(u) => {
                            el.push_str(
                                &u.iter()
                                    .map(|a| self.format_node(a))
                                    .collect::<Result<Vec<_>, _>>()?
                                    .join(" "),
                            );
                        }
                        None => return Err(Error::OrderByUsingWithoutOp),
                    }
                }
            }
            match s.sortby_nulls {
                SortByNulls::SortbyNullsDefault => (),
                SortByNulls::SortbyNullsFirst => el.push_str(" NULLS FIRST"),
                SortByNulls::SortbyNullsLast => el.push_str(" NULLS LAST"),
            }
            ob.push(el);
        }

        let mut order = "ORDER BY ".to_string();
        let one_line = ob.join(", ");
        if self.current_indent() + order.len() + one_line.len() > self.max_line_length {
            self.push_context_from_str(&order);
            order.push_str(&ob.remove(0));
            order.push_str("\n");
            order.push_str(
                &ob.iter()
                    .map(|s| {
                        let mut i = self.indent_str(s);
                        i.push_str("\n");
                        i
                    })
                    .collect::<Vec<String>>()
                    .join(""),
            )
        } else {
            order.push_str(&one_line);
        }

        Ok(order)
    }

    fn format_subselect(&mut self, sub: &RangeSubselect) -> R {
        // We make the indent 0, get the subselect, and then reindent it. This
        // is a lot simpler than trying to propogate the right context into
        // the subselect formatting.
        self.push_context_with_indent(0);
        let SelectStmtWrapper::SelectStmt(stmt) = &*sub.subquery;
        // The select will end with a newline, but we want to remove that,
        // then wrap the whole thing in parens, at which point we'll add the
        // trailing newline back.
        let formatted = &self.format_select_stmt(&stmt)?.trim_end().to_string();
        self.pop_context();

        self.push_context_one_level();
        let mut s = "(\n".to_string();
        s.push_str(&self.indent_multiline_str(formatted));
        self.pop_context();

        s.push_str("\n");
        s.push_str(&self.indent_str(")"));
        if let Some(AliasWrapper::Alias(a)) = &sub.alias {
            s.push_str(&Self::alias_name(&a.aliasname));
        }

        Ok(s)
    }

    fn push_context_from_str(&mut self, s: &str) {
        let mut indent = s.len();
        if let Some(c) = self.contexts.last() {
            indent += c.indent;
        }
        self.contexts.push(Context { indent });
    }

    fn push_context_one_level(&mut self) {
        self.contexts.push(Context {
            indent: self.current_indent() + self.indent_width as usize,
        });
    }

    fn push_context_with_indent(&mut self, indent: usize) {
        self.contexts.push(Context { indent });
    }

    fn pop_context(&mut self) {
        if self.contexts.is_empty() {
            panic!("No more contexts to pop!");
        }
        self.contexts.pop();
    }

    fn len_after_nl(&self, s: &str) -> usize {
        if let Some(nl) = s.rfind('\n') {
            return s.len() - nl;
        }
        s.len()
    }

    fn fits_on_one_line(&self, line: &str) -> bool {
        self.current_indent() + line.len() < self.max_line_length
    }

    fn quote_string(&self, s: &str) -> String {
        format!("'{}'", s.replace("'", "''"))
    }

    fn indent_str(&self, s: &str) -> String {
        let mut indented = " ".repeat(self.current_indent());
        indented.push_str(s);
        indented
    }

    fn indent_multiline_str(&self, s: &str) -> String {
        let mut indented = " ".repeat(self.current_indent());
        indented.push_str(s);

        let mut replacement = "\n".to_owned();
        replacement.push_str(&" ".repeat(self.current_indent()));
        indented.replace("\n", &replacement)
    }

    fn current_indent(&self) -> usize {
        match self.contexts.last() {
            Some(c) => c.indent,
            None => panic!("No contexts!"),
        }
    }

    fn alias_name<S: AsRef<str>>(s: S) -> String {
        format!(" AS {}", Self::maybe_quote(s))
    }

    fn maybe_quote<S: AsRef<str>>(s: S) -> String {
        let r = s.as_ref();
        for c in r.chars() {
            if c.is_uppercase() {
                return format!(r#""{}""#, r);
            }
        }
        String::from(r)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use anyhow::Result;
    use spectral::prelude::*;
    #[test]
    fn test_maybe_quote() {
        assert_that(&Formatter::maybe_quote("foo")).is_equal_to("foo".to_string());
        assert_that(&Formatter::maybe_quote("Foo")).is_equal_to(String::from(r#""Foo""#));
        assert_that(&Formatter::maybe_quote("fooBar")).is_equal_to(String::from(r#""fooBar""#));
    }

    #[test]
    fn test_join_type() -> Result<()> {
        let f = Formatter::new();
        assert_that(&f.join_type(&JoinType::JoinInner)?).is_equal_to("JOIN");
        assert_that(&f.join_type(&JoinType::JoinLeft)?).is_equal_to("LEFT OUTER JOIN");
        assert_that(&f.join_type(&JoinType::JoinRight)?).is_equal_to("RIGHT OUTER JOIN");
        assert_that(&f.join_type(&JoinType::JoinFull)?).is_equal_to("FULL OUTER JOIN");

        let res = f.join_type(&JoinType::JoinSemi);
        assert_that(&res).is_err();
        assert_that(&res.unwrap_err().to_string()).is_equal_to(String::from(
            "join type is JoinSemi but there is no Pg keyword for this",
        ));
        Ok(())
    }

    struct TestCase {
        name: String,
        node: Node,
        expect: String,
        indent: usize,
    }
    type FormatFn = fn(&mut Formatter, Node) -> R;

    macro_rules! case {
        ( $name:literal, $node:expr, $expect:literal $(,)? ) => {
            case!($name, $node, $expect, 0);
        };
        ( $name:literal, $node:expr, $expect:literal, $indent:literal $(,)? ) => {
            TestCase {
                name: $name.to_string(),
                node: $node,
                expect: $expect.to_string(),
                indent: $indent,
            }
        };
    }

    macro_rules! range_var {
        ( $relname:literal $(,)? ) => {
            make_range_var(None, None, $relname, None)
        };
        ( $relname:literal AS $alias:literal ) => {
            make_range_var(None, None, $relname, Some($alias))
        };
        ( $catalogname:literal, $relname:literal $(,)? ) => {
            make_range_var(None, Some($catalogname), $relname, None)
        };
        ( $catalogname:literal, $relname:literal AS $alias:literal ) => {
            make_range_var(None, Some($catalogname), $relname, Some($alias))
        };
        ( $schemaname:literal, $catalogname:literal, $relname:literal $(,)? ) => {
            make_range_var(Some($schemaname), Some($catalogname), $relname, None)
        };
        ( $schemaname:literal, $catalogname:literal, $relname:literal AS $alias:literal ) => {
            make_range_var(
                Some($schemaname),
                Some($catalogname),
                $relname,
                Some($alias),
            )
        };
    }

    macro_rules! join_expr {
        ( $larg:expr, $($join_type:ident)*, $rarg:expr, ON, $lon:literal = $ron:literal $(,)? ) => {
            {
                let (jt, n) = _join_type!($($join_type)*);
                make_join_expr(
                    jt,
                    n,
                    $larg,
                    $rarg,
                    None,
                    Some(($lon.split(".").collect(), "=", $ron.split(".").collect())),
                    None,
                )
            }
        };
        ( $larg:expr, $($join_type:ident)*, $rarg:expr, USING, $($using:literal),+ $(,)? ) => {
            {
                let (jt, n) = _join_type!($($join_type)*);
                make_join_expr(
                    jt,
                    n,
                    $larg,
                    $rarg,
                    Some(vec![$($using),+]),
                    None,
                    None,
                )
            }
        };
    }

    macro_rules! _join_type {
        (JOIN) => {
            (JoinType::JoinInner, false)
        };
        (NATURAL JOIN) => {
            (JoinType::JoinInner, true)
        };
        (LEFT OUTER JOIN) => {
            (JoinType::JoinLeft, false)
        };
        (RIGHT OUTER JOIN) => {
            (JoinType::JoinRight, false)
        };
    }

    fn run_tests(tests: Vec<TestCase>, format_fn: FormatFn) -> Result<()> {
        for t in tests {
            let mut f = Formatter::new();
            f.push_context_with_indent(t.indent);
            let formatted = format_fn(&mut f, t.node)?;
            assert_that(&formatted).named(&t.name).is_equal_to(t.expect);
        }
        Ok(())
    }

    #[test]
    fn test_range_var() -> Result<()> {
        let tests: Vec<TestCase> = vec![
            case!("table", range_var!("people"), "people"),
            case!(
                "table with alias",
                range_var!("people" AS "persons"),
                "people AS persons",
            ),
            case!(
                "schema & table",
                range_var!("some_schema", "people"),
                "some_schema.people",
            ),
            case!(
                "catalog, schema, & table",
                range_var!("some_catalog", "some_schema", "people"),
                "some_catalog.some_schema.people",
            ),
            case!("uc table", range_var!("People"), r#""People""#),
            case!(
                "uc schema & table",
                range_var!("Schema", "People"),
                r#""Schema"."People""#,
            ),
            case!(
                "uc catalog, schema, & table",
                range_var!("Cat", "Schema", "People"),
                r#""Cat"."Schema"."People""#,
            ),
            case!(
                "catalog, schema, & table with alias",
                range_var!("cata", "mySchema", "People" AS "Persons"),
                r#"cata."mySchema"."People" AS "Persons""#,
            ),
        ];
        run_tests(tests, |f, n| {
            let rv = match n {
                Node::RangeVar(rv) => rv,
                _ => panic!("Got a something that isn't a range var from make_range_var()!"),
            };
            Ok(f.format_range_var(&rv))
        })
    }

    #[test]
    fn test_from_element() -> Result<()> {
        let tests: Vec<TestCase> = vec![
            case!(
                "inner join ON",
                join_expr!(
                    range_var!("table1"),
                    JOIN,
                    range_var!("table2"),
                    ON,
                    "table1.foo" = "table2.foo",
                ),
                "table1\nJOIN table2 ON table1.foo = table2.foo",
            ),
            case!(
                "inner join ON with indent",
                join_expr!(
                    range_var!("table1"),
                    JOIN,
                    range_var!("table2"),
                    ON,
                    "table1.foo" = "table2.foo",
                ),
                "    table1\n    JOIN table2 ON table1.foo = table2.foo",
                4,
            ),
            case!(
                "left outer join ON",
                join_expr!(
                    range_var!("table1"),
                    LEFT OUTER JOIN,
                    range_var!("table2"),
                    ON,
                    "table1.foo" = "table2.foo",
                ),
                "table1\nLEFT OUTER JOIN table2 ON table1.foo = table2.foo",
            ),
            case!(
                "right outer join ON",
                join_expr!(
                    range_var!("table1"),
                    RIGHT OUTER JOIN,
                    range_var!("table2"),
                    ON,
                    "table1.foo" = "table2.foo",
                ),
                "table1\nRIGHT OUTER JOIN table2 ON table1.foo = table2.foo",
            ),
            case!(
                "inner join USING",
                join_expr!(
                    range_var!("table1"),
                    JOIN,
                    range_var!("table2"),
                    USING,
                    "foo",
                ),
                "table1\nJOIN table2 USING (foo)",
            ),
            case!(
                "inner join USING with indent",
                join_expr!(
                    range_var!("table1"),
                    JOIN,
                    range_var!("table2"),
                    USING,
                    "foo"
                ),
                "    table1\n    JOIN table2 USING (foo)",
                4,
            ),
        ];
        run_tests(tests, |f, n| f.format_from_element(&n, false))?;

        let tests: Vec<TestCase> = vec![
            case!(
                "inner join ON",
                join_expr!(
                    range_var!("table1"),
                    JOIN,
                    range_var!("table2"),
                    ON,
                    "table1.foo" = "table2.foo",
                ),
                "table1\nJOIN table2\n    ON table1.foo = table2.foo",
            ),
            case!(
                "inner join ON with indent",
                join_expr!(
                    range_var!("table1"),
                    JOIN,
                    range_var!("table2"),
                    ON,
                    "table1.foo" = "table2.foo",
                ),
                "    table1\n    JOIN table2\n        ON table1.foo = table2.foo",
                4,
            ),
            case!(
                "left outer join ON",
                join_expr!(
                    range_var!("table1"),
                    LEFT OUTER JOIN,
                    range_var!("table2"),
                    ON,
                    "table1.foo" = "table2.foo",
                ),
                "table1\nLEFT OUTER JOIN table2\n    ON table1.foo = table2.foo",
            ),
            case!(
                "right outer join ON",
                join_expr!(
                    range_var!("table1"),
                    RIGHT OUTER JOIN,
                    range_var!("table2"),
                    ON,
                    "table1.foo" = "table2.foo",
                ),
                "table1\nRIGHT OUTER JOIN table2\n    ON table1.foo = table2.foo",
            ),
            case!(
                "inner join USING",
                join_expr!(
                    range_var!("table1"),
                    JOIN,
                    range_var!("table2"),
                    USING,
                    "foo",
                ),
                "table1\nJOIN table2\n    USING (foo)",
            ),
            case!(
                "inner join USING with indent",
                join_expr!(
                    range_var!("table1"),
                    JOIN,
                    range_var!("table2"),
                    USING,
                    "foo",
                ),
                "    table1\n    JOIN table2\n        USING (foo)",
                4,
            ),
        ];
        run_tests(tests, |f, n| {
            f.max_line_length = 20;
            f.format_from_element(&n, false)
        })
    }

    fn make_range_var(c: Option<&str>, s: Option<&str>, r: &str, a: Option<&str>) -> Node {
        let alias = match a {
            Some(a) => Some(AliasWrapper::Alias(Alias {
                aliasname: a.to_string(),
                colnames: None,
            })),
            None => None,
        };
        let catalogname = match c {
            Some(c) => Some(c.to_string()),
            None => None,
        };
        let schemaname = match s {
            Some(s) => Some(s.to_string()),
            None => None,
        };

        Node::RangeVar(RangeVar {
            catalogname,
            schemaname,
            relname: r.to_string(),
            inh: false,
            relpersistence: None,
            alias,
            location: None,
        })
    }

    fn make_join_expr(
        jointype: JoinType,
        is_natural: bool,
        l: Node,
        r: Node,
        u: Option<Vec<&str>>,
        q: Option<(Vec<&str>, &str, Vec<&str>)>,
        a: Option<String>,
    ) -> Node {
        let using_clause = match u {
            Some(u) => Some(
                u.iter()
                    .map(|n| StringStructWrapper::StringStruct(StringStruct { str: n.to_string() }))
                    .collect(),
            ),
            None => None,
        };

        let quals = match q {
            Some(q) => {
                let opl = q.0;
                let op = q.1;
                let opr = q.2;
                Some(Box::new(Node::AExpr(AExpr {
                    kind: AExprKind::AExprOp,
                    name: Some(vec![Node::StringStruct(StringStruct {
                        str: op.to_string(),
                    })]),
                    lexpr: Box::new(make_column_ref(opl)),
                    rexpr: OneOrManyNodes::One(Box::new(make_column_ref(opr))),
                    location: None,
                })))
            }
            None => None,
        };

        let alias = match a {
            Some(a) => Some(AliasWrapper::Alias(Alias {
                aliasname: a,
                colnames: None,
            })),
            None => None,
        };

        Node::JoinExpr(JoinExpr {
            jointype,
            is_natural,
            larg: Box::new(l),
            rarg: Box::new(r),
            using_clause,
            quals,
            alias,
            rtindex: None,
        })
    }

    fn make_column_ref(names: Vec<&str>) -> Node {
        let fields = names
            .iter()
            .map(|n| ColumnRefField::StringStruct(StringStruct { str: n.to_string() }))
            .collect();
        Node::ColumnRef(ColumnRef {
            fields,
            location: None,
        })
    }
}
