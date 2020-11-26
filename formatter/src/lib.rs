use pg_pretty_parser::{ast::*, flags::FrameOptions};
use scopeguard::guard;
use std::collections::HashMap;
use thiserror::Error;
//use trace::trace;

#[derive(Debug, Error)]
pub enum Error {
    #[error("unexpected node {:?} in {}", .node, .func)]
    UnexpectedNode { node: String, func: String },
    #[error("no target list for select")]
    NoTargetListForSelect,
    #[error("missing {} arg for {} op", .side, .op)]
    MissingSideForOp { side: String, op: String },
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
    #[error("range function node does not have any functions")]
    RangeFunctionDoesNotHaveAnyFunctions,
    #[error("range function's list of func calls contains a non-FuncCall node")]
    RangeFunctionHasNonFuncCallFunction,
    #[error("frame options specified {} but did not contain a value", opt)]
    FrameOptionsValueWithoutOffset { opt: String },
}

#[derive(Debug, PartialEq)]
enum ContextType {
    AExpr,
    GroupingSet,
    OrderByUsingClause,
    RangeTableSample,
    SubLink,
}

#[derive(Debug)]
pub struct Formatter {
    a_expr_depth: u8,
    bool_expr_depth: u8,
    max_line_len: usize,
    indent_width: usize,
    indents: Vec<usize>,
    contexts: Vec<ContextType>,
    type_renaming: HashMap<String, String>,
}

type R = Result<String, Error>;

//trace::init_depth_var!();

impl Formatter {
    #[allow(clippy::new_without_default)]
    pub fn new() -> Self {
        let mut type_renaming: HashMap<String, String> = HashMap::new();
        type_renaming.insert("int4".to_string(), "int".to_string());

        Self {
            a_expr_depth: 0,
            bool_expr_depth: 0,
            max_line_len: 100,
            indent_width: 4,
            indents: vec![0],
            contexts: vec![],
            type_renaming,
        }
    }

    fn new_subformatter(&self) -> Self {
        let mut sub = Self::new();
        sub.indents = vec![self.current_indent() + self.indent_width];
        sub
    }

    pub fn format_root_stmt(&mut self, root: &Root) -> R {
        match root {
            Root::RawStmt(raw) => Ok(self.format_node(&raw.stmt)?),
        }
    }

    //#[trace]
    fn format_node(&mut self, node: &Node) -> R {
        match &node {
            Node::AConst(a) => self.format_a_const(&a),
            Node::AExpr(a) => self.format_a_expr(&a),
            Node::BoolExpr(b) => self.format_bool_expr(&b),
            Node::ColumnRef(c) => self.format_column_ref(&c),
            Node::FuncCall(f) => self.format_func_call(&f),
            Node::GroupingSet(g) => self.format_grouping_set(&g),
            Node::RangeVar(r) => Ok(self.format_range_var(&r)),
            Node::RowExpr(r) => self.format_row_expr(&r),
            Node::SelectStmt(s) => self.format_select_stmt(&s),
            Node::StringStruct(s) => Ok(self.format_string(&s.str)),
            Node::SubLink(s) => self.format_sub_link(&s),
            Node::TypeCast(t) => self.format_type_cast(&t),
            Node::WindowDef(w) => self.format_window_def(&w, 0),
            _ => Err(Error::UnexpectedNode {
                node: node.to_string(),
                func: "format_node".to_string(),
            }),
        }
    }

    fn format_string(&self, s: &str) -> String {
        // Depending on the context the string may be a string literal or it
        // may something like an operator. Tracking the context seems to be
        // the only way to figure that out. I have no idea if this actually
        // works in practice though ...
        match self.contexts.last() {
            Some(c) => match c {
                ContextType::AExpr
                | ContextType::OrderByUsingClause
                | ContextType::RangeTableSample
                | ContextType::SubLink => s.to_string(),
                _ => self.quote_string(&s),
            },
            None => self.quote_string(&s),
        }
    }

    //#[trace]
    fn format_select_stmt(&mut self, s: &SelectStmt) -> R {
        if let Some(mut op) = self.match_op(&s.op) {
            // XXX - this is so gross. I think the unstable box matching
            // syntax would make this much less gross.
            let left = match &s.larg {
                Some(l) => {
                    let SelectStmtWrapper::SelectStmt(l) = &**l;
                    self.format_select_stmt(l)?
                }
                None => {
                    return Err(Error::MissingSideForOp {
                        side: "left".to_string(),
                        op,
                    })
                }
            };
            let right = match &s.rarg {
                Some(r) => {
                    let SelectStmtWrapper::SelectStmt(r) = &**r;
                    self.format_select_stmt(r)?
                }
                None => {
                    return Err(Error::MissingSideForOp {
                        side: "left".to_string(),
                        op,
                    })
                }
            };

            if s.all {
                op.push_str(" ALL");
            }

            return Ok(format!("{}{}\n{}", left, op, right));
        }

        let t = match &s.target_list {
            Some(tl) => tl,
            None => return Err(Error::NoTargetListForSelect),
        };
        let mut select = self.format_select_clause(t, s.distinct_clause.as_ref())?;
        if let Some(f) = &s.from_clause {
            select.push_str(&self.format_from_clause(f)?);
        }
        if let Some(w) = &s.where_clause {
            select.push_str(&self.format_where_clause(w)?);
        }
        if let Some(g) = &s.group_clause {
            select.push_str(&self.format_group_by_clause(g)?);
        }
        if let Some(h) = &s.having_clause {
            select.push_str(&self.format_having_clause(h)?);
        }
        if let Some(w) = &s.window_clause {
            select.push_str(&self.format_window_clause(w)?);
        }
        if let Some(o) = &s.sort_clause {
            select.push_str(&self.format_order_by_clause(o)?);
            select.push('\n');
        }
        select.push_str(&self.maybe_format_limit(s)?);
        if let Some(l) = &s.locking_clause {
            select.push_str(&self.format_locking_clause(l)?);
        }

        Ok(select)
    }

    fn match_op(&self, op: &SetOperation) -> Option<String> {
        match op {
            SetOperation::SetopUnion => Some("UNION".to_string()),
            SetOperation::SetopIntersect => Some("INTERSECT".to_string()),
            SetOperation::SetopExcept => Some("EXCEPT".to_string()),
            SetOperation::SetopNone => None,
        }
    }

    //#[trace]
    fn format_select_clause(&mut self, tl: &[Node], d: Option<&Vec<Option<Node>>>) -> R {
        let mut prefix = "SELECT ".to_string();

        if let Some(distinct) = d {
            if distinct.len() == 1 && distinct[0].is_none() {
                prefix.push_str("DISTINCT ");
            } else {
                prefix.push_str("DISTINCT ON ");
                // All of the elements should be Some(...).
                let maker = |f: &mut Self| {
                    distinct
                        .iter()
                        .map(|c| match c {
                            Some(c) => f.format_node(c),
                            None => panic!("multi-element distinct clause contains a null!"),
                        })
                        .collect::<Result<Vec<_>, _>>()
                };
                prefix = self.one_line_or_many(&prefix, false, true, true, 0, maker)?;
                if prefix.contains('\n') {
                    prefix.push('\n');
                } else {
                    prefix.push(' ');
                }
            }
        }

        let mut can_be_one_line = !prefix.contains('\n');
        if can_be_one_line {
            for t in tl {
                if self.is_complex_target_element(t)? {
                    can_be_one_line = false;
                    break;
                }
            }
        }

        let maker = |f: &mut Self| {
            tl.iter()
                .map(|t| f.format_target_element(t))
                .collect::<Result<Vec<_>, _>>()
        };
        if can_be_one_line {
            return Ok(format!(
                "{}\n",
                self.one_line_or_many(&prefix, true, false, true, 0, maker)?
            ));
        }

        Ok(format!(
            "{}\n",
            self.many_lines(&prefix, true, false, true, maker)?
        ))
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

    //#[trace]
    fn format_target_element(&mut self, t: &Node) -> R {
        match &t {
            Node::ResTarget(rt) => self.format_res_target(rt),
            _ => Err(Error::UnexpectedNode {
                node: t.to_string(),
                func: "format_target_element".to_string(),
            }),
        }
    }

    //#[trace]
    fn format_res_target(&mut self, rt: &ResTarget) -> R {
        let mut f = String::new();
        f.push_str(&self.format_node(&rt.val)?);
        if let Some(n) = &rt.name {
            f.push_str(&Self::alias_name(&n));
        }

        Ok(f)
    }

    //#[trace]
    fn format_where_clause(&mut self, w: &Node) -> R {
        let mut wh = self.indent_str("WHERE ");

        self.push_indent_from_str(&wh);
        wh.push_str(&self.format_node(w)?);
        self.pop_indent();

        wh.push('\n');

        Ok(wh)
    }

    //#[trace]
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

    //#[trace]
    fn format_a_const(&mut self, a: &AConst) -> R {
        match &a.val {
            Value::StringStruct(s) => Ok(self.quote_string(&s.str)),
            Value::BitString(s) => Ok(self.quote_string(&s.str)),
            Value::Integer(i) => Ok(i.ival.to_string()),
            Value::Float(f) => Ok(f.str.clone()),
            Value::Null(_) => Ok("NULL".to_string()),
        }
    }

    //#[trace]
    fn format_a_expr(&mut self, a: &AExpr) -> R {
        self.a_expr_depth += 1;
        self.contexts.push(ContextType::AExpr);
        let mut formatter = guard(self, |s| {
            s.a_expr_depth -= 1;
            s.contexts.pop();
        });

        let op = match &a.kind {
            AExprKind::AExprParen => panic!("no idea how to handle AExprParen kind of AExpr"),
            _ => {
                let o = a.name.as_ref().unwrap_or_else(|| {
                    panic!(format!(
                        "must have a name defined for a {} kind of AExpr",
                        a.kind
                    ))
                });
                formatter.formatted_list(o)?.join(" ")
            }
        };

        let real_op = match a.kind {
            AExprKind::AExprOp => op,
            AExprKind::AExprOpAny => format!("{} ANY", op),
            AExprKind::AExprOpAll => format!("{} ALL", op),
            AExprKind::AExprDistinct => "IS DISTINCT FROM".to_string(),
            AExprKind::AExprNotDistinct => "IS NOT DISTINCT FROM".to_string(),
            AExprKind::AExprNullif => "NULLIF".to_string(),
            AExprKind::AExprOf => format!("IS {}OF", formatter.maybe_not(&op)),
            AExprKind::AExprIn => format!("{}IN", formatter.maybe_not(&op)),
            // For all the rest we can use the op as is.
            _ => op,
        };

        formatter.format_infix_expr(&a.lexpr, real_op, &a.rexpr)
    }

    fn maybe_not(&self, op: &str) -> String {
        match op {
            "=" => String::new(),
            "<>" => "NOT ".to_string(),
            _ => panic!(format!(
                "got an AExprOf AExpr with an invalid op name: {}",
                op
            )),
        }
    }

    //#[trace]
    fn format_infix_expr(&mut self, left: &Node, op: String, right: &OneOrManyNodes) -> R {
        let mut e: Vec<String> = vec![];
        e.push(self.format_node(&left)?);
        e.push(op);
        match right {
            OneOrManyNodes::One(r) => e.push(self.format_node(&r)?),
            // XXX - Is this right? The right side could be the right side of
            // something like "foo IN (1, 2)", but could it also be something
            // else that shouldn't be joined by commas?
            OneOrManyNodes::Many(r) => {
                e.push(self.one_line_or_many("", false, true, true, 0, |f| f.formatted_list(&r))?)
            }
        }
        if self.a_expr_depth > 1 {
            e.insert(0, "(".to_string());
            e.push(")".to_string());
        }

        Ok(e.join(" "))
    }

    // If a bool expr is just a series of "AND" clauses, then we get one
    // BoolExpr with many args. If there is a mix of "AND" and "OR", then the
    // args will contain nested BoolExpr clauses. So if our depth is > 1 then
    // we need to add wrapping parens.
    //
    // If it's a NOT clause it should only have one child and we don't add
    // parens around it.
    //#[trace]
    fn format_bool_expr(&mut self, b: &BoolExpr) -> R {
        let op = match b.boolop {
            BoolExprType::AndExpr => "AND",
            BoolExprType::OrExpr => "OR",
            BoolExprType::NotExpr => "NOT",
        };

        self.bool_expr_depth += 1;

        if op == "NOT" {
            let res = self.format_not_expr(b);
            self.bool_expr_depth -= 1;
            return res;
        }

        let mut args = self.formatted_list(&b.args)?;

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
                self.push_indent_one_level();
            }
        };

        // The first level of bool expr will be immediately after a "WHERE" or
        // an "ON", and so does not need additional indentation.
        if self.bool_expr_depth == 1 {
            expr.push_str(&args.remove(0));
        } else {
            expr.push_str(&self.indent_str(&args.remove(0)));
        }
        expr.push('\n');

        let last = args.len();
        for (n, f) in args.iter().enumerate() {
            expr.push_str(&self.indent_str(&op));
            expr.push(' ');
            expr.push_str(&f);
            if n < last - 1 {
                expr.push('\n');
            }
        }

        // If we're closing out a nested boolean, we want to ...
        if self.bool_expr_depth > 1 {
            // add a newline
            expr.push('\n');
            // outdent one level
            self.pop_indent();
            // add our closing delimiter
            expr.push_str(&self.indent_str(")"));
            self.bool_expr_depth -= 1;
            // outdent back to 1 to match what we did earlier.
            for _ in 1..self.bool_expr_depth {
                self.pop_indent();
            }
        } else {
            self.bool_expr_depth -= 1;
        }

        Ok(expr)
    }

    //#[trace]
    fn format_not_expr(&mut self, b: &BoolExpr) -> R {
        let maker = |f: &mut Self| Ok(vec![f.format_node(&b.args[0])?]);
        self.one_line_or_many("NOT ", false, true, false, 0, maker)
    }

    //#[trace]
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
                Node::StringStruct(s) => Ok(s.str.clone()),
                _ => self.format_node(&n),
            })
            .collect::<Result<Vec<_>, _>>()?
            .join(" ");
        func.push('(');

        let mut arg_is_simple = true;

        let mut args = String::new();
        if f.agg_distinct {
            arg_is_simple = false;
            args.push_str("DISTINCT ");
        }

        // We'll ignore f.func_variadic since it's optional.
        if f.agg_star {
            args.push('*');
        }

        match &f.args {
            Some(a) => {
                let f = self.joined_list(&a, ", ")?;
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

        // XXX need to handle f.agg_within_group

        if !arg_is_simple {
            func.push(' ');
            func.push_str(&args);
            func.push(' ');
        } else {
            func.push_str(&args);
        }

        func.push(')');

        if let Some(WindowDefWrapper::WindowDef(w)) = &f.over {
            func.push_str(" OVER ");
            let current_indent = if func.contains('\n') { 0 } else { func.len() };
            func.push_str(&self.format_window_def(&w, current_indent)?);
        }

        Ok(func)
    }

    //#[trace]
    fn format_window_def(&mut self, w: &WindowDef, current_indent: usize) -> R {
        let mut window = String::new();
        // XXX - should we look at the context to determine whether we should
        // allow a name or refname? It seems like name isn't allowed in SELECT
        // clause (?) and refname is not allowed in WINDOW clause (?).
        if let Some(n) = &w.name {
            window.push_str(n);
        }

        let maker = |f: &mut Self| {
            let mut items: Vec<String> = vec![];
            if let Some(n) = &w.refname {
                items.push(n.clone());
            }
            if let Some(p) = &w.partition_clause {
                items.push(f.format_partition_clause(&window, &p, current_indent)?);
            }
            if let Some(ob) = &w.order_clause {
                items.push(f.format_order_by_clause(&ob)?);
            }
            if w.frame_options != FrameOptions::DEFAULTS {
                items.push(f.format_frame_options(w)?);
            }

            Ok(items)
        };

        let full_indent = if window.is_empty() {
            current_indent
        } else {
            // 4 for " AS "
            current_indent + window.len() + 4
        };

        let rest = self.one_line_or_many("", false, true, false, full_indent, maker)?;
        if !rest.is_empty() {
            if !window.is_empty() {
                window.push_str(" AS ");
            }
            window.push_str(&rest);
        }

        Ok(window)
    }

    //#[trace]
    fn format_partition_clause(&mut self, w: &str, p: &[Node], current_indent: usize) -> R {
        self.one_line_or_many(
            "PARTITION BY ",
            false,
            false,
            true,
            current_indent + w.len(),
            |f| f.formatted_list(p),
        )
    }

    //#[trace]
    fn format_frame_options(&mut self, w: &WindowDef) -> R {
        let flags = &w.frame_options;
        let mut options: Vec<&str> = vec![];
        // XXX - I'm not sure if it's possible to have options where neither
        // of these are true.
        if flags.contains(FrameOptions::RANGE) {
            options.push("RANGE");
        } else if flags.contains(FrameOptions::ROWS) {
            options.push("ROWS");
        }
        if flags.contains(FrameOptions::BETWEEN) {
            options.push("BETWEEN");
        }

        if flags.contains(FrameOptions::START_UNBOUNDED_PRECEDING) {
            options.push("UNBOUNDED PRECEDING");
        }
        if flags.contains(FrameOptions::START_UNBOUNDED_FOLLOWING) {
            options.push("UNBOUNDED FOLLOWING");
        }
        if flags.contains(FrameOptions::START_CURRENT_ROW) {
            options.push("CURRENT ROW");
        }
        let mut range: Vec<String> = vec![];
        if flags.contains(FrameOptions::START_VALUE_PRECEDING)
            | flags.contains(FrameOptions::START_VALUE_FOLLOWING)
        {
            let direction = if flags.contains(FrameOptions::START_VALUE_PRECEDING) {
                "PRECEDING"
            } else {
                "FOLLOWING"
            };
            if let Some(o) = &w.start_offset {
                range.push(self.format_node(&*o)?);
            } else {
                return Err(Error::FrameOptionsValueWithoutOffset {
                    opt: format!("START {}", direction),
                });
            }
            range.push(direction.to_string());
        }

        if flags.intersects(FrameOptions::START) && flags.intersects(FrameOptions::END) {
            range.push("AND".to_string());
        }

        if flags.contains(FrameOptions::END_UNBOUNDED_PRECEDING) {
            range.push("UNBOUNDED PRECEDING".to_string());
        }
        if flags.contains(FrameOptions::END_UNBOUNDED_FOLLOWING) {
            range.push("UNBOUNDED FOLLOWING".to_string());
        }
        if flags.contains(FrameOptions::END_CURRENT_ROW) {
            range.push("CURRENT ROW".to_string());
        }
        if flags.contains(FrameOptions::END_VALUE_PRECEDING)
            | flags.contains(FrameOptions::END_VALUE_FOLLOWING)
        {
            let direction = if flags.contains(FrameOptions::END_VALUE_PRECEDING) {
                "PRECEDING"
            } else {
                "FOLLOWING"
            };
            if let Some(o) = &w.end_offset {
                range.push(self.format_node(&*o)?);
            } else {
                return Err(Error::FrameOptionsValueWithoutOffset {
                    opt: format!("END {}", direction),
                });
            }
            range.push(direction.to_string());
        }

        let joined = &range.join(" ");
        options.push(joined);

        Ok(options.join(" "))
    }

    //#[trace]
    fn maybe_format_limit(&mut self, s: &SelectStmt) -> R {
        let mut limit = String::new();
        if let Some(c) = &s.limit_count {
            limit.push_str("LIMIT ");
            match **c {
                Node::AConst(AConst {
                    val: Value::Null(_),
                    ..
                }) => limit.push_str("ALL"),
                _ => limit.push_str(&self.format_node(&*c)?),
            }
        }
        if let Some(o) = &s.limit_offset {
            if !limit.is_empty() {
                limit.push(' ');
            }
            limit.push_str("OFFSET ");
            limit.push_str(&self.format_node(&*o)?);
        }
        if !limit.is_empty() {
            limit.push('\n');
        }

        Ok(limit)
    }

    //#[trace]
    fn format_sub_link(&mut self, s: &SubLink) -> R {
        self.contexts.push(ContextType::SubLink);
        let mut formatter = guard(self, |s| {
            s.contexts.pop();
        });

        let mut link = match &s.testexpr {
            Some(n) => formatter.format_node(&*n)?,
            None => String::new(),
        };

        link.push_str(&formatter.format_sub_link_oper(s)?);
        link.push_str("(\n");

        let mut subformatter = formatter.new_subformatter();
        link.push_str(&subformatter.format_node(&*s.subselect)?);

        link.push_str(&formatter.indent_str(")"));

        Ok(link)
    }

    //#[trace]
    fn format_sub_link_oper(&mut self, s: &SubLink) -> R {
        match s.sub_link_type {
            SubLinkType::ExistsSublink => Ok("EXISTS".to_string()),
            SubLinkType::AllSublink => {
                let o = s
                    .oper_name
                    .as_ref()
                    .expect("should never have an AllSublink without an operator");
                // The operator _should_ be a Vec of StringStructs, but
                // who knows what wackiness might exist.
                let mut j = " ".to_string();
                j.push_str(&self.joined_list(&o, " ")?);
                j.push_str(" ALL");
                Ok(j)
            }
            SubLinkType::AnySublink => match &s.oper_name {
                None => Ok(" IN ".to_string()),
                Some(o) => {
                    // The operator _should_ be a Vec of StringStructs, but
                    // who knows what wackiness might exist.
                    let mut j = " ".to_string();
                    j.push_str(&self.joined_list(&o, " ")?);
                    j.push_str(" ANY");
                    Ok(j)
                }
            },
            SubLinkType::RowcompareSublink => {
                let o = s
                    .oper_name
                    .as_ref()
                    .expect("should never have an RowcompareSublink without an operator");

                // The operator _should_ be a Vec of StringStructs, but
                // who knows what wackiness might exist.
                let mut j = " ".to_string();
                j.push_str(&self.joined_list(&o, " ")?);
                Ok(j)
            }
            // I'm not sure exactly what sort of SQL produces these two
            // options.
            SubLinkType::ExprSublink => Ok(String::new()),
            SubLinkType::MultiexprSublink => Ok(String::new()),
            SubLinkType::ArraySublink => Ok("ARRAY".to_string()),
            SubLinkType::CteSublink => {
                panic!("I don't think this can ever happen in a SubLink as opposed to a SubPlan")
            }
        }
    }

    //#[trace]
    fn format_row_expr(&mut self, r: &RowExpr) -> R {
        let prefix = match r.row_format {
            CoercionForm::CoerceExplicitCall => "ROW",
            CoercionForm::CoerceImplicitCast => "",
            CoercionForm::CoerceExplicitCast => {
                panic!("coercion_form should never be CoerceExplicitCast for a RowExpr")
            }
        };

        self.one_line_or_many(prefix, false, true, true, 0, |f| f.formatted_list(&r.args))
    }

    //#[trace]
    fn format_grouping_set(&mut self, gs: &GroupingSet) -> R {
        let is_nested = self.is_in_context(ContextType::GroupingSet);

        self.contexts.push(ContextType::GroupingSet);
        let mut formatter = guard(self, |s| {
            s.contexts.pop();
        });

        if let GroupingSetKind::GroupingSetEmpty = gs.kind {
            return Ok(String::new());
        }

        let members = gs
            .content
            .as_ref()
            .expect("we should always have content unless the kind if GroupingSetEmpty!");

        let (grouping_set, needs_parens) = match gs.kind {
            GroupingSetKind::GroupingSetEmpty => {
                panic!("we already matched GroupingSetEmpty, wtf!")
            }
            GroupingSetKind::GroupingSetSimple => (String::new(), false),
            GroupingSetKind::GroupingSetRollup => ("ROLLUP ".to_string(), false),
            GroupingSetKind::GroupingSetCube => ("CUBE ".to_string(), false),
            GroupingSetKind::GroupingSetSets => {
                if is_nested {
                    (String::new(), true)
                } else {
                    // If we have an unnested set with one member there's no
                    // need for additional parens.
                    ("GROUPING SETS ".to_string(), members.len() > 1)
                }
            }
        };

        let maker = |f: &mut Self| -> Result<Vec<String>, Error> {
            Ok(f.formatted_list(&members)?
                .into_iter()
                .map(|g| {
                    // If the element is a RowExpr it will already have
                    // wrapping parens.
                    if needs_parens && !(g.starts_with('(') && g.ends_with(')')) {
                        format!("({})", g)
                    } else {
                        g
                    }
                })
                .collect::<Vec<String>>())
        };

        formatter.one_line_or_many(&grouping_set, false, true, true, 0, maker)
    }

    //#[trace]
    fn format_from_clause(&mut self, fc: &[Node]) -> R {
        let start = "FROM ";
        let mut from = self.indent_str("FROM ");

        // We want to indent by the width of "FROM ", not any additional
        // indent before that string.
        self.push_indent_from_str(start);
        for (n, f) in fc.iter().enumerate() {
            if n > 0 {
                from.push_str(",\n");
                from.push_str(&" ".repeat(self.current_indent()));
            }
            from.push_str(&self.format_from_element(&f, n == 0)?);
        }
        self.pop_indent();

        from.push('\n');

        Ok(from)
    }

    //#[trace]
    fn format_from_element(&mut self, f: &Node, is_first: bool) -> R {
        match f {
            Node::JoinExpr(j) => self.format_join_expr(&j, is_first),
            Node::RangeVar(r) => Ok(self.format_range_var(&r)),
            Node::RangeSubselect(s) => self.format_subselect(&s),
            Node::RangeFunction(f) => self.format_range_function(&f),
            Node::RangeTableSample(s) => self.format_range_table_sample(&s),
            _ => Ok("from_element".to_string()),
        }
    }

    //#[trace]
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
        e.push('\n');
        e.push_str(&" ".repeat(self.current_indent()));
        if j.is_natural {
            e.push_str("NATURAL ");
        }
        e.push_str(self.join_type(&j.jointype)?);
        e.push(' ');
        e.push_str(&self.format_from_element(&j.rarg, is_first)?);

        if let Some(q) = &j.quals {
            // For now, we'll just always put a newline before the "ON ..."
            // clause. This simplifies the formatting and makes it consistent
            // with how we format WHERE clauses.
            e.push('\n');
            self.push_indent_one_level();
            e.push_str(&self.indent_str("ON "));
            e.push_str(&self.format_node(q)?);
            self.pop_indent();
        }
        if let Some(u) = &j.using_clause {
            let using = format!("USING {}", self.format_using_clause(u));
            // + 1 for space before "USING"
            if self.len_after_nl(&e) + using.len() + 1 > self.max_line_len {
                e.push('\n');
                self.push_indent_one_level();
                e.push_str(&self.indent_str(&using));
                self.pop_indent();
            } else {
                e.push(' ');
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

    //#[trace]
    fn format_using_clause(&self, u: &[StringStructWrapper]) -> String {
        let mut using = "(".to_string();
        if u.len() > 1 {
            using.push(' ');
        }
        using.push_str(
            u.iter()
                .map(|StringStructWrapper::StringStruct(u)| u.str.clone())
                .collect::<Vec<String>>()
                .join(", ")
                .as_str(),
        );
        if u.len() > 1 {
            using.push(' ');
        }
        using.push(')');
        using
    }

    //#[trace]
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

    //#[trace]
    fn format_group_by_clause(&mut self, g: &[Node]) -> R {
        Ok(format!(
            "{}\n",
            self.one_line_or_many("GROUP BY ", false, false, true, 0, |f| f.formatted_list(g))?
        ))
    }

    //#[trace]
    fn format_having_clause(&mut self, h: &Node) -> R {
        let mut having = self.indent_str("HAVING ");

        self.push_indent_from_str(&having);
        having.push_str(&self.format_node(h)?);
        self.pop_indent();

        having.push('\n');

        Ok(having)
    }

    fn format_window_clause(&mut self, w: &[Node]) -> R {
        Ok(format!(
            "{}\n",
            self.one_line_or_many("WINDOW ", false, false, true, 0, |f| f.formatted_list(w))?
        ))
    }

    //#[trace]
    fn format_locking_clause(&mut self, l: &[LockingClauseWrapper]) -> R {
        let mut locking: Vec<String> = vec![];

        for LockingClauseWrapper::LockingClause(c) in l {
            let s = match &c.strength {
                LockClauseStrength::LcsNone => panic!("got locking strength of none!"),
                LockClauseStrength::LcsForkeyshare => "KEY SHARE",
                LockClauseStrength::LcsForshare => "SHARE",
                LockClauseStrength::LcsFornokeyupdate => "NO KEY UPDATE",
                LockClauseStrength::LcsForupdate => "UPDATE",
            };

            let mut rels: Vec<String> = vec![];
            if let Some(lr) = &c.locked_rels {
                rels = lr
                    .iter()
                    .map(|RangeVarWrapper::RangeVar(r)| self.format_range_var(r))
                    .collect();
            }

            let mut wait = "";
            if let Some(w) = &c.wait_policy {
                wait = match w {
                    LockWaitPolicy::Lockwaitblock => "",
                    LockWaitPolicy::Lockwaitskip => "SKIP LOCKED",
                    LockWaitPolicy::Lockwaiterror => "NOWAIT",
                }
            }

            let mut one_line = self.indent_str(&format!("FOR {}", s));
            if !rels.is_empty() {
                one_line.push_str(" OF ");
                one_line.push_str(&rels.join(", "));
            }
            if !wait.is_empty() {
                one_line.push(' ');
                one_line.push_str(wait);
            }
            if self.fits_on_one_line(&one_line, 0) {
                locking.push(one_line);
                continue;
            }

            let mut many = self.indent_str(&format!("FOR {}\n", s));
            self.push_indent_one_level();
            if !rels.is_empty() {
                let of = "OF ";
                for (n, r) in rels.iter().enumerate() {
                    if n == 0 {
                        many.push_str(&self.indent_str(of));
                    } else {
                        many.push_str(&self.indent_str(&" ".repeat(of.len())));
                    }
                    many.push_str(r);
                    if n != rels.len() - 1 {
                        many.push(',');
                    }
                    many.push('\n');
                }
            }
            if !wait.is_empty() {
                many.push_str(&self.indent_str(wait));
            }
            self.pop_indent();

            locking.push(many);
        }

        Ok(format!("{}\n", locking.join("\n")))
    }

    //#[trace]
    fn format_order_by_clause(&mut self, o: &[SortByWrapper]) -> R {
        let maker = |f: &mut Self| {
            let mut order_by: Vec<String> = vec![];
            for SortByWrapper::SortBy(s) in o {
                let mut el = f.format_node(&s.node)?;
                match s.sortby_dir {
                    SortByDir::SortbyDefault => (),
                    SortByDir::SortbyAsc => el.push_str(" ASC"),
                    SortByDir::SortbyDesc => el.push_str(" DESC"),
                    SortByDir::SortbyUsing => {
                        el.push_str(" USING ");
                        match &s.use_op {
                            Some(u) => {
                                // Using a scopeguard here doesn't work because it
                                // takes ownership of formatter and then it's not
                                // available next time through the loop.
                                f.contexts.push(ContextType::OrderByUsingClause);
                                let res = f.formatted_list(u);
                                f.contexts.pop();
                                el.push_str(&res?.join(" "));
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
                order_by.push(el);
            }
            Ok(order_by)
        };

        self.one_line_or_many("ORDER BY ", false, false, true, 0, maker)
    }

    //#[trace]
    fn format_type_cast(&mut self, tc: &TypeCast) -> R {
        let mut type_cast = self.format_node(&*tc.arg)?;
        type_cast.push_str("::");
        type_cast.push_str(&self.format_type_name(&tc.type_name)?);

        // This is some oddity of the parser. It turns TRUE and FALSE literals
        // into this cast expression.
        if type_cast == "'t'::bool" {
            return Ok("TRUE".to_string());
        } else if type_cast == "'f'::bool" {
            return Ok("FALSE".to_string());
        }

        Ok(type_cast)
    }

    //#[trace]
    fn format_type_name(&mut self, tn: &TypeNameWrapper) -> R {
        match tn {
            TypeNameWrapper::TypeName(t) => Ok(t
                .names
                .iter()
                // Is this clone necessary? It feels like there should be a
                // way to work with the original reference until the join.
                .map(|StringStructWrapper::StringStruct(n)| {
                    self.type_renaming.get(&n.str).unwrap_or(&n.str).clone()
                })
                .filter(|n| n != "pg_catalog")
                .collect::<Vec<String>>()
                .join(".")),
        }
    }

    //#[trace]
    fn format_subselect(&mut self, s: &RangeSubselect) -> R {
        let mut subselect = if s.lateral {
            "LATERAL ".to_string()
        } else {
            String::new()
        };
        subselect.push_str("(\n");

        let SelectStmtWrapper::SelectStmt(stmt) = &*s.subquery;
        let mut subformatter = self.new_subformatter();
        // The select will end with a newline, but we want to remove that,
        // then wrap the whole thing in parens, at which point we'll add the
        // trailing newline back.
        let formatted = &subformatter
            .format_select_stmt(&stmt)?
            .trim_end()
            .to_string();
        subselect.push_str(formatted);

        subselect.push('\n');
        subselect.push_str(&self.indent_str(")"));
        if let Some(AliasWrapper::Alias(a)) = &s.alias {
            subselect.push_str(&Self::alias_name(&a.aliasname));
        }

        Ok(subselect)
    }

    //#[trace]
    fn format_range_function(&mut self, rf: &RangeFunction) -> R {
        let funcs = &rf.functions;
        if funcs.is_empty() {
            return Err(Error::RangeFunctionDoesNotHaveAnyFunctions);
        }

        let mut prefix = if rf.lateral {
            "LATERAL ".to_string()
        } else {
            String::new()
        };

        let mut add_parens = false;
        if funcs.len() > 1 {
            prefix.push_str("ROWS FROM ");
            add_parens = true;
        } else if rf.is_rowsfrom {
            prefix.push_str("ROWS FROM ");
        }

        let maker = |f: &mut Self| {
            funcs
                .iter()
                .map(|elt| match &elt.0 {
                    Node::FuncCall(c) => {
                        let mut c = f.format_func_call(&c)?;
                        if let Some(defs) = &elt.1 {
                            c.push_str(" AS ");
                            let last_line_len = f.last_line_len(&c);
                            c.push_str(&f.format_column_def_list(defs, last_line_len)?);
                        }
                        Ok(c)
                    }
                    _ => Err(Error::RangeFunctionHasNonFuncCallFunction),
                })
                .collect::<Result<Vec<_>, _>>()
        };

        let mut range_func = self.one_line_or_many(&prefix, false, add_parens, true, 0, maker)?;
        if rf.alias.is_some() || rf.coldeflist.is_some() {
            range_func.push_str(" AS ");
        }

        if let Some(AliasWrapper::Alias(a)) = &rf.alias {
            range_func.push_str(&a.aliasname);
        }

        if let Some(defs) = &rf.coldeflist {
            if rf.alias.is_some() {
                range_func.push(' ');
            }
            let last_line_len = self.last_line_len(&range_func);
            range_func.push_str(&self.format_column_def_list(&defs, last_line_len)?);
        }

        Ok(range_func)
    }

    fn format_range_table_sample(&mut self, rts: &RangeTableSample) -> R {
        self.contexts.push(ContextType::RangeTableSample);

        let mut table_sample = self.format_node(&rts.relation)?;
        table_sample.push_str(" TABLESAMPLE ");
        table_sample.push_str(&self.joined_list(&rts.method, ".")?);
        table_sample.push('(');
        table_sample.push_str(&self.joined_list(&rts.args, ", ")?);
        table_sample.push(')');

        if let Some(r) = &rts.repeatable {
            table_sample.push_str(" REPEATABLE (");
            table_sample.push_str(&self.format_node(&*r)?);
            table_sample.push(')');
        }

        Ok(table_sample)
    }

    fn format_column_def_list(&mut self, defs: &[ColumnDefWrapper], last_line_len: usize) -> R {
        let maker = |f: &mut Self| {
            defs.iter()
                .map(|d| {
                    let ColumnDefWrapper::ColumnDef(d) = d;
                    f.format_column_def(d)
                })
                .collect::<Result<Vec<_>, _>>()
        };

        self.one_line_or_many("", false, true, true, last_line_len, maker)
    }

    fn format_column_def(&mut self, def: &ColumnDef) -> R {
        // XXX - is there any way to steal this string instead? That'd require
        // passing the ColumnDef as a non-ref, I believe.
        let mut d = def.colname.clone();
        d.push(' ');
        d.push_str(&self.format_type_name(&def.type_name)?);
        Ok(d)
    }

    //#[trace(disable(items_maker))]
    fn one_line_or_many<F>(
        &mut self,
        prefix: &str,
        indent_prefix: bool,
        add_parens: bool,
        join_with_comma: bool,
        current_indent: usize,
        mut items_maker: F,
    ) -> R
    where
        F: FnMut(&mut Formatter) -> Result<Vec<String>, Error>,
    {
        let mut one_line = prefix.to_string();
        if indent_prefix {
            one_line = self.indent_str(&one_line);
        }

        let items = items_maker(self)?;
        if items.is_empty() {
            return Ok(String::new());
        }

        let mut parens: [&str; 2] = ["", ""];
        if add_parens {
            // This could be a string with a space in it. It'd be better to be
            // a bit smarter about the contents somehow.
            if items.len() == 1 && !items[0].contains(' ') {
                parens = ["(", ")"];
            } else {
                parens = ["( ", " )"];
            }
        }

        one_line.push_str(parens[0]);
        if join_with_comma {
            one_line.push_str(&items.join(", "));
        } else {
            one_line.push_str(&items.join(" "));
        }
        one_line.push_str(parens[1]);

        if !one_line.contains('\n') && self.fits_on_one_line(&one_line, current_indent) {
            return Ok(one_line);
        }

        self.many_lines(
            prefix,
            indent_prefix,
            add_parens,
            join_with_comma,
            items_maker,
        )
    }

    //#[trace(disable(items_maker))]
    fn many_lines<F>(
        &mut self,
        prefix: &str,
        indent_prefix: bool,
        add_parens: bool,
        join_with_comma: bool,
        mut items_maker: F,
    ) -> R
    where
        F: FnMut(&mut Formatter) -> Result<Vec<String>, Error>,
    {
        let mut many = prefix.to_string();
        if indent_prefix {
            many = self.indent_str(&many);
        }

        if add_parens {
            many.push_str("(\n");
        }

        // If we're adding parens we will indent every line inside the parens
        // by the standard indentation level. The same applies if the prefix
        // ends in a newline, If not, we need to indent every line _after the
        // first_ with the width of the prefix on the first line.
        if add_parens || prefix.ends_with('\n') {
            self.push_indent_one_level();
        } else {
            self.push_indent_from_str(&many);
        }

        let items = items_maker(self)?;
        let last_idx = items.len() - 1;
        for (n, i) in items.iter().enumerate() {
            // If we added parens then every line must be indented the same
            // way. If the prefix ends with a newline (meaning its a
            // multi-line string), then we should also indent the first line.
            if add_parens || n != 0 || prefix.ends_with('\n') {
                many.push_str(&" ".repeat(self.current_indent()));
            }
            many.push_str(i);
            if n < last_idx {
                if join_with_comma {
                    many.push(',');
                }
                many.push('\n');
            }
        }

        self.pop_indent();

        if add_parens {
            many.push('\n');
            many.push_str(&self.indent_str(")"));
        }

        Ok(many)
    }

    fn fits_on_one_line(&self, line: &str, extra_indent: usize) -> bool {
        if line.contains('\n') {
            return false;
        }
        self.current_indent() + line.len() + extra_indent <= self.max_line_len
    }

    fn last_line_len(&self, text: &str) -> usize {
        let index = match text.rfind('\n') {
            Some(i) => i,
            None => 0,
        };
        text.len() - index
    }

    fn is_in_context(&self, t: ContextType) -> bool {
        self.contexts.contains(&t)
    }

    fn joined_list(&mut self, v: &[Node], joiner: &str) -> R {
        Ok(self.formatted_list(v)?.join(joiner))
    }

    fn formatted_list(&mut self, v: &[Node]) -> Result<Vec<String>, Error> {
        v.iter()
            .map(|n| self.format_node(n))
            .collect::<Result<Vec<_>, _>>()
    }

    fn push_indent_from_str(&mut self, s: &str) {
        let mut indent = s.len();
        if let Some(i) = self.indents.last() {
            indent += i;
        }
        self.push_indent(indent);
    }

    fn push_indent_one_level(&mut self) {
        self.push_indent(self.current_indent() + self.indent_width);
    }

    #[cfg(test)]
    fn push_indent_by(&mut self, by: usize) {
        self.push_indent(self.current_indent() + by);
    }

    fn push_indent(&mut self, indent: usize) {
        self.indents.push(indent);
    }

    fn pop_indent(&mut self) {
        if self.indents.is_empty() {
            panic!("No more indents to pop!");
        }
        self.indents.pop();
    }

    fn len_after_nl(&self, s: &str) -> usize {
        if let Some(nl) = s.rfind('\n') {
            return s.len() - nl;
        }
        s.len()
    }

    fn quote_string(&self, s: &str) -> String {
        format!("'{}'", s.replace("'", "''"))
    }

    fn indent_str(&self, s: &str) -> String {
        let mut indented = " ".repeat(self.current_indent());
        indented.push_str(s);
        indented
    }

    fn current_indent(&self) -> usize {
        match self.indents.last() {
            Some(i) => *i,
            None => panic!("No indents!"),
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
            f.push_indent_by(t.indent);
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
            f.max_line_len = 20;
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
