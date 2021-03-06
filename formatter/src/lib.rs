//mod intermediate;

use pg_pretty_parser::{
    ast::*,
    flags::{FrameOptions, IntervalMask, IntervalMaskError},
};
use scopeguard::guard;
use std::collections::HashMap;
use strum_macros::AsRefStr;
use thiserror::Error;
//use trace::trace;

#[derive(Debug, Error)]
pub enum FormatterError {
    #[error("root contained unexpected statement type: {stmt}")]
    RootContainedUnexpectedStatement { stmt: String },
    #[error("unexpected node {node:?} in {func}")]
    UnexpectedNode { node: String, func: &'static str },
    #[error("no target list for select")]
    NoTargetListForSelect,
    #[error("missing {side} arg for {op} op")]
    MissingSideForOp { side: &'static str, op: String },
    #[error("join type is {jt} but there is no Pg keyword for this")]
    InexpressibleJoinType { jt: String },
    #[error("join type is inner but is not natual and has no qualifiers or using clause")]
    InvalidInnerJoin,
    #[error("cannot mix join strategies but found {strat1} and {strat2} in the FROM clause")]
    CannotMixJoinStrategies {
        strat1: &'static str,
        strat2: &'static str,
    },
    #[error("order by clause had a USING without an op")]
    OrderByUsingWithoutOp,
    #[error("range function node does not have any functions")]
    RangeFunctionDoesNotHaveAnyFunctions,
    #[error("range function's list of func calls contains a non-FuncCall node")]
    RangeFunctionHasNonFuncCallFunction,
    #[error("frame options specified {opt} but did not contain a value")]
    FrameOptionsValueWithoutOffset { opt: String },
    #[error("select contained a res target without a val")]
    SelectResTargetWithoutVal,
    #[error("update contained a res target without a val")]
    UpdateResTargetWithoutVal,
    #[error("{what} contained a res target without a name")]
    ResTargetWithoutName { what: &'static str },
    #[error("index element has no name or expression")]
    IndexElemWithoutNameOrExpr,
    #[error("on conflict clause that is neither DO NOTHING nor DO UPDATE")]
    OnConflictClauseWithUnknownAction,
    #[error("infer clause has no index or elements and does not have a constraint name")]
    InferClauseWithoutIndexElementsOrConstraint,
    #[error("on conflict clause is UPDATE but without any targets to set")]
    OnConflictUpdateWithoutTargets,
    #[error("an element in a list of targets for an UPDATE had no name")]
    UpdateTargetWithoutName,
    #[error("a CREATE TABLE statement contained a 0-length vec of columns")]
    CreateStmtWithZeroLengthColumnsVec,
    #[error("a partition spec had an element without a name or expression")]
    PartitionElemWithoutNameOrExpression,
    #[error("{source:}")]
    IntervalMaskError {
        #[from]
        source: IntervalMaskError,
    },
}

#[derive(AsRefStr, Debug, PartialEq)]
enum ContextType {
    AExpr(u8),
    ExclusionOperator,
    GroupingSet,
    Having,
    InsertStmt,
    JoinQualifiers,
    OnConflictUpdate,
    OrderByUsing,
    RangeTableSample,
    Returning,
    SelectStmt,
    SubLink,
    UniqueConstraintKeys,
    UpdateStmt,
    Where,
}

impl ContextType {
    fn is_statement(&self) -> bool {
        self.as_ref().ends_with("Stmt")
    }
}

macro_rules! new_context {
    ( $self:ident, $context:expr ) => {{
        $self.contexts.push($context);
        guard($self, |s| {
            s.contexts.pop();
        })
    }};
}

#[derive(Debug)]
pub struct Formatter {
    bool_expr_depth: u8,
    max_line_len: usize,
    indent_width: usize,
    indents: Vec<usize>,
    contexts: Vec<ContextType>,
    type_renaming: HashMap<String, String>,
}

type R = Result<String, FormatterError>;

//trace::init_depth_var!();

impl Formatter {
    #[allow(clippy::new_without_default)]
    pub fn new() -> Self {
        let mut type_renaming: HashMap<String, String> = HashMap::new();
        type_renaming.insert("int4".to_string(), "int".to_string());
        type_renaming.insert("int8".to_string(), "bigint".to_string());
        type_renaming.insert("bpchar".to_string(), "char".to_string());

        Self {
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
            Root::RawStmt(RawStmt { stmt, .. }) => {
                match stmt {
                    // DML
                    Node::DeleteStmt(d) => self.format_delete_stmt(d),
                    Node::InsertStmt(i) => self.format_insert_stmt(i),
                    Node::SelectStmt(s) => self.format_select_stmt(s),
                    Node::UpdateStmt(u) => self.format_update_stmt(u),

                    // CREATE statements
                    Node::CreateStmt(c) => self.format_create_table_stmt(c),
                    Node::IndexStmt(c) => self.format_index_stmt(c),

                    _ => Err(FormatterError::RootContainedUnexpectedStatement {
                        stmt: stmt.to_string(),
                    }),
                }
            }
        }
    }

    //#[trace]
    fn format_node(&mut self, node: &Node) -> R {
        match &node {
            // This can show up in subselects
            Node::SelectStmt(s) => self.format_select_stmt(s),

            // expressions
            Node::AConst(a) => Ok(self.format_a_const(a)),
            Node::AExpr(a) => self.format_a_expr(a),
            Node::AIndirection(a) => self.format_a_indirection(a),
            Node::BoolExpr(b) => self.format_bool_expr(b),
            Node::ColumnRef(c) => Ok(self.format_column_ref(c)),
            Node::Constraint(c) => self.format_constraint(c),
            Node::CurrentOfExpr(c) => Ok(self.format_current_of_expr(c)),
            Node::DefElem(d) => self.format_def_elem(d),
            Node::FuncCall(f) => self.format_func_call(f),
            Node::GroupingSet(g) => self.format_grouping_set(g),
            Node::IndexElem(i) => self.format_index_elem(i),
            Node::Integer(i) => Ok(i.ival.to_string()),
            Node::PartitionElem(p) => self.format_partition_elem(p),
            Node::PartitionRangeDatum(p) => self.format_partition_range_datum(p),
            Node::RangeVar(r) => Ok(self.format_range_var(r)),
            Node::ResTarget(r) => self.format_res_target(r),
            Node::RowExpr(r) => self.format_row_expr(r),
            Node::SetToDefault(_) => Ok("DEFAULT".into()),
            Node::SQLValueFunction(f) => Ok(self.format_sql_value_function(f)),
            Node::StringStruct(s) => Ok(self.format_string(&s.str)),
            Node::SubLink(s) => self.format_sub_link(s),
            Node::TypeCast(t) => self.format_type_cast(t),
            Node::WindowDef(w) => self.format_window_def(w, 0),
            _ => Err(FormatterError::UnexpectedNode {
                node: node.to_string(),
                func: "format_node",
            }),
        }
    }

    //#[trace]
    fn format_delete_stmt(&mut self, d: &DeleteStmt) -> R {
        let RangeVarWrapper::RangeVar(r) = &d.relation;

        let mut delete = self.indent_str(&format!("DELETE FROM {}\n", self.format_range_var(r)));

        if let Some(u) = &d.using_clause {
            delete.push_str(&self.format_from_clause("USING", u)?);
        }
        if let Some(w) = &d.where_clause {
            delete.push_str(&self.format_where_clause(w)?);
        }
        if let Some(r) = &d.returning_list {
            delete.push_str(&self.format_returning_clause(r)?);
        }

        Ok(delete)
    }

    //#[trace]
    fn format_insert_stmt(&mut self, i: &InsertStmt) -> R {
        let mut formatter = new_context!(self, ContextType::InsertStmt);

        let RangeVarWrapper::RangeVar(r) = &i.relation;

        let mut insert = formatter.indent_str("INSERT INTO\n");

        formatter.push_indent_one_level();

        let mut prefix = formatter.indent_str(&formatter.format_range_var(r));

        if let Some(cols) = &i.cols {
            prefix.push(' ');
            let maker = |f: &mut Self| {
                cols.iter()
                    .map(|ResTargetWrapper::ResTarget(c)| f.format_res_target(c))
                    .collect::<Result<Vec<_>, _>>()
            };
            insert.push_str(&formatter.one_line_or_many(&prefix, false, true, true, 0, maker)?);
        } else {
            insert.push_str(&prefix);
        }
        insert.push('\n');

        formatter.pop_indent();

        if let Some(o) = &i.r#override {
            match o {
                OverridingKind::OverridingSystemValue => {
                    insert.push_str("OVERRIDING SYSTEM VALUE\n")
                }
                OverridingKind::OverridingUserValue => insert.push_str("OVERRIDING USER VALUE\n"),
                _ => (),
            }
        }

        insert.push_str(&formatter.format_insert_values(&i)?);

        if let Some(OnConflictClauseWrapper::OnConflictClause(occ)) = &i.on_conflict_clause {
            insert.push_str(&formatter.format_on_conflict_clause(occ)?);
        }

        // If this is an INSERT ... SELECT without an ON CONFLICT clause then
        // it already has a newline.
        if !insert.ends_with('\n') {
            insert.push('\n');
        }

        if let Some(r) = &i.returning_list {
            insert.push_str(&formatter.format_returning_clause(r)?);
        }

        Ok(insert)
    }

    //#[trace]
    fn format_select_stmt(&mut self, s: &SelectStmt) -> R {
        let mut formatter = new_context!(self, ContextType::SelectStmt);

        if let Some(mut op) = formatter.select_op(&s.op) {
            // XXX - this is so gross. I think the unstable box matching
            // syntax would make this much less gross.
            let left = match &s.larg {
                Some(l) => {
                    let SelectStmtWrapper::SelectStmt(l) = &**l;
                    formatter.format_select_stmt(l)?
                }
                None => return Err(FormatterError::MissingSideForOp { side: "left", op }),
            };
            let right = match &s.rarg {
                Some(r) => {
                    let SelectStmtWrapper::SelectStmt(r) = &**r;
                    formatter.format_select_stmt(r)?
                }
                None => return Err(FormatterError::MissingSideForOp { side: "left", op }),
            };

            if s.all {
                op.push_str(" ALL");
            }

            return Ok(format!("{}{}\n{}", left, op, right));
        }

        let t = match &s.target_list {
            Some(tl) => tl,
            None => return Err(FormatterError::NoTargetListForSelect),
        };
        let mut select = formatter.format_select_clause(t, s.distinct_clause.as_ref())?;
        if let Some(f) = &s.from_clause {
            select.push_str(&formatter.format_from_clause("FROM", f)?);
        }
        if let Some(w) = &s.where_clause {
            select.push_str(&formatter.format_where_clause(w)?);
        }
        if let Some(g) = &s.group_clause {
            select.push_str(&formatter.format_group_by_clause(g)?);
        }
        if let Some(h) = &s.having_clause {
            select.push_str(&formatter.format_having_clause(h)?);
        }
        if let Some(w) = &s.window_clause {
            select.push_str(&formatter.format_window_clause(w)?);
        }
        if let Some(o) = &s.sort_clause {
            select.push_str(&formatter.format_order_by_clause(o)?);
            select.push('\n');
        }
        select.push_str(&formatter.maybe_format_limit(s)?);
        if let Some(l) = &s.locking_clause {
            select.push_str(&formatter.format_locking_clause(l));
        }

        Ok(select)
    }

    //#[trace]
    fn format_update_stmt(&mut self, u: &UpdateStmt) -> R {
        let mut formatter = new_context!(self, ContextType::UpdateStmt);

        let RangeVarWrapper::RangeVar(r) = &u.relation;

        let mut update =
            formatter.indent_str(&format!("UPDATE {}\n", formatter.format_range_var(r)));

        let maker = |f: &mut Formatter| f.update_stmt_res_target_lines(&u.target_list);

        update.push_str(&formatter.many_lines("SET ", false, false, true, maker)?);
        update.push('\n');

        if let Some(f) = &u.from_clause {
            update.push_str(&formatter.format_from_clause("FROM", f)?);
        }
        if let Some(w) = &u.where_clause {
            update.push_str(&formatter.format_where_clause(w)?);
        }
        if let Some(r) = &u.returning_list {
            update.push_str(&formatter.format_returning_clause(r)?);
        }

        Ok(update)
    }

    //#[trace]
    fn format_create_table_stmt(&mut self, c: &CreateStmt) -> R {
        let RangeVarWrapper::RangeVar(rel) = &c.relation;

        let mut create = self.indent_str("CREATE");
        if let Some(per) = rel.persistence() {
            create.push(' ');
            create.push_str(per);
        }
        create.push_str(" TABLE ");

        if c.if_not_exists {
            create.push_str("IF NOT EXISTS ");
        }

        create.push_str(&self.format_range_var(rel));

        if let Some(i) = &c.inh_relations {
            create.push_str("\nPARTITION OF ");
            // This is a Vec<Node> but if we look at gram.y we see it's always
            // one element.
            create.push_str(&self.format_node(&i[0])?);
        }

        if let Some(elts) = &c.table_elts {
            create.push_str(&self.format_create_table_elements(elts)?);
        }

        create.push('\n');

        if let Some(opts) = &c.options {
            create.push_str(&format!("WITH {}\n", self.parenthesized_list(opts)?));
        }

        if let Some(t) = &c.tablespacename {
            create.push_str("TABLESPACE ");
            create.push_str(&t);
            create.push('\n');
        }

        if let Some(PartitionBoundSpecWrapper::PartitionBoundSpec(p)) = &c.partbound {
            if p.strategy == 'r' {
                create.push_str(&format!(
                    "FOR VALUES FROM {} TO {}\n",
                    self.parenthesized_list(p.lowerdatums.as_ref().unwrap())?,
                    self.parenthesized_list(p.upperdatums.as_ref().unwrap())?,
                ));
            } else {
                // The postgres gram.y seems to indicate that these values
                // could include boolean literals, but a test case with `FOR
                // VALUES IN (true, false)` fails to parse, so apparently it's
                // just strings, numbers, and NULL?
                create.push_str(&format!(
                    "FOR VALUES IN {}\n",
                    self.parenthesized_list(p.listdatums.as_ref().unwrap())?,
                ));
            }
        }

        if let Some(PartitionSpecWrapper::PartitionSpec(p)) = &c.partspec {
            create.push_str(&format!(
                "PARTITION BY {} {}\n",
                p.strategy.to_uppercase(),
                self.parenthesized_list(&p.part_params)?,
            ));
        }

        Ok(create)
    }

    fn format_create_table_elements(&mut self, elts: &[CreateStmtElement]) -> R {
        let mut create = String::new();

        let mut cols: Vec<&ColumnDef> = vec![];
        let mut constraints: Vec<&Constraint> = vec![];
        let mut likes: Vec<&TableLikeClause> = vec![];

        for e in elts {
            match e {
                CreateStmtElement::ColumnDef(e) => cols.push(e),
                CreateStmtElement::Constraint(e) => constraints.push(e),
                CreateStmtElement::TableLikeClause(e) => likes.push(e),
            }
        }

        create.push_str(" (\n");

        self.push_indent_one_level();

        create.push_str(&self.format_column_defs_for_create(&cols)?);
        // Is there a less gross way to do this?
        if !cols.is_empty() && !constraints.is_empty() {
            create.pop();
            create.push_str(",\n");
        }
        create.push_str(&self.format_constraints_for_create(&constraints)?);
        if (!cols.is_empty() || !constraints.is_empty()) && !likes.is_empty() {
            create.pop();
            create.push_str(",\n");
        }

        self.pop_indent();

        create.push_str(&self.indent_str(")"));

        Ok(create)
    }

    fn format_column_defs_for_create(&mut self, cols: &[&ColumnDef]) -> R {
        if cols.is_empty() {
            return Ok(String::new());
        }

        let mut create = String::new();

        // If this ends up being 0 that will not work well.
        let max_name_width = cols
            .iter()
            .map(|c| c.colname.len())
            .max()
            .ok_or(FormatterError::CreateStmtWithZeroLengthColumnsVec)?;

        for (i, cd) in cols.iter().enumerate() {
            let f = self.format_column_def(cd, max_name_width, true)?;
            create.push_str(&self.indent_str(&f));
            if let Some(attrs) = self.column_attributes(&cd)? {
                // Columns in partition tables may not have a type.
                if cd.type_name.is_some() {
                    create.push(' ');
                }
                create.push_str(&attrs.join(" "));
            }
            if i < cols.len() - 1 {
                create.push(',');
            }
            create.push('\n');
        }

        Ok(create)
    }

    fn column_attributes(&mut self, c: &ColumnDef) -> Result<Option<Vec<String>>, FormatterError> {
        let mut attrs: Vec<String> = vec![];

        if let Some(cons) = &c.constraints {
            attrs.append(
                &mut cons
                    .iter()
                    .map(|con| self.format_node(con))
                    .collect::<Result<Vec<_>, _>>()?,
            );
        }

        if let Some(d) = &c.cooked_default {
            attrs.push(format!("DEFAULT {}", &self.format_node(d)?));
        }

        // There are a number of attributes of the ColumnDef that will never
        // be populated, like is_not_null, identity, and others. I'm guessing
        // that at some point in the past, these were set when parsing, but
        // modern Postgres now treats these all as constraints.

        if attrs.is_empty() {
            return Ok(None);
        }

        Ok(Some(attrs))
    }

    fn format_constraints_for_create(&mut self, cons: &[&Constraint]) -> R {
        if cons.is_empty() {
            return Ok(String::new());
        }

        let formatted = cons
            .iter()
            .map(|c| self.format_constraint(c))
            .collect::<Result<Vec<_>, _>>()?;
        let mut create = formatted
            .iter()
            .map(|f| self.indent_str(f))
            .collect::<Vec<String>>()
            .join(",\n");
        create.push('\n');

        Ok(create)
    }

    //#[trace]
    fn format_index_stmt(&mut self, i: &IndexStmt) -> R {
        let mut create = "CREATE ".to_string();
        if i.unique {
            create.push_str("UNIQUE ");
        }
        create.push_str("INDEX ");

        if i.concurrent {
            create.push_str("CONCURRENTLY ");
        }

        if i.if_not_exists {
            create.push_str("IF NOT EXISTS ");
        }

        if let Some(n) = &i.idxname {
            create.push_str(n.as_ref());
            create.push(' ');
        }

        create.push_str("ON ");
        let RangeVarWrapper::RangeVar(r) = &i.relation;
        create.push_str(&self.format_range_var(r));

        if let Some(a) = &i.access_method {
            // If there was no access method set in the original statement,
            // this will be populated. I don't think there's a way to know if
            // someone put an explicit "USING btree" in the statement.
            if a != "btree" {
                create.push_str(&format!(" USING {}", a.to_uppercase()));
            }
        }

        let maker = |f: &mut Self| {
            i.index_params
                .iter()
                .map(|IndexElemWrapper::IndexElem(p)| f.format_index_elem(p))
                .collect::<Result<Vec<_>, _>>()
        };

        create.push(' ');
        create.push_str(&self.one_line_or_many("", false, true, true, 0, maker)?);

        if let Some(opts) = &i.options {
            let params = opts
                .iter()
                .map(|DefElemWrapper::DefElem(d)| self.format_def_elem(d))
                .collect::<Result<Vec<_>, _>>()?
                .join(", ");
            create.push_str(&format!(" WITH ( {} )", params));
        }

        if let Some(t) = &i.table_space {
            create.push_str(&format!(" TABLESPACE {}", t));
        }

        create.push('\n');

        Ok(create)
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

        let maker = |f: &mut Self| {
            tl.iter()
                .map(|t| f.format_target_element(t))
                .collect::<Result<Vec<_>, _>>()
        };
        Ok(format!(
            "{}\n",
            self.one_line_or_many(&prefix, true, false, true, 0, maker)?
        ))
    }

    fn format_string(&self, s: &str) -> String {
        // Depending on the context the string may be a string literal or it
        // may something like an operator. Tracking the context seems to be
        // the only way to figure that out. I have no idea if this actually
        // works in practice though ...
        match self.contexts.last() {
            Some(c) => match c {
                ContextType::AExpr(_)
                | ContextType::ExclusionOperator
                | ContextType::OrderByUsing
                | ContextType::RangeTableSample
                | ContextType::SubLink
                | ContextType::UniqueConstraintKeys => s.to_string(),
                _ => self.quote_string(&s),
            },
            None => self.quote_string(&s),
        }
    }

    fn select_op(&self, op: &SetOperation) -> Option<String> {
        match op {
            SetOperation::SetopExcept => Some("EXCEPT".to_string()),
            SetOperation::SetopIntersect => Some("INTERSECT".to_string()),
            SetOperation::SetopNone => None,
            SetOperation::SetopUnion => Some("UNION".to_string()),
        }
    }

    //#[trace]
    fn format_target_element(&mut self, t: &Node) -> R {
        match &t {
            Node::ResTarget(rt) => self.format_res_target(rt),
            _ => Err(FormatterError::UnexpectedNode {
                node: t.to_string(),
                func: "format_target_element",
            }),
        }
    }

    //#[trace]
    fn format_res_target(&mut self, rt: &ResTarget) -> R {
        // We need to check for OnConflictUpdate before we check for
        // InsertStmt.
        if (self.most_recent_stmt_is(ContextType::UpdateStmt)
            && !self.last_context_is(ContextType::Returning))
            || self.last_context_is(ContextType::OnConflictUpdate)
        {
            return match &rt.name {
                Some(n) => match &rt.val {
                    Some(v) => Ok(format!(
                        "{} = {}",
                        self.format_name_and_maybe_indirection(&n, rt.indirection.as_ref())?,
                        self.format_node(&*v)?,
                    )),
                    None => Err(FormatterError::UpdateResTargetWithoutVal),
                },
                None => Err(FormatterError::ResTargetWithoutName { what: "UPDATE" }),
            };
        } else if self.most_recent_stmt_is(ContextType::InsertStmt)
            && !self.last_context_is(ContextType::Returning)
        {
            return match &rt.name {
                Some(n) => self.format_name_and_maybe_indirection(&n, rt.indirection.as_ref()),
                None => Err(FormatterError::ResTargetWithoutName { what: "INSERT" }),
            };
        }

        if let Some(v) = &rt.val {
            let mut target = self.format_node(v)?;
            if let Some(n) = &rt.name {
                target.push_str(&Self::alias_name(n));
            }
            return Ok(target);
        }

        Err(FormatterError::SelectResTargetWithoutVal)
    }

    //#[trace]
    fn format_name_and_maybe_indirection(
        &mut self,
        n: &str,
        i: Option<&Vec<IndirectionListElement>>,
    ) -> R {
        let indirection = match i {
            Some(inds) => self.format_indirection_list(inds)?,
            None => String::new(),
        };
        Ok(format!("{}{}", n, indirection))
    }

    fn format_a_indirection(&mut self, a: &AIndirection) -> R {
        Ok(format!(
            "{}{}",
            self.format_node(&*a.arg)?,
            self.format_indirection_list(&a.indirection)?,
        ))
    }

    fn format_indirection_list(&mut self, list: &[IndirectionListElement]) -> R {
        Ok(list
            .iter()
            .map(|e| self.format_indirection_element(e))
            .collect::<Result<Vec<_>, _>>()?
            .join(""))
    }

    fn format_indirection_element(&mut self, i: &IndirectionListElement) -> R {
        match i {
            IndirectionListElement::AIndices(i) => self.format_a_indices(i),
            IndirectionListElement::AStar(_) => Ok(".*".to_string()),
            IndirectionListElement::StringStruct(s) => Ok(format!(".{}", s.str)),
        }
    }

    fn format_a_indices(&mut self, i: &AIndices) -> R {
        let upper = self.format_node(&*i.uidx)?;
        if !i.is_slice {
            return Ok(format!("[{}]", upper));
        }

        let lower = match &i.lidx {
            Some(l) => self.format_node(&*l)?,
            None => String::new(),
        };

        Ok(format!("[{}:{}]", lower, upper))
    }

    //#[trace]
    fn format_where_clause(&mut self, w: &Node) -> R {
        let mut formatter = new_context!(self, ContextType::Where);

        let mut wh = formatter.indent_str("WHERE ");

        formatter.push_indent_from_str(&wh);
        wh.push_str(&formatter.format_node(w)?);
        formatter.pop_indent();

        wh.push('\n');

        Ok(wh)
    }

    //#[trace]
    fn format_column_ref(&mut self, c: &ColumnRef) -> String {
        let mut cols: Vec<String> = vec![];
        for f in &c.fields {
            match f {
                ColumnRefField::StringStruct(s) => cols.push(Self::maybe_quote(&s.str)),
                ColumnRefField::AStar(_) => cols.push("*".to_string()),
            }
        }

        cols.join(".")
    }

    //#[trace]
    fn format_current_of_expr(&self, c: &CurrentOfExpr) -> String {
        format!("CURRENT OF {}", c.cursor_name)
    }

    //#[trace]
    fn format_a_const(&mut self, a: &AConst) -> String {
        self.format_value(&a.val)
    }

    //#[trace]
    fn format_value(&mut self, v: &Value) -> String {
        match &v {
            Value::StringStruct(s) => self.quote_string(&s.str),
            Value::BitString(s) => self.quote_string(&s.str),
            Value::Integer(i) => i.ival.to_string(),
            Value::Float(f) => f.str.clone(),
            Value::Null(_) => "NULL".to_string(),
        }
    }

    //#[trace]
    fn format_value_or_type_name(&mut self, v: &ValueOrTypeName) -> R {
        match &v {
            ValueOrTypeName::StringStruct(s) => Ok(self.quote_string(&s.str)),
            ValueOrTypeName::BitString(s) => Ok(self.quote_string(&s.str)),
            ValueOrTypeName::Integer(i) => Ok(i.ival.to_string()),
            ValueOrTypeName::Float(f) => Ok(f.str.clone()),
            ValueOrTypeName::Null(_) => Ok("NULL".into()),
            ValueOrTypeName::TypeName(t) => self.format_type_name(t),
        }
    }

    // XXX - this does terrible things to simple expressions like `2 + 3 + 4`,
    // adding unnecessary parens. Every additional operate gets it own new
    // parens. This needs to be fixed.
    //
    //#[trace]
    fn format_a_expr(&mut self, a: &AExpr) -> R {
        let last_p = match self.contexts.last() {
            Some(ContextType::AExpr(p)) => *p,
            _ => 0,
        };
        let current_p = self.operator_precedence(&a.name);

        let mut formatter = new_context!(self, ContextType::AExpr(current_p));

        let op = match &a.kind {
            AExprKind::AExprParen => panic!("no idea how to handle AExprParen kind of AExpr"),
            _ => {
                let o = a.name.as_ref().unwrap_or_else(|| {
                    panic!("must have a name defined for a {} kind of AExpr", a.kind)
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
            // XXX - need to handle BETWEEN - might require refactoring to
            // pass a format string to format_infix_expr, like "BETWEEN {} AND
            // {}", and then format_infix_expr would invoke format! to fill in
            // the left & right expressions.
            //
            // For all the rest we can use the op as is.
            _ => op,
        };

        formatter.format_infix_expr(&a.lexpr, real_op, &a.rexpr, current_p, last_p)
    }

    // From https://www.postgresql.org/docs/13/sql-syntax-lexical.html
    fn operator_precedence(&self, name: &Option<List>) -> u8 {
        let op = if let Some(n) = &name {
            if n.len() == 1 {
                if let Node::StringStruct(StringStruct { str: s }) = &n[0] {
                    s
                } else {
                    return 0;
                }
            } else {
                return 0;
            }
        } else {
            return 0;
        };

        // This isn't all operators, it's just the ones that are going to show
        // up in an AExpr. Things like array access and table/column name
        // separators are handled elsewhere. The numbers are based on their
        // position in the table, with the lowest precedence being 1.
        match op.as_ref() {
            "^" => 10,
            "*" | "/" | "%" => 9,
            "+" | "-" => 8,
            "<" | ">" | "=" | "<=" | ">=" | "<>" => 6,
            "IS" | "ISNULL" | "NOTNULL" => 5,
            _ => 7,
        }
    }

    fn maybe_not(&self, op: &str) -> &str {
        match op {
            "=" => "",
            "<>" => "NOT ",
            _ => panic!("got an AExprOf AExpr with an invalid op name: {}", op),
        }
    }

    //#[trace]
    fn format_infix_expr(
        &mut self,
        left: &Node,
        op: String,
        right: &OneOrManyNodes,
        current_p: u8,
        last_p: u8,
    ) -> R {
        let mut e: Vec<String> = vec![self.format_node(&left)?, op];
        match right {
            OneOrManyNodes::One(r) => e.push(self.format_node(&r)?),
            // XXX - Is this right? The right side could be the right side of
            // something like "foo IN (1, 2)", but could it also be something
            // else that shouldn't be joined by commas?
            OneOrManyNodes::Many(r) => {
                e.push(self.one_line_or_many("", false, true, true, 0, |f| f.formatted_list(&r))?)
            }
        }

        // If our AST has a higher precedence operator deeper in the tree than
        // the current operator, then we know there were parens surrounding
        // the higher precedence operator in the original SQL.
        if last_p > current_p {
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

        // The first level of bool expr immediately after a "HAVING", "ON", or
        // "WHERE" does not need additional indentation.
        if self.bool_expr_depth == 1
            && (self.last_context_is(ContextType::Having)
                || self.last_context_is(ContextType::JoinQualifiers)
                || self.last_context_is(ContextType::Where))
        {
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
    fn format_def_elem(&mut self, d: &DefElem) -> R {
        let elem = d.defname.to_string();
        match elem.as_ref() {
            "fastupdate" | "fillfactor" => Ok(format!(
                "{} = {}",
                elem,
                self.format_value_or_type_name(&d.arg)?
            )),
            r => panic!("unhandled defname: {}", r),
        }
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
            .filter_map(|n| match n {
                // We don't want to quote a string here.
                Node::StringStruct(s) => {
                    // The parser will stick this in front of lots of func
                    // calls so you get "pg_catalog.date_part", but no one
                    // wants to see that (I think).
                    if s.str == "pg_catalog" {
                        return None;
                    }
                    Some(Ok(s.str.clone()))
                }
                _ => Some(self.format_node(&n)),
            })
            .collect::<Result<Vec<_>, _>>()?
            .join(".");
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
                let f = self.joined_list(a, ", ")?;
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
                return Err(FormatterError::FrameOptionsValueWithoutOffset {
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
                return Err(FormatterError::FrameOptionsValueWithoutOffset {
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
        let mut formatter = new_context!(self, ContextType::SubLink);

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
                    j.push_str(&self.joined_list(o, " ")?);
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
    fn format_sql_value_function(&mut self, v: &SQLValueFunction) -> String {
        match v.op {
            SQLValueFunctionOp::SvfopCurrentCatalog => "current_catalog".into(),
            SQLValueFunctionOp::SvfopCurrentDate => "current_date".into(),
            SQLValueFunctionOp::SvfopCurrentRole => "current_role".into(),
            SQLValueFunctionOp::SvfopCurrentSchema => "current_schema".into(),
            SQLValueFunctionOp::SvfopCurrentTime => "current_time".into(),
            SQLValueFunctionOp::SvfopCurrentTimeN => format!("current_time({})", v.typmod.unwrap()),
            SQLValueFunctionOp::SvfopCurrentTimestamp => "current_timestamp".into(),
            SQLValueFunctionOp::SvfopCurrentTimestampN => {
                format!("current_timestamp({})", v.typmod.unwrap())
            }
            SQLValueFunctionOp::SvfopCurrentUser => "current_user".into(),
            SQLValueFunctionOp::SvfopLocaltime => "localtime".into(),
            SQLValueFunctionOp::SvfopLocaltimeN => format!("localtime({})", v.typmod.unwrap()),
            SQLValueFunctionOp::SvfopLocaltimestamp => "localtimestamp".into(),
            SQLValueFunctionOp::SvfopLocaltimestampN => {
                format!("localtimestamp({})", v.typmod.unwrap())
            }
            SQLValueFunctionOp::SvfopSessionUser => "session_user".into(),
            SQLValueFunctionOp::SvfopUser => "user".into(),
        }
    }

    //#[trace]
    fn format_grouping_set(&mut self, gs: &GroupingSet) -> R {
        let is_nested = self.is_in_context(ContextType::GroupingSet);

        let mut formatter = new_context!(self, ContextType::GroupingSet);

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

        let maker = |f: &mut Self| -> Result<Vec<String>, FormatterError> {
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
    fn format_from_clause(&mut self, keyword: &str, fc: &[Node]) -> R {
        let start = format!("{} ", keyword);
        let mut from = self.indent_str(&start);

        // We want to indent by the width of "FROM " or "USING ", not any
        // additional indent before that string.
        self.push_indent_from_str(&start);
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
                return Err(FormatterError::CannotMixJoinStrategies {
                    strat1: "NATURAL",
                    strat2: "ON",
                });
            }
            if j.using_clause.is_some() {
                return Err(FormatterError::CannotMixJoinStrategies {
                    strat1: "NATURAL",
                    strat2: "USING",
                });
            }
        }
        if j.quals.is_some() && j.using_clause.is_some() {
            return Err(FormatterError::CannotMixJoinStrategies {
                strat1: "ON",
                strat2: "USING",
            });
        }

        e.push_str(&self.format_from_element(&j.larg, is_first)?);
        e.push('\n');
        e.push_str(&" ".repeat(self.current_indent()));
        e.push_str(&self.join_type(j)?);
        e.push(' ');
        e.push_str(&self.format_from_element(&j.rarg, is_first)?);

        if let Some(q) = &j.quals {
            e.push_str(&self.format_join_qualifiers(&*q)?);
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

    fn join_type(&self, j: &JoinExpr) -> Result<String, FormatterError> {
        let jt = match &j.jointype {
            JoinType::JoinInner => {
                if j.quals.is_some() || j.using_clause.is_some() || j.is_natural {
                    "JOIN"
                } else {
                    return Ok("CROSS JOIN".to_string());
                }
            }
            JoinType::JoinLeft => "LEFT OUTER JOIN",
            JoinType::JoinRight => "RIGHT OUTER JOIN",
            JoinType::JoinFull => "FULL OUTER JOIN",
            _ => {
                return Err(FormatterError::InexpressibleJoinType {
                    jt: j.jointype.to_string(),
                })
            }
        };

        if j.is_natural {
            return Ok(format!("NATURAL {}", jt));
        }

        Ok(jt.to_string())
    }

    //#[trace]
    fn format_join_qualifiers(&mut self, q: &Node) -> R {
        let mut formatter = new_context!(self, ContextType::JoinQualifiers);

        let mut quals = "\n".to_string();
        // For now, we'll just always put a newline before the "ON ..."
        // clause. This simplifies the formatting and makes it consistent
        // with how we format WHERE clauses.
        formatter.push_indent_one_level();
        quals.push_str(&formatter.indent_str("ON "));
        quals.push_str(&formatter.format_node(q)?);
        formatter.pop_indent();

        Ok(quals)
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
    fn format_partition_elem(&mut self, p: &PartitionElem) -> R {
        if let Some(n) = &p.name {
            Ok(n.clone())
        } else if let Some(e) = &p.expr {
            self.format_node(&*e)
        } else {
            Err(FormatterError::PartitionElemWithoutNameOrExpression)
        }
    }

    fn format_partition_range_datum(&mut self, p: &PartitionRangeDatum) -> R {
        match &p.kind {
            PartitionRangeDatumKind::PartitionRangeDatumMinvalue => Ok("MINVALUE".into()),
            PartitionRangeDatumKind::PartitionRangeDatumMaxvalue => Ok("MAXVALUE".into()),
            PartitionRangeDatumKind::PartitionRangeDatumValue => {
                self.format_node(&*p.value.as_ref().unwrap())
            }
        }
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
        let mut formatter = new_context!(self, ContextType::Having);

        let mut having = formatter.indent_str("HAVING ");

        formatter.push_indent_from_str(&having);
        having.push_str(&formatter.format_node(h)?);
        formatter.pop_indent();

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
    fn format_locking_clause(&mut self, l: &[LockingClauseWrapper]) -> String {
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

        format!("{}\n", locking.join("\n"))
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
                                f.contexts.push(ContextType::OrderByUsing);
                                let res = f.formatted_list(u);
                                f.contexts.pop();
                                el.push_str(&res?.join(" "));
                            }
                            None => return Err(FormatterError::OrderByUsingWithoutOp),
                        }
                    }
                }
                if let Some(sb) = f.format_sort_by_nulls_ordering(&s.sortby_nulls) {
                    el.push(' ');
                    el.push_str(&sb);
                }
                order_by.push(el);
            }
            Ok(order_by)
        };

        self.one_line_or_many("ORDER BY ", false, false, true, 0, maker)
    }

    fn format_sort_by_nulls_ordering(&self, o: &SortByNulls) -> Option<String> {
        match o {
            SortByNulls::SortbyNullsDefault => None,
            SortByNulls::SortbyNullsFirst => Some("NULLS FIRST".into()),
            SortByNulls::SortbyNullsLast => Some("NULLS LAST".into()),
        }
    }

    //#[trace]
    fn format_type_cast(&mut self, tc: &TypeCast) -> R {
        let mut type_cast = self.format_node(&*tc.arg)?;
        type_cast.push_str("::");
        let TypeNameWrapper::TypeName(tn) = &tc.type_name;
        type_cast.push_str(&self.format_type_name(&tn)?);

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
    fn format_type_name(&mut self, tn: &TypeName) -> R {
        let mut name = String::new();
        if tn.setof {
            name.push_str("SET OF ");
        }

        let n = match &tn.names {
            Some(names) => names
                .iter()
                // Is this clone necessary? It feels like there should be a
                // way to work with the original reference until the join.
                .map(|StringStructWrapper::StringStruct(n)| {
                    self.type_renaming.get(&n.str).unwrap_or(&n.str).clone()
                })
                .filter(|n| n != "pg_catalog")
                .collect::<Vec<String>>()
                .join("."),
            None => panic!("not sure how to handle a nameless type"),
        };

        name.push_str(&n);

        // XXX - I have no idea when this is set to a value that _isn't_ -1.
        if let Some(m) = &tn.typemod {
            if *m != -1 {
                name.push_str(&format!("({})", m));
            }
        }
        if let Some(m) = &tn.typmods {
            if m.len() == 1 {
                if n.eq("interval") {
                    // If the type is INTERVAL then we _should_ have exactly
                    // one node, which is an integer constant. That constant
                    // is actually an INTERVAL_MASK flag representing an
                    // interval precision.
                    let mask = match &m[0] {
                        Node::AConst(AConst {
                            val: Value::Integer(Integer { ival: i }),
                            location: _,
                        }) => IntervalMask::from_i64(*i)?,
                        _ => panic!("argh"),
                    };
                    name.push(' ');
                    name.push_str(&mask.type_modifiers()?);
                } else {
                    name.push_str(&format!("({})", &self.format_node(&m[0])?));
                }
            }
        }

        if let Some(ab) = &tn.array_bounds {
            for e in ab {
                match e {
                    // If the element is -1 then this is an unbounded array.
                    Node::Integer(Integer { ival: -1 }) => name.push_str("[]"),
                    _ => name.push_str(&format!("[{}]", self.format_node(e)?)),
                }
            }
        }

        Ok(name)
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
            return Err(FormatterError::RangeFunctionDoesNotHaveAnyFunctions);
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
                    Node::FuncCall(fc) => {
                        let mut c = f.format_func_call(fc)?;
                        if let Some(defs) = &elt.1 {
                            c.push_str(" AS ");
                            let last_line_len = f.last_line_len(&c);
                            c.push_str(&f.format_column_def_list(defs, last_line_len)?);
                        }
                        Ok(c)
                    }
                    _ => Err(FormatterError::RangeFunctionHasNonFuncCallFunction),
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

    //#[trace]
    fn format_range_table_sample(&mut self, rts: &RangeTableSample) -> R {
        let mut formatter = new_context!(self, ContextType::RangeTableSample);

        let mut table_sample = formatter.format_node(&rts.relation)?;
        table_sample.push_str(" TABLESAMPLE ");
        table_sample.push_str(&formatter.joined_list(&rts.method, ".")?);
        table_sample.push('(');
        table_sample.push_str(&formatter.joined_list(&rts.args, ", ")?);
        table_sample.push(')');

        if let Some(r) = &rts.repeatable {
            table_sample.push_str(" REPEATABLE (");
            table_sample.push_str(&formatter.format_node(&*r)?);
            table_sample.push(')');
        }

        Ok(table_sample)
    }

    //#[trace]
    fn format_column_def_list(&mut self, defs: &[ColumnDefWrapper], last_line_len: usize) -> R {
        let maker = |f: &mut Self| {
            defs.iter()
                .map(|ColumnDefWrapper::ColumnDef(d)| f.format_column_def(d, 0, false))
                .collect::<Result<Vec<_>, _>>()
        };

        self.one_line_or_many("", false, true, true, last_line_len, maker)
    }

    //#[trace]
    fn format_column_def(&mut self, def: &ColumnDef, name_padding: usize, in_table: bool) -> R {
        let mut d = if name_padding > 1 {
            self.pad_str(&def.colname, name_padding)
        } else {
            def.colname.clone()
        };

        if in_table {
            d.push_str("  ");
        } else {
            d.push(' ');
        }

        if let Some(TypeNameWrapper::TypeName(tn)) = &def.type_name {
            d.push_str(&self.format_type_name(&tn)?);
        }

        Ok(d)
    }

    fn format_constraint(&mut self, c: &Constraint) -> R {
        match &c.contype {
            ConstrType::ConstrNull => Ok("NULL".into()),
            ConstrType::ConstrNotnull => Ok("NOT NULL".into()),
            ConstrType::ConstrDefault => self.format_default_constraint(c),
            ConstrType::ConstrIdentity => Ok(self.format_identity_constraint(c)),
            ConstrType::ConstrCheck => self.format_check_constraint(c),
            ConstrType::ConstrPrimary => self.format_unique_constraint(c, true),
            ConstrType::ConstrUnique => self.format_unique_constraint(c, false),
            ConstrType::ConstrExclusion => self.format_exclusion_constraint(c),
            ConstrType::ConstrForeign => Ok("FOREIGN".into()),
            // attributes for previous constraint node
            ConstrType::ConstrAttrDeferrable => Ok(String::new()),
            ConstrType::ConstrAttrNotDeferrable => Ok(String::new()),
            ConstrType::ConstrAttrDeferred => Ok(String::new()),
            ConstrType::ConstrAttrImmediate => Ok(String::new()),
        }
    }

    fn format_default_constraint(&mut self, c: &Constraint) -> R {
        let e = match &c.raw_expr {
            Some(e) => e,
            None => panic!("DEFAULT constraint without an expression"),
        };
        Ok(format!("DEFAULT {}", self.format_node(&*e)?))
    }

    fn format_identity_constraint(&mut self, c: &Constraint) -> String {
        let g = match c.generated_when() {
            Some(g) => g,
            None => panic!("IDENTITY constraint without generated_when"),
        };
        format!("GENERATED {} AS IDENTITY", g)
    }

    fn format_check_constraint(&mut self, c: &Constraint) -> R {
        let e = match &c.raw_expr {
            Some(e) => e,
            None => panic!("CHECK constraint without an expression"),
        };

        let mut check = "CHECK (".to_string();

        // This
        let expr = self.format_node(&*e)?;
        if expr.contains('\n') {
            check.push('\n');
            self.push_indent_one_level();
            check.push_str(&self.format_node(&*e)?);
            self.pop_indent();
            check.push('\n');
            check.push_str(&self.indent_str(")"));
        } else {
            check.push(' ');
            check.push_str(expr.trim());
            check.push_str(" )");
        }

        match &c.conname {
            Some(n) => Ok(format!("CONSTRAINT {} {}", n, check)),
            None => Ok(check),
        }
    }

    fn format_unique_constraint(&mut self, c: &Constraint, is_pk: bool) -> R {
        let typ = if is_pk { "PRIMARY KEY" } else { "UNIQUE" };
        let mut cons = match &c.conname {
            Some(n) => format!("CONSTRAINT {} {}", n, typ),
            None => typ.into(),
        };

        if let Some(k) = &c.keys {
            cons.push_str(&self.format_keys(k)?);
        }

        if let Some(opts) = &c.options {
            cons.push_str(&format!(" WITH {}", self.parenthesized_list(opts)?));
        }

        Ok(cons)
    }

    //#[trace]
    fn format_exclusion_constraint(&mut self, c: &Constraint) -> R {
        let am = match &c.access_method {
            Some(a) => a,
            None => panic!("EXCLUDE constraint without an access_method"),
        };

        let ex = match &c.exclusions {
            Some(e) => e,
            None => panic!("EXCLUDE constraint without exclusions"),
        };

        Ok(format!(
            "EXCLUDE USING {} ( {} )",
            am,
            ex.iter()
                .map(|e| self.format_exclusion(e))
                .collect::<Result<Vec<_>, _>>()?
                .join(", "),
        ))
    }

    //#[trace]
    fn format_exclusion(&mut self, e: &Exclusion) -> R {
        let mut excl = self.format_node(&e.0)?;
        excl.push_str(" WITH ");

        let mut formatter = new_context!(self, ContextType::ExclusionOperator);
        let oper =
            e.1.iter()
                .map(|n| formatter.format_node(n))
                .collect::<Result<Vec<_>, _>>()?
                // I'm not sure if this is right. What produces multiple
                // elements for the operator?
                .join(" ");
        excl.push_str(&oper);

        Ok(excl)
    }

    //#[trace]
    fn format_keys(&mut self, k: &[Node]) -> R {
        let mut formatter = new_context!(self, ContextType::UniqueConstraintKeys);

        let mut keys = " (".to_string();

        let cols = formatter.formatted_list(k)?;
        if cols.len() > 1 {
            keys.push(' ');
        }
        keys.push_str(&cols.join(", "));
        if cols.len() > 1 {
            keys.push(' ');
        }
        keys.push(')');

        Ok(keys)
    }

    //#[trace]
    fn format_insert_values(&mut self, i: &InsertStmt) -> R {
        match &i.select_stmt {
            Some(SelectStmtWrapper::SelectStmt(s)) => {
                if !Self::is_values_insert(s) {
                    return self.format_select_stmt(s);
                }
                let mut values = "VALUES\n".to_string();

                self.push_indent_one_level();

                values.push_str(
                    &s.values_lists
                        .as_ref()
                        .unwrap()
                        .iter()
                        .map(|v| {
                            let maker = |f: &mut Self| f.formatted_list(v);
                            self.one_line_or_many("", true, true, true, 0, maker)
                        })
                        .collect::<Result<Vec<_>, _>>()?
                        .join(",\n"),
                );

                self.pop_indent();

                values.push('\n');

                Ok(values)
            }
            None => Ok("DEFAULT VALUES\n".to_string()),
        }
    }

    // The values are given as a SelectStmt. If the _only_ thing that's not
    // none in the SelectStmt is the values_list then it's a simple "INSERT
    // INTO x VALUES ( ... )" type of INSERT. Otherwise the select is a real
    // SELECT so we have "INSERT INTO x SELECT ...".
    //
    //#[trace]
    fn is_values_insert(s: &SelectStmt) -> bool {
        s.distinct_clause.is_none()
            && s.into_clause.is_none()
            && s.target_list.is_none()
            && s.from_clause.is_none()
            && s.where_clause.is_none()
            && s.group_clause.is_none()
            && s.having_clause.is_none()
            && s.window_clause.is_none()
            && s.sort_clause.is_none()
            && s.limit_offset.is_none()
            && s.limit_count.is_none()
            && s.locking_clause.is_none()
            && s.with_clause.is_none()
            && s.larg.is_none()
            && s.rarg.is_none()
            && s.values_lists.is_some()
    }

    //#[trace]
    fn format_on_conflict_clause(&mut self, occ: &OnConflictClause) -> R {
        let mut on_conflict = "ON CONFLICT ".to_string();

        if let Some(InferClauseWrapper::InferClause(i)) = &occ.infer {
            on_conflict.push_str(&self.format_infer_clause(&i)?);
        }

        match occ.action {
            OnConflictAction::OnconflictNothing => {
                on_conflict.push_str(&self.indent_str("DO NOTHING"));
                return Ok(on_conflict);
            }
            OnConflictAction::OnconflictUpdate => (),
            _ => return Err(FormatterError::OnConflictClauseWithUnknownAction),
        }

        let targets = match occ.target_list.as_ref() {
            Some(t) => t,
            None => return Err(FormatterError::OnConflictUpdateWithoutTargets),
        };

        let do_update = "DO UPDATE SET ";
        on_conflict.push_str(&self.format_on_conflict_update_clause(targets, do_update)?);

        if let Some(w) = &occ.where_clause {
            on_conflict.push('\n');
            self.push_indent_one_level();
            on_conflict.push_str(&self.format_where_clause(&*w)?);
            self.pop_indent();
        }

        Ok(on_conflict)
    }

    //#[trace]
    fn format_infer_clause(&mut self, i: &InferClause) -> R {
        let mut infer = String::new();
        if let Some(ie) = &i.index_elems {
            let maker = |f: &mut Self| f.formatted_list(ie);
            infer.push_str(&self.one_line_or_many("", false, true, true, 0, maker)?);
            infer.push('\n');
            if let Some(w) = &i.where_clause {
                infer.push_str(&self.format_where_clause(&*w)?);
                infer.push('\n');
            }
        } else if let Some(c) = &i.conname {
            infer = format!("ON CONSTRAINT {}", c);
        } else {
            return Err(FormatterError::InferClauseWithoutIndexElementsOrConstraint);
        }

        Ok(infer)
    }

    // XXX - This is still incomplete. It has enough for ON CONFLICT clauses,
    // but for CREATE INDEX it needs to be finished.
    //
    //#[trace]
    fn format_index_elem(&mut self, i: &IndexElem) -> R {
        let mut elem = String::new();
        if let Some(n) = &i.name {
            elem.push_str(n);
        } else if let Some(e) = &i.expr {
            elem.push_str(&self.format_node(&*e)?);
        } else {
            return Err(FormatterError::IndexElemWithoutNameOrExpr);
        }

        if let Some(c) = &i.collation {
            elem.push_str(" COLLATE ");
            // This will only ever have 1 element per gram.y. I'm not sure it
            // can ever be something other than a StringStruct.
            match &c[0] {
                Node::StringStruct(StringStruct { str: name }) => {
                    elem.push_str(&Self::maybe_quote(name))
                }
                _ => elem.push_str(&self.format_node(&c[0])?),
            }
        }

        if let Some(o) = self.format_sort_by_nulls_ordering(&i.nulls_ordering) {
            elem.push(' ');
            elem.push_str(&o);
        }

        if let Some(o) = &i.opclass {
            elem.push(' ');
            elem.push_str(&self.formatted_list(o)?.join(" "));
        }

        Ok(elem)
    }

    fn format_on_conflict_update_clause(
        &mut self,
        target_list: &[Node],
        prefix: &str,
    ) -> Result<String, FormatterError> {
        let mut formatter = new_context!(self, ContextType::OnConflictUpdate);

        let maker = |f: &mut Self| {
            target_list
                .iter()
                .map(|t| f.format_target_element(t))
                .collect::<Result<Vec<_>, _>>()
        };
        formatter.many_lines(prefix, false, false, true, maker)
    }

    //#[trace]
    fn format_returning_clause(&mut self, r: &[Node]) -> R {
        let mut formatter = new_context!(self, ContextType::Returning);

        let maker = |f: &mut Self| f.formatted_list(r);
        let mut returning =
            formatter.one_line_or_many("RETURNING ", true, false, true, 0, maker)?;
        returning.push('\n');

        Ok(returning)
    }

    fn update_stmt_res_target_lines(
        &mut self,
        list: &[Node],
    ) -> Result<Vec<String>, FormatterError> {
        let mut lines: Vec<String> = vec![];

        let mut i = 0;
        while i < list.len() {
            if let Node::ResTarget(t) = &list[i] {
                let v = match &t.val {
                    Some(v) => &**v,
                    None => {
                        panic!("got a ResTarget in an UpdateStmt target_list which is None!")
                    }
                };
                let l = if let Node::MultiAssignRef(m) = v {
                    let mut names: Vec<String> = vec![];
                    let mut j = 1;
                    while j <= m.ncolumns {
                        names.push(self.res_target_name_or_panic(&list[i]));
                        i += 1;
                        j += 1;
                    }
                    self.format_multi_assign(names, m)?
                } else {
                    self.format_res_target(t)?
                };
                lines.push(l);
            } else {
                panic!("got a node that is not a ResTarget in an UpdateStmt target_list!");
            };

            i += 1;
        }

        Ok(lines)
    }

    fn res_target_name_or_panic(&self, node: &Node) -> String {
        if let Node::ResTarget(t) = node {
            if let Some(n) = &t.name {
                return n.clone();
            }
            panic!("got a ResTarget in an UpdateStmt target_list without a name!");
        }
        panic!("got a Node in an UpdateStmt target_list that is not a ResTarget!");
    }

    //#[trace]
    fn format_multi_assign(&mut self, names: Vec<String>, m: &MultiAssignRef) -> R {
        if names.len() == 1 {
            return Ok(format!("{} = {}", names[0], self.format_node(&*m.source)?));
        }

        let mut left = "( ".to_string();
        left.push_str(&names.join(", "));
        left.push_str(" )");

        // This seems to just work if RHS is a subselect, wrapping and all.
        let right = self.format_node(&*m.source)?;

        Ok(format!("{} = {}", left, right))
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
        F: FnMut(&mut Self) -> Result<Vec<String>, FormatterError>,
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
        F: FnMut(&mut Self) -> Result<Vec<String>, FormatterError>,
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
        let index = text.rfind('\n').unwrap_or(0);
        text.len() - index
    }

    fn is_in_context(&self, t: ContextType) -> bool {
        self.contexts.contains(&t)
    }

    fn last_context_is(&self, t: ContextType) -> bool {
        match self.contexts.last() {
            Some(l) => *l == t,
            None => false,
        }
    }

    fn most_recent_stmt_is(&self, t: ContextType) -> bool {
        match self.contexts.iter().rev().find(|c| c.is_statement()) {
            Some(c) => *c == t,
            // XXX - Can this happen?
            None => false,
        }
    }

    fn joined_list(&mut self, v: &[Node], joiner: &str) -> R {
        Ok(self.formatted_list(v)?.join(joiner))
    }

    fn parenthesized_list(&mut self, v: &[Node]) -> R {
        let list = self.formatted_list(v)?.join(", ");
        if list.contains(&[',', '(', ' '][..]) {
            return Ok(format!("( {} )", list));
        }
        Ok(format!("({})", list))
    }

    fn formatted_list(&mut self, v: &[Node]) -> Result<Vec<String>, FormatterError> {
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

    fn pad_str(&self, s: &str, width: usize) -> String {
        format!("{:width$}", s, width = width)
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
