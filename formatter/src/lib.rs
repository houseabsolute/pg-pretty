mod context;
mod formatter;

use context::*;
use formatter::*;
use pg_pretty_parser::{
    ast::*,
    flags::{FrameOptions, IntervalMask, IntervalMaskError},
};
use std::collections::HashMap;
use std::result;
use thiserror::Error;
//use trace::trace;

#[derive(Debug, Error)]
pub enum TransformerError {
    #[error("root contained unexpected statement type: {stmt}")]
    RootContainedUnexpectedStatement { stmt: String },
    #[error("unexpected node {node} in {func}")]
    UnexpectedNode { node: String, func: &'static str },
    #[error("unexpected node type in list - {node} - expected {expected}")]
    UnexpectedNodeInList {
        node: String,
        expected: &'static str,
    },
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
    RangeFunctionHasNonFuncCallElement,
    #[error("frame options specified {opt} but did not contain a value")]
    FrameOptionsValueWithoutOffset { opt: String },
    #[error("select contained a res target without a val")]
    SelectResTargetWithoutVal,
    #[error("update contained a res target without a val")]
    UpdateResTargetWithoutVal,
    #[error("update contained a res target without a name")]
    UpdateResTargetWithoutName,
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
    #[error("aexpr without a name")]
    AExprWithoutName,
    #[error("a boolean expression {bool_op} clause had {num} arguments")]
    BoolExprWithWrongNumberOfArguments { bool_op: &'static str, num: usize },
    #[error("{sublink_type} sublink without an operator")]
    SubLinkHasNoOperator { sublink_type: &'static str },
    #[error("coercion_form should never be CoerceExplicitCast for a RowExpr")]
    RowExprWithExplicitCast,
    #[error("{source:}")]
    IntervalMaskError {
        #[from]
        source: IntervalMaskError,
    },
    #[error("{source:}")]
    FormatterError {
        #[from]
        source: formatter::FormatterError,
    },
}

#[derive(Debug)]
pub struct Transformer {
    bool_expr_depth: u8,
    contexts: Vec<ContextType>,
    max_line_len: usize,
    indent_width: usize,
    indents: Vec<usize>,
    type_renaming: HashMap<String, String>,
}

pub type Result<T> = result::Result<T, TransformerError>;

//trace::init_depth_var!();

impl Transformer {
    #[allow(clippy::new_without_default)]
    pub fn new() -> Self {
        let mut type_renaming: HashMap<String, String> = HashMap::new();
        type_renaming.insert("int4".to_string(), "int".to_string());
        type_renaming.insert("int8".to_string(), "bigint".to_string());
        type_renaming.insert("bpchar".to_string(), "char".to_string());

        Self {
            bool_expr_depth: 0,
            contexts: vec![],
            max_line_len: 100,
            indent_width: 4,
            indents: vec![0],
            type_renaming,
        }
    }

    pub fn format_root_stmt(&mut self, root: &Root) -> Result<String> {
        let stmt = match root {
            Root::RawStmt(RawStmt { stmt, .. }) => {
                match stmt {
                    // DML
                    Node::DeleteStmt(d) => self.delete_stmt(d)?,
                    Node::InsertStmt(i) => self.insert_stmt(i)?,
                    Node::SelectStmt(s) => self.select_stmt(s)?,
                    Node::UpdateStmt(u) => self.update_stmt(u)?,

                    // // CREATE statements
                    // Node::CreateStmt(c) => self.format_create_table_stmt(c),
                    Node::IndexStmt(c) => self.index_stmt(c)?,
                    _ => {
                        return Err(TransformerError::RootContainedUnexpectedStatement {
                            stmt: stmt.to_string(),
                        })
                    }
                }
            }
        };
        let f = stmt.as_string(None)?;
        Ok(f)
    }

    fn node(&mut self, node: &Node) -> Result<Box<dyn Chunk>> {
        match &node {
            // This can show up in subselects
            Node::SelectStmt(s) => Ok(Box::new(self.select_stmt(s)?)),

            // expressions
            Node::AConst(a) => Ok(self.a_const(a)),
            Node::AExpr(a) => self.a_expr(a),
            //            Node::AIndirection(a) => self.a_indirection(a),
            Node::BoolExpr(b) => self.bool_expr(b),
            Node::ColumnRef(c) => Ok(self.column_ref(c)),
            //            Node::Constraint(c) => self.constraint(c),
            Node::CurrentOfExpr(c) => Ok(self.current_of_expr(c)),
            Node::DefElem(d) => self.def_elem(d),
            Node::FuncCall(f) => Ok(Box::new(self.func_call(f)?)),
            //            Node::GroupingSet(g) => self.grouping_set(g),
            Node::IndexElem(i) => self.index_elem(i),
            Node::Integer(i) => Ok(Box::new(Token::new_number(i.ival.to_string().as_str()))),
            //            Node::PartitionElem(p) => self.partition_elem(p),
            //            Node::PartitionRangeDatum(p) => self.partition_range_datum(p),
            Node::RangeVar(r) => Ok(self.range_var(r)),
            // Can we ever end up here from a SELECT or RETURNING clause? I
            // don't think so, since formatting for those two always calls
            // res_target() directly, rather than through node().
            Node::ResTarget(r) => self.res_target(r, false),
            Node::RowExpr(r) => self.row_expr(r),
            Node::SetToDefault(_) => Ok(Box::new(Token::new_keyword("DEFAULT"))),
            //            Node::SQLValueFunction(f) => Ok(self.sql_value_function(f)),

            // We explicitly _don't_ check for StringStruct. We need to handle
            // StringStruct locally in each method, as that's the only place
            // where we can determine whether a string is a string literal, an
            // operator, or a keyword.
            //
            //            Node::StringStruct(s) => Ok(Box::new(Token::new_string(s.str.as_str()))),
            Node::SubLink(s) => self.sub_link(s),
            //            Node::TypeCast(t) => self.type_cast(t),
            //            Node::WindowDef(w) => self.window_def(w, 0),
            _ => Err(TransformerError::UnexpectedNode {
                node: node.to_string(),
                func: "format_node",
            }),
        }
    }

    //#[trace]
    fn delete_stmt(&mut self, d: &DeleteStmt) -> Result<Statement> {
        let mut delete = Statement::new(self.indent_width, self.max_line_len);
        delete.push_keyword("DELETE");

        let mut from = ChunkList::new(Joiner::Space);
        from.push_keyword("FROM");

        let RangeVarWrapper::RangeVar(r) = &d.relation;
        from.push_chunk(self.range_var(r));

        delete.push_chunk(Box::new(from));

        if let Some(u) = &d.using_clause {
            delete.push_chunk(self.from_clause("USING", u)?);
        }
        if let Some(w) = &d.where_clause {
            delete.push_chunk(self.where_clause(w)?);
        }
        if let Some(r) = &d.returning_list {
            delete.push_chunk(self.returning_clause(r)?);
        }

        Ok(delete)
    }

    //#[trace]
    fn insert_stmt(&mut self, i: &InsertStmt) -> Result<Statement> {
        let mut insert = Statement::new(self.indent_width, self.max_line_len);

        let mut insert_table_clause = ChunkList::new(Joiner::Space);
        insert_table_clause.push_keyword("INSERT INTO");

        let RangeVarWrapper::RangeVar(r) = &i.relation;
        insert_table_clause.push_chunk(self.range_var(r));

        if let Some(cols) = &i.cols {
            let mut cols_clause = DelimitedExpression::new(Delimiter::Paren, Joiner::Comma, false);
            for ResTargetWrapper::ResTarget(c) in cols {
                cols_clause.push_args_chunk(self.res_target(c, false)?);
            }
            insert_table_clause.push_chunk(Box::new(cols_clause));
        }

        insert.push_chunk(Box::new(insert_table_clause));

        if let Some(o) = &i.r#override {
            let mut overriding_clause = Tokens::new(Joiner::None);
            match o {
                OverridingKind::OverridingSystemValue => {
                    overriding_clause.push_keyword("OVERRIDING SYSTEM VALUE");
                    insert.push_chunk(Box::new(overriding_clause));
                }
                OverridingKind::OverridingUserValue => {
                    overriding_clause.push_keyword("OVERRIDING USER VALUE");
                    insert.push_chunk(Box::new(overriding_clause));
                }
                OverridingKind::OverridingNotSet => (),
            }
        }

        insert.push_chunk(self.insert_values(i)?);

        if let Some(OnConflictClauseWrapper::OnConflictClause(occ)) = &i.on_conflict_clause {
            insert.push_chunk(self.on_conflict_clause(occ)?);
        }

        if let Some(r) = &i.returning_list {
            insert.push_chunk(self.returning_clause(r)?);
        }

        Ok(insert)
    }

    //#[trace]
    fn select_stmt(&mut self, s: &SelectStmt) -> Result<Statement> {
        if let Some(op) = self.select_op(&s.op) {
            // XXX - this is so gross. I think the unstable box matching
            // syntax would make this much less gross.
            let left = match &s.larg {
                Some(l) => {
                    let SelectStmtWrapper::SelectStmt(l) = &**l;
                    self.select_stmt(l)?
                }
                None => return Err(TransformerError::MissingSideForOp { side: "left", op }),
            };
            let right = match &s.rarg {
                Some(r) => {
                    let SelectStmtWrapper::SelectStmt(r) = &**r;
                    self.select_stmt(r)?
                }
                None => return Err(TransformerError::MissingSideForOp { side: "left", op }),
            };

            let mut select_with_op = Statement::new(self.indent_width, self.max_line_len);
            select_with_op.push_chunk(Box::new(left));
            select_with_op.push_keyword(&op);
            if s.all {
                select_with_op.push_keyword("ALL");
            }
            select_with_op.push_chunk(Box::new(right));

            return Ok(select_with_op);
        }

        let t = match &s.target_list {
            Some(tl) => tl,
            None => return Err(TransformerError::NoTargetListForSelect),
        };

        let mut select = Statement::new(self.indent_width, self.max_line_len);
        select.push_chunk(self.select_clause(t, s.distinct_clause.as_ref())?);
        if let Some(f) = &s.from_clause {
            select.push_chunk(self.from_clause("FROM", f)?);
        }
        if let Some(w) = &s.where_clause {
            select.push_chunk(self.where_clause(w)?);
        }
        if let Some(g) = &s.group_clause {
            select.push_chunk(self.group_by_clause(g)?);
        }
        if let Some(h) = &s.having_clause {
            select.push_chunk(self.having_clause(h)?);
        }
        if let Some(w) = &s.window_clause {
            select.push_chunk(self.window_clause(w)?);
        }
        if let Some(o) = &s.sort_clause {
            select.push_chunk(self.order_by_clause(o)?);
        }
        if let Some(l) = self.maybe_limit(s)? {
            select.push_chunk(l);
        }
        if let Some(l) = &s.locking_clause {
            select.push_chunk(self.locking_clause(l));
        }

        Ok(select)
    }

    //#[trace]
    fn update_stmt(&mut self, u: &UpdateStmt) -> Result<Statement> {
        let mut update = Statement::new(self.indent_width, self.max_line_len);

        let mut update_clause = ChunkList::new(Joiner::Space);
        update_clause.push_keyword("UPDATE");

        let RangeVarWrapper::RangeVar(r) = &u.relation;
        update_clause.push_chunk(self.range_var(r));

        update.push_chunk(Box::new(update_clause));

        update.push_chunk(self.res_targets_for_update(&u.target_list)?);

        if let Some(f) = &u.from_clause {
            update.push_chunk(self.from_clause("FROM", f)?);
        }
        if let Some(w) = &u.where_clause {
            update.push_chunk(self.where_clause(w)?);
        }
        if let Some(r) = &u.returning_list {
            update.push_chunk(self.returning_clause(r)?);
        }

        Ok(update)
    }

    // //#[trace]
    // fn format_create_table_stmt(&mut self, c: &CreateStmt) -> R {
    //     let RangeVarWrapper::RangeVar(rel) = &c.relation;

    //     let mut create = self.indent_str("CREATE");
    //     if let Some(per) = rel.persistence() {
    //         create.push(' ');
    //         create.push_str(per);
    //     }
    //     create.push_str(" TABLE ");

    //     if c.if_not_exists {
    //         create.push_str("IF NOT EXISTS ");
    //     }

    //     create.push_str(&self.format_range_var(rel));

    //     if let Some(i) = &c.inh_relations {
    //         create.push_str("\nPARTITION OF ");
    //         // This is a Vec<Node> but if we look at gram.y we see it's always
    //         // one element.
    //         create.push_str(&self.format_node(&i[0])?);
    //     }

    //     if let Some(elts) = &c.table_elts {
    //         create.push_str(&self.format_create_table_elements(elts)?);
    //     }

    //     create.push('\n');

    //     if let Some(opts) = &c.options {
    //         create.push_str(&format!("WITH {}\n", self.parenthesized_list(opts)?));
    //     }

    //     if let Some(t) = &c.tablespacename {
    //         create.push_str("TABLESPACE ");
    //         create.push_str(&t);
    //         create.push('\n');
    //     }

    //     if let Some(PartitionBoundSpecWrapper::PartitionBoundSpec(p)) = &c.partbound {
    //         if p.strategy == 'r' {
    //             create.push_str(&format!(
    //                 "FOR VALUES FROM {} TO {}\n",
    //                 self.parenthesized_list(p.lowerdatums.as_ref().unwrap())?,
    //                 self.parenthesized_list(p.upperdatums.as_ref().unwrap())?,
    //             ));
    //         } else {
    //             // The postgres gram.y seems to indicate that these values
    //             // could include boolean literals, but a test case with `FOR
    //             // VALUES IN (true, false)` fails to parse, so apparently it's
    //             // just strings, numbers, and NULL?
    //             create.push_str(&format!(
    //                 "FOR VALUES IN {}\n",
    //                 self.parenthesized_list(p.listdatums.as_ref().unwrap())?,
    //             ));
    //         }
    //     }

    //     if let Some(PartitionSpecWrapper::PartitionSpec(p)) = &c.partspec {
    //         create.push_str(&format!(
    //             "PARTITION BY {} {}\n",
    //             p.strategy.to_uppercase(),
    //             self.parenthesized_list(&p.part_params)?,
    //         ));
    //     }

    //     Ok(create)
    // }

    // fn format_create_table_elements(&mut self, elts: &[CreateStmtElement]) -> R {
    //     let mut create = String::new();

    //     let mut cols: Vec<&ColumnDef> = vec![];
    //     let mut constraints: Vec<&Constraint> = vec![];
    //     let mut likes: Vec<&TableLikeClause> = vec![];

    //     for e in elts {
    //         match e {
    //             CreateStmtElement::ColumnDef(e) => cols.push(e),
    //             CreateStmtElement::Constraint(e) => constraints.push(e),
    //             CreateStmtElement::TableLikeClause(e) => likes.push(e),
    //         }
    //     }

    //     create.push_str(" (\n");

    //     self.push_indent_one_level();

    //     create.push_str(&self.format_column_defs_for_create(&cols)?);
    //     // Is there a less gross way to do this?
    //     if !cols.is_empty() && !constraints.is_empty() {
    //         create.pop();
    //         create.push_str(",\n");
    //     }
    //     create.push_str(&self.format_constraints_for_create(&constraints)?);
    //     if (!cols.is_empty() || !constraints.is_empty()) && !likes.is_empty() {
    //         create.pop();
    //         create.push_str(",\n");
    //     }

    //     self.pop_indent();

    //     create.push_str(&self.indent_str(")"));

    //     Ok(create)
    // }

    // fn format_column_defs_for_create(&mut self, cols: &[&ColumnDef]) -> R {
    //     if cols.is_empty() {
    //         return Ok(String::new());
    //     }

    //     let mut create = String::new();

    //     // If this ends up being 0 that will not work well.
    //     let max_name_width = cols
    //         .iter()
    //         .map(|c| c.colname.len())
    //         .max()
    //         .ok_or(TransformerError::CreateStmtWithZeroLengthColumnsVec)?;

    //     for (i, cd) in cols.iter().enumerate() {
    //         let f = self.format_column_def(cd, max_name_width, true)?;
    //         create.push_str(&self.indent_str(&f));
    //         if let Some(attrs) = self.column_attributes(&cd)? {
    //             // Columns in partition tables may not have a type.
    //             if cd.type_name.is_some() {
    //                 create.push(' ');
    //             }
    //             create.push_str(&attrs.join(" "));
    //         }
    //         if i < cols.len() - 1 {
    //             create.push(',');
    //         }
    //         create.push('\n');
    //     }

    //     Ok(create)
    // }

    // fn column_attributes(&mut self, c: &ColumnDef) -> Result<Option<Vec<String>>, TransformerError> {
    //     let mut attrs: Vec<String> = vec![];

    //     if let Some(cons) = &c.constraints {
    //         attrs.append(
    //             &mut cons
    //                 .iter()
    //                 .map(|con| self.format_node(con))
    //                 .collect::<Result<Vec<_>, _>>()?,
    //         );
    //     }

    //     if let Some(d) = &c.cooked_default {
    //         attrs.push(format!("DEFAULT {}", &self.format_node(d)?));
    //     }

    //     // There are a number of attributes of the ColumnDef that will never
    //     // be populated, like is_not_null, identity, and others. I'm guessing
    //     // that at some point in the past, these were set when parsing, but
    //     // modern Postgres now treats these all as constraints.

    //     if attrs.is_empty() {
    //         return Ok(None);
    //     }

    //     Ok(Some(attrs))
    // }

    // fn format_constraints_for_create(&mut self, cons: &[&Constraint]) -> R {
    //     if cons.is_empty() {
    //         return Ok(String::new());
    //     }

    //     let formatted = cons
    //         .iter()
    //         .map(|c| self.format_constraint(c))
    //         .collect::<Result<Vec<_>, _>>()?;
    //     let mut create = formatted
    //         .iter()
    //         .map(|f| self.indent_str(f))
    //         .collect::<Vec<String>>()
    //         .join(",\n");
    //     create.push('\n');

    //     Ok(create)
    // }

    //#[trace]
    fn index_stmt(&mut self, i: &IndexStmt) -> Result<Statement> {
        let mut index = Statement::new(self.indent_width, self.max_line_len);

        let mut create = Tokens::new(Joiner::Space);
        create.push_keyword("CREATE");
        if i.unique {
            create.push_keyword("UNIQUE");
        }
        create.push_keyword("INDEX");

        if i.concurrent {
            create.push_keyword("CONCURRENTLY");
        }

        if i.if_not_exists {
            create.push_keyword("IF NOT EXISTS");
        }

        if let Some(n) = &i.idxname {
            create.push_identifier(n);
        }

        index.push_chunk(Box::new(create));

        let mut on = ChunkList::new(Joiner::Space);
        on.push_keyword("ON");

        let RangeVarWrapper::RangeVar(r) = &i.relation;
        on.push_chunk(self.range_var(r));

        index.push_chunk(Box::new(on));

        let mut using_and_elems = ChunkList::new(Joiner::Space);
        if let Some(a) = &i.access_method {
            // If there was no access method set in the original statement,
            // this will be populated. I don't think there's a way to know if
            // someone put an explicit "USING btree" in the statement.
            if a != "btree" {
                let mut using = Tokens::new(Joiner::Space);
                using.push_keyword("USING");
                using.push_identifier(a);

                using_and_elems.push_chunk(Box::new(using));
            }
        }

        let mut elems = DelimitedExpression::new(Delimiter::Paren, Joiner::Comma, false);
        for IndexElemWrapper::IndexElem(p) in &i.index_params {
            elems.push_args_chunk(self.index_elem(p)?);
        }
        using_and_elems.push_chunk(Box::new(elems));
        index.push_chunk(Box::new(using_and_elems));

        if let Some(opts) = &i.options {
            let mut with = ChunkList::new(Joiner::Space);
            with.push_keyword("WITH");
            let mut with_opts = DelimitedExpression::new(Delimiter::Paren, Joiner::Comma, false);
            for DefElemWrapper::DefElem(d) in opts {
                with_opts.push_args_chunk(self.def_elem(d)?);
            }
            with.push_chunk(Box::new(with_opts));
            index.push_chunk(Box::new(with));
        }

        if let Some(t) = &i.table_space {
            index.push_chunk(Box::new(Token::new_keyword("TABLESPACE")));
            index.push_chunk(Box::new(Token::new_identifier(t)));
        }

        Ok(index)
    }

    //#[trace]
    fn select_clause(
        &mut self,
        tl: &[Node],
        d: Option<&Vec<Option<Node>>>,
    ) -> Result<Box<dyn Chunk>> {
        let mut select_clause = ChunkList::new(Joiner::Space);
        select_clause.push_keyword("SELECT");

        if let Some(distinct) = d {
            if distinct.len() == 1 && distinct[0].is_none() {
                select_clause.push_keyword("DISTINCT");
            } else {
                select_clause.push_keyword("DISTINCT ON");
                for d in distinct {
                    match d {
                        Some(d) => select_clause.push_chunk(self.node(d)?),
                        None => panic!("multi-element distinct clause contains a null!"),
                    };
                }
            }
        }

        let mut targets = ChunkList::new(Joiner::Comma);
        for t in tl {
            targets.push_chunk(self.target_element(t)?);
        }
        select_clause.push_chunk(Box::new(targets));

        Ok(Box::new(select_clause))
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
    fn target_element(&mut self, t: &Node) -> Result<Box<dyn Chunk>> {
        match t {
            Node::ResTarget(rt) => self.res_target(rt, true),
            _ => Err(TransformerError::UnexpectedNode {
                node: t.to_string(),
                func: "format_target_element",
            }),
        }
    }

    //#[trace]
    fn res_target(&mut self, rt: &ResTarget, name_is_alias: bool) -> Result<Box<dyn Chunk>> {
        let mut res_target = ChunkList::new(Joiner::Space);
        // In a SELECT clause, .val contains the expression to select, .name
        // is the alias, and .indirection is empty. In an INSERT, .name is the
        // column name and .indirection contains any subscripts (like array
        // access), and .val is empty. In an UPDATE, .name and .indirection
        // are the same as INSERT, but .val is the value to set. A RETURNING
        // clause is just like a SELECT.
        //
        // In practice, this means that we can treat INSERT and UPDATE the
        // same, assuming that if .val is set, it's an assignment in an UPDATE
        // clause.
        if name_is_alias {
            match &rt.val {
                Some(v) => res_target.push_chunk(self.node(&*v)?),
                None => return Err(TransformerError::SelectResTargetWithoutVal),
            }
            if let Some(n) = &rt.name {
                res_target.push_keyword("AS");
                res_target.push_identifier(n);
            }
        } else {
            let has_name = rt.name.is_some();
            let has_val = rt.val.is_some();

            if has_name {
                res_target.push_chunk(self.name_and_maybe_indirection(
                    rt.name.as_ref().unwrap(),
                    rt.indirection.as_ref(),
                )?);
            }
            if has_name && has_val {
                res_target.push_operator("=");
            }
            if has_val {
                res_target.push_chunk(self.node(rt.val.as_ref().unwrap())?);
            }
        }

        Ok(Box::new(res_target))
    }

    //#[trace]
    fn name_and_maybe_indirection(
        &mut self,
        n: &str,
        i: Option<&Vec<IndirectionListElement>>,
    ) -> Result<Box<dyn Chunk>> {
        let mut name_ind = ChunkList::new(Joiner::Period);
        name_ind.push_identifier(n);
        if let Some(l) = i {
            name_ind.push_chunk(self.indirection_list(l)?);
        };

        Ok(Box::new(name_ind))
    }

    // fn a_indirection(&mut self, a: &AIndirection) -> Result<Box<dyn Chunk>> {
    //     let mut ir = Intermediate::new("", self.max_line_len);
    //     ir.push_chunk(Chunk::new_chunks(
    //         self.indirection_list(&a.indirection)?,
    //         "",
    //     ));
    //     let ind = ir.formatted()?;

    //     Ok(format!("{}{}", self.format_node(&*a.arg)?, ind))
    // }

    fn indirection_list(&mut self, ind: &[IndirectionListElement]) -> Result<Box<dyn Chunk>> {
        let mut list = ChunkList::new(Joiner::Period);
        for i in ind {
            list.push_chunk(self.indirection_element(i)?);
        }
        Ok(Box::new(list))
    }

    fn indirection_element(&mut self, i: &IndirectionListElement) -> Result<Box<dyn Chunk>> {
        match i {
            IndirectionListElement::AIndices(i) => self.a_indices(i),
            IndirectionListElement::AStar(_) => Ok(Box::new(Token::new_operator("*"))),
            IndirectionListElement::StringStruct(s) => {
                Ok(Box::new(Token::new_identifier(s.str.as_str())))
            }
        }
    }

    fn a_indices(&mut self, i: &AIndices) -> Result<Box<dyn Chunk>> {
        // Can this ever produce >1 Chunk in the Vec?
        let mut upper = ChunkList::new(Joiner::None);
        upper.push_chunk(self.node(&*i.uidx)?);
        if !i.is_slice {
            let mut indices = DelimitedExpression::new(Delimiter::Square, Joiner::None, false);
            indices.push_args_chunk(Box::new(upper));
            return Ok(Box::new(indices));
        }

        let mut indices = DelimitedExpression::new(Delimiter::Square, Joiner::None, false);

        if let Some(l) = &i.lidx {
            let mut lower = ChunkList::new(Joiner::None);
            lower.push_chunk(self.node(&*l)?);
            indices.push_args_chunk(Box::new(lower));
        }
        indices.push_args_chunk(Box::new(upper));
        Ok(Box::new(indices))
    }

    //#[trace]
    fn where_clause(&mut self, w: &Node) -> Result<Box<dyn Chunk>> {
        let mut wher = ChunkList::new(Joiner::Space);
        wher.push_keyword("WHERE");
        wher.push_chunk(self.node(w)?);
        Ok(Box::new(wher))
    }

    //#[trace]
    fn column_ref(&mut self, c: &ColumnRef) -> Box<dyn Chunk> {
        let mut cr = Tokens::new(Joiner::Period);
        for f in &c.fields {
            match f {
                ColumnRefField::StringStruct(s) => cr.push_identifier(&s.str),
                ColumnRefField::AStar(_) => cr.push_operator("*"),
            }
        }
        Box::new(cr)
    }

    //#[trace]
    fn current_of_expr(&self, c: &CurrentOfExpr) -> Box<dyn Chunk> {
        let mut current_of = ChunkList::new(Joiner::Space);
        current_of.push_keyword("CURRENT OF");
        current_of.push_identifier(&c.cursor_name);
        Box::new(current_of)
    }

    //#[trace]
    fn a_const(&mut self, a: &AConst) -> Box<dyn Chunk> {
        self.value(&a.val)
    }

    //#[trace]
    fn value(&mut self, v: &Value) -> Box<dyn Chunk> {
        let val = match &v {
            Value::StringStruct(s) => Token::new_string(s.str.as_str()),
            Value::BitString(s) => Token::new_string(s.str.as_str()),
            Value::Integer(i) => Token::new_number(i.ival.to_string().as_str()),
            Value::Float(f) => Token::new_number(f.str.as_str()),
            Value::Null(_) => Token::new_keyword("NULL"),
        };
        Box::new(val)
    }

    //#[trace]
    fn value_or_type_name(&mut self, v: &ValueOrTypeName) -> Result<Box<dyn Chunk>> {
        match &v {
            ValueOrTypeName::StringStruct(s) => Ok(Box::new(Token::new_string(s.str.as_str()))),
            ValueOrTypeName::BitString(s) => Ok(Box::new(Token::new_string(s.str.as_str()))),
            ValueOrTypeName::Integer(i) => {
                Ok(Box::new(Token::new_number(i.ival.to_string().as_str())))
            }
            ValueOrTypeName::Float(f) => Ok(Box::new(Token::new_number(f.str.as_str()))),
            ValueOrTypeName::Null(_) => Ok(Box::new(Token::new_keyword("NULL"))),
            ValueOrTypeName::TypeName(t) => self.type_name(t),
        }
    }

    //#[trace]
    fn a_expr(&mut self, a: &AExpr) -> Result<Box<dyn Chunk>> {
        let last_p = match self.contexts.last() {
            Some(ContextType::AExpr(p)) => *p,
            _ => 0,
        };
        let current_p = self.operator_precedence(&a.name);

        let mut transformer = new_context!(self, ContextType::AExpr(current_p));

        let mut op = ChunkList::new(Joiner::Space);
        match &a.kind {
            AExprKind::AExprParen => panic!("no idea how to handle AExprParen kind of AExpr"),
            AExprKind::AExprDistinct => op.push_keyword("IS DISTINCT FROM"),
            AExprKind::AExprNotDistinct => op.push_keyword("IS NOT DISTINCT FROM"),
            AExprKind::AExprNullif => {
                // For some reason NULLIF gets special handling as an a_expr
                // in the Pg grammar, even though AFAICT it's just a function.
                let mut name = Tokens::new(Joiner::None);
                name.push_keyword("NULLIF");
                let mut nullif = Func::new(name);
                nullif.push_args_chunk(transformer.node(&*a.lexpr)?);
                nullif.push_args_chunk(transformer.one_or_many_nodes(&a.rexpr)?);
                return Ok(Box::new(nullif));
            }
            AExprKind::AExprOf => {
                op.push_keyword(&format!("IS {}OF", transformer.maybe_not(&a.name)))
            }
            AExprKind::AExprIn => op.push_keyword(&format!("{}IN", transformer.maybe_not(&a.name))),
            k => {
                match &a.name {
                    Some(nodes) => {
                        let mut sub_op = Tokens::new(Joiner::Period);
                        for n in nodes {
                            // XXX - If we had a mix would that make sense? I
                            // don't think so.
                            match n {
                                Node::StringStruct(StringStruct { str: s }) => {
                                    sub_op.push_operator(s)
                                }
                                _ => op.push_chunk(transformer.node(n)?),
                            }
                        }
                        op.push_chunk(Box::new(sub_op));
                    }
                    None => return Err(TransformerError::AExprWithoutName),
                }
                match k {
                    AExprKind::AExprOpAny => op.push_keyword("ANY"),
                    AExprKind::AExprOpAll => op.push_keyword("ALL"),
                    _ => (),
                }
            }
        }

        transformer.infix_expr(&a.lexpr, Box::new(op), &a.rexpr, current_p, last_p)
    }

    // From https://www.postgresql.org/docs/13/sql-syntax-lexical.html
    fn operator_precedence(&self, name: &Option<List>) -> u8 {
        let op = if let Some(n) = name {
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

    fn maybe_not(&self, name: &Option<List>) -> &str {
        let op = if let Some(n) = name {
            if n.len() == 1 {
                if let Node::StringStruct(StringStruct { str: s }) = &n[0] {
                    s
                } else {
                    return "";
                }
            } else {
                return "";
            }
        } else {
            return "";
        };
        match op.as_str() {
            "=" => "",
            "<>" => "NOT ",
            _ => panic!("got an AExprOf AExpr with an invalid op name: {}", op),
        }
    }

    //#[trace]
    fn infix_expr(
        &mut self,
        left: &Node,
        op: Box<dyn Chunk>,
        right: &OneOrManyNodes,
        current_p: u8,
        last_p: u8,
    ) -> Result<Box<dyn Chunk>> {
        if last_p > current_p {
            let mut parens = DelimitedExpression::new(Delimiter::Paren, Joiner::Space, false);
            parens.push_args_chunk(self.node(left)?);
            parens.push_args_chunk(op);
            parens.push_args_chunk(self.one_or_many_nodes(right)?);
            return Ok(Box::new(parens));
        }

        let mut expr = ChunkList::new(Joiner::Space);
        expr.push_chunk(self.node(left)?);
        expr.push_chunk(op);
        expr.push_chunk(self.one_or_many_nodes(right)?);
        Ok(Box::new(expr))
    }

    fn one_or_many_nodes(&mut self, nodes: &OneOrManyNodes) -> Result<Box<dyn Chunk>> {
        match nodes {
            OneOrManyNodes::One(one) => self.node(&*one),
            // XXX - Is this right? The right side could be the right side of
            // something like "foo IN (1, 2)", but could it also be something
            // else that shouldn't be joined by commas?
            OneOrManyNodes::Many(many) => {
                let mut expr = DelimitedExpression::new(Delimiter::Paren, Joiner::Comma, false);
                for n in many {
                    expr.push_args_chunk(self.node(n)?);
                }
                Ok(Box::new(expr))
            }
        }
    }

    // If a bool expr is just a series of "AND" clauses, then we get one
    // BoolExpr with many args. If there is a mix of "AND" and "OR", then the
    // args will contain nested BoolExpr clauses. So if our depth is > 1 then
    // we need to add wrapping parens.
    //
    // If it's a NOT clause it should only have one child and we don't add
    // parens around it.
    //#[trace]
    fn bool_expr(&mut self, b: &BoolExpr) -> Result<Box<dyn Chunk>> {
        self.bool_expr_depth += 1;
        let bool_expr = self.bool_expr_container(b);
        self.bool_expr_depth -= 1;
        bool_expr
    }

    fn bool_expr_container(&mut self, b: &BoolExpr) -> Result<Box<dyn Chunk>> {
        let bool_op = match b.boolop {
            BoolExprType::AndExpr => "AND",
            BoolExprType::OrExpr => "OR",
            BoolExprType::NotExpr => "NOT",
        };

        // A NOT expression never needs extra parens and it has no right side.
        if bool_op == "NOT" {
            if b.args.len() != 1 {
                return Err(TransformerError::BoolExprWithWrongNumberOfArguments {
                    bool_op,
                    num: b.args.len(),
                });
            }

            let mut not = ChunkList::new(Joiner::Space);
            not.push_keyword("NOT");
            not.push_chunk(self.node(&b.args[0])?);
            return Ok(Box::new(not));
        }

        if b.args.len() < 2 {
            return Err(TransformerError::BoolExprWithWrongNumberOfArguments {
                bool_op,
                num: b.args.len(),
            });
        }

        let bool_clause = BoolOpChunk::new(
            Token::new_keyword(bool_op),
            b.args
                .iter()
                .map(|n| self.node(n))
                .collect::<Result<Vec<Box<dyn Chunk>>>>()?,
        );

        if self.bool_expr_depth > 1 {
            let mut parens = DelimitedExpression::new(Delimiter::Paren, Joiner::None, false);
            parens.push_args_chunk(Box::new(bool_clause));
            return Ok(Box::new(parens));
        }

        Ok(Box::new(bool_clause))
    }

    //#[trace]
    fn def_elem(&mut self, d: &DefElem) -> Result<Box<dyn Chunk>> {
        let mut def_elem = ChunkList::new(Joiner::Space);

        let defname = d.defname.to_uppercase();
        def_elem.push_chunk(Box::new(Token::new_keyword(&defname)));

        match defname.as_str() {
            "FASTUPDATE" | "FILLFACTOR" => {
                def_elem.push_chunk(Box::new(Token::new_operator("=")));
                def_elem.push_chunk(self.value_or_type_name(&d.arg)?);
                Ok(Box::new(def_elem))
            }
            r => panic!("unhandled defname: {}", r),
        }
    }

    // This returns a Func instead of Box<dyn Chunk> because sometimes the
    // caller will need to append more chunks to the Func.
    //
    //#[trace]
    fn func_call(&mut self, f: &FuncCall) -> Result<ChunkList> {
        // There are a number of special case "functions" like "thing AT TIME
        // ZONE ..." and "thing LIKE foo ESCAPE bar" that need to be handled
        // differently.

        let mut name = Tokens::new(Joiner::Period);
        for n in f.funcname.iter().filter_map(|n| match n {
            // We don't want to quote a string here.
            Node::StringStruct(s) => {
                // The parser will stick this in front of lots of func call
                // names so you get "pg_catalog.date_part", but no one wants
                // to see that (I think).
                if s.str.as_str() == "pg_catalog" {
                    return None;
                }
                Some(s.str.as_str())
            }
            _ => panic!("should never have a funcname element that is not a string"),
        }) {
            name.push_identifier(n);
        }

        let mut func = Func::new(name);
        // We'll ignore f.func_variadic since it's optional.
        if f.agg_star {
            func.push_args_chunk(Box::new(Token::new_operator("*")));
        } else {
            match &f.args {
                Some(args) => {
                    for (i, arg) in args.iter().enumerate() {
                        if i == 0 && f.agg_distinct {
                            func.push_args_prefix_chunk(Box::new(Token::new_keyword("DISTINCT")));
                        }
                        let mut a = ChunkList::new(Joiner::Space);
                        a.push_chunk(self.node(arg)?);

                        if let Some(ob) = &f.agg_order {
                            a.push_chunk(self.order_by_clause(ob)?);
                        }

                        func.push_args_chunk(Box::new(a));
                    }
                }
                None => (),
            }
        }

        let mut call = ChunkList::new(Joiner::Space);
        call.push_chunk(Box::new(func));

        if let Some(fil) = &f.agg_filter {
            let mut filter = ChunkList::new(Joiner::Space);
            filter.push_chunk(Box::new(Token::new_keyword("FILTER")));
            filter.push_chunk(self.node(fil)?);
            call.push_chunk(Box::new(filter));
        }

        // XXX need to handle f.agg_within_group

        if let Some(WindowDefWrapper::WindowDef(w)) = &f.over {
            let mut over = ChunkList::new(Joiner::Space);
            over.push_chunk(Box::new(Token::new_keyword("OVER")));
            over.push_chunk(self.window_def(w)?);
            call.push_chunk(Box::new(over));
        }

        Ok(call)
    }

    //#[trace]
    fn window_def(&mut self, w: &WindowDef) -> Result<Box<dyn Chunk>> {
        let mut window = ChunkList::new(Joiner::Space);
        window.push_keyword("AS");

        // XXX - should we look at the context to determine whether we should
        // allow a name or refname? It seems like name isn't allowed in SELECT
        // clause (?) and refname is not allowed in WINDOW clause (?).
        if let Some(n) = &w.name {
            window.push_identifier(n);
        }

        if let Some(n) = &w.refname {
            window.push_identifier(n);
        }
        if let Some(p) = &w.partition_clause {
            window.push_chunk(self.partition_clause(&p)?);
        }
        if let Some(ob) = &w.order_clause {
            window.push_chunk(self.order_by_clause(&ob)?);
        }
        if w.frame_options != FrameOptions::DEFAULTS {
            window.push_chunk(self.frame_options(w)?);
        }

        Ok(Box::new(window))
    }

    //#[trace]
    fn partition_clause(&mut self, partition: &[Node]) -> Result<Box<dyn Chunk>> {
        let mut part = ChunkList::new(Joiner::Space);
        part.push_keyword("PARTITION BY");
        for p in partition {
            part.push_chunk(self.node(p)?);
        }
        Ok(Box::new(part))
    }

    //#[trace]
    fn frame_options(&mut self, w: &WindowDef) -> Result<Box<dyn Chunk>> {
        let mut options = ChunkList::new(Joiner::Space);

        let flags = &w.frame_options;
        // XXX - I'm not sure if it's possible to have options where neither
        // of these are true.
        if flags.contains(FrameOptions::RANGE) {
            options.push_keyword("RANGE");
        } else if flags.contains(FrameOptions::ROWS) {
            options.push_keyword("ROWS");
        }
        if flags.contains(FrameOptions::BETWEEN) {
            options.push_keyword("BETWEEN");
        }

        if flags.contains(FrameOptions::START_UNBOUNDED_PRECEDING) {
            options.push_keyword("UNBOUNDED PRECEDING");
        }
        if flags.contains(FrameOptions::START_UNBOUNDED_FOLLOWING) {
            options.push_keyword("UNBOUNDED FOLLOWING");
        }
        if flags.contains(FrameOptions::START_CURRENT_ROW) {
            options.push_keyword("CURRENT ROW");
        }

        if flags.contains(FrameOptions::START_VALUE_PRECEDING)
            | flags.contains(FrameOptions::START_VALUE_FOLLOWING)
        {
            options.push_keyword("START");

            let direction = if flags.contains(FrameOptions::START_VALUE_PRECEDING) {
                "PRECEDING"
            } else {
                "FOLLOWING"
            };

            if let Some(o) = &w.start_offset {
                options.push_chunk(self.node(&*o)?);
            } else {
                return Err(TransformerError::FrameOptionsValueWithoutOffset {
                    opt: format!("START {}", direction),
                });
            }

            options.push_keyword(direction);
        }

        if flags.intersects(FrameOptions::START) && flags.intersects(FrameOptions::END) {
            options.push_keyword("AND");
        }

        if flags.contains(FrameOptions::END_UNBOUNDED_PRECEDING) {
            options.push_keyword("UNBOUNDED PRECEDING");
        }
        if flags.contains(FrameOptions::END_UNBOUNDED_FOLLOWING) {
            options.push_keyword("UNBOUNDED FOLLOWING");
        }
        if flags.contains(FrameOptions::END_CURRENT_ROW) {
            options.push_keyword("CURRENT ROW");
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
                options.push_chunk(self.node(&*o)?);
            } else {
                return Err(TransformerError::FrameOptionsValueWithoutOffset {
                    opt: format!("END {}", direction),
                });
            }

            options.push_keyword(direction);
        }

        Ok(Box::new(options))
    }

    //#[trace]
    fn maybe_limit(&mut self, s: &SelectStmt) -> Result<Option<Box<dyn Chunk>>> {
        let mut limit = ChunkList::new(Joiner::Space);
        if let Some(c) = &s.limit_count {
            limit.push_keyword("LIMIT");
            match **c {
                Node::AConst(AConst {
                    val: Value::Null(_),
                    ..
                }) => limit.push_keyword("ALL"),
                _ => limit.push_chunk(self.node(&*c)?),
            }
        }
        if let Some(o) = &s.limit_offset {
            limit.push_keyword("OFFSET ");
            limit.push_chunk(self.node(&*o)?);
        }

        match limit.is_empty() {
            true => Ok(None),
            false => Ok(Some(Box::new(limit))),
        }
    }

    //#[trace]
    fn sub_link(&mut self, s: &SubLink) -> Result<Box<dyn Chunk>> {
        let mut sub_link = ChunkList::new(Joiner::Space);
        if let Some(n) = &s.testexpr {
            sub_link.push_chunk(self.node(&*n)?);
        }
        if let Some(o) = self.sub_link_oper(s)? {
            sub_link.push_chunk(o);
        }

        let SelectStmtWrapper::SelectStmt(sub) = &s.subselect;
        sub_link.push_chunk(Box::new(SubStatement::new(self.select_stmt(sub)?, false)));

        Ok(Box::new(sub_link))
    }

    //#[trace]
    fn sub_link_oper(&mut self, s: &SubLink) -> Result<Option<Box<dyn Chunk>>> {
        match s.sub_link_type {
            SubLinkType::ExistsSublink => Ok(Some(Box::new(Token::new_keyword("EXISTS")))),
            SubLinkType::AllSublink => match &s.oper_name {
                Some(o) => {
                    let mut oper = self.sub_link_oper_name(&o)?;
                    oper.push_keyword("ALL");
                    Ok(Some(Box::new(oper)))
                }
                None => Err(TransformerError::SubLinkHasNoOperator {
                    sublink_type: "All",
                }),
            },
            SubLinkType::AnySublink => match &s.oper_name {
                None => Ok(Some(Box::new(Token::new_keyword("IN")))),
                Some(o) => {
                    let mut oper = self.sub_link_oper_name(&o)?;
                    oper.push_keyword("ANY");
                    Ok(Some(Box::new(oper)))
                }
            },
            SubLinkType::RowcompareSublink => match &s.oper_name {
                Some(o) => Ok(Some(Box::new(self.sub_link_oper_name(&o)?))),
                None => Err(TransformerError::SubLinkHasNoOperator {
                    sublink_type: "Rowcompare",
                }),
            },
            SubLinkType::ExprSublink => match &s.oper_name {
                // Is this possible?
                Some(o) => Ok(Some(Box::new(self.sub_link_oper_name(&o)?))),
                None => Ok(None),
            },
            // Looking at the Pg (10) source, I don't think the parse can
            // produce this type of sublink. Is it only for plans?
            SubLinkType::MultiexprSublink => {
                panic!("Found a MultiexprSublink, which is not possible according to the Pg 10 parser code")
            }
            SubLinkType::ArraySublink => Ok(Some(Box::new(Token::new_keyword("ARRAY")))),
            SubLinkType::CteSublink => {
                panic!(
                    "Found a CteSublink, which is not possible according to the Pg 10 parser code"
                )
            }
        }
    }

    fn sub_link_oper_name(&self, name: &[Node]) -> Result<ChunkList> {
        let mut oper_name = ChunkList::new(Joiner::Space);
        for n in name {
            match n {
                Node::StringStruct(s) => oper_name.push_keyword(&s.str),
                _ => {
                    return Err(TransformerError::UnexpectedNodeInList {
                        node: n.to_string(),
                        expected: "StringStruct",
                    })
                }
            }
        }
        Ok(oper_name)
    }

    //#[trace]
    fn row_expr(&mut self, r: &RowExpr) -> Result<Box<dyn Chunk>> {
        let mut row_expr = ChunkList::new(Joiner::Space);
        match r.row_format {
            CoercionForm::CoerceExplicitCall => row_expr.push_keyword("ROW"),
            CoercionForm::CoerceImplicitCast => (),
            CoercionForm::CoerceExplicitCast => {
                return Err(TransformerError::RowExprWithExplicitCast)
            }
        };

        let mut args = DelimitedExpression::new(Delimiter::Paren, Joiner::Comma, false);
        for a in &r.args {
            args.push_args_chunk(self.node(a)?);
        }

        row_expr.push_chunk(Box::new(args));

        Ok(Box::new(row_expr))
    }

    // //#[trace]
    // fn format_sql_value_function(&mut self, v: &SQLValueFunction) -> String {
    //     match v.op {
    //         SQLValueFunctionOp::SvfopCurrentCatalog => "current_catalog".into(),
    //         SQLValueFunctionOp::SvfopCurrentDate => "current_date".into(),
    //         SQLValueFunctionOp::SvfopCurrentRole => "current_role".into(),
    //         SQLValueFunctionOp::SvfopCurrentSchema => "current_schema".into(),
    //         SQLValueFunctionOp::SvfopCurrentTime => "current_time".into(),
    //         SQLValueFunctionOp::SvfopCurrentTimeN => format!("current_time({})", v.typmod.unwrap()),
    //         SQLValueFunctionOp::SvfopCurrentTimestamp => "current_timestamp".into(),
    //         SQLValueFunctionOp::SvfopCurrentTimestampN => {
    //             format!("current_timestamp({})", v.typmod.unwrap())
    //         }
    //         SQLValueFunctionOp::SvfopCurrentUser => "current_user".into(),
    //         SQLValueFunctionOp::SvfopLocaltime => "localtime".into(),
    //         SQLValueFunctionOp::SvfopLocaltimeN => format!("localtime({})", v.typmod.unwrap()),
    //         SQLValueFunctionOp::SvfopLocaltimestamp => "localtimestamp".into(),
    //         SQLValueFunctionOp::SvfopLocaltimestampN => {
    //             format!("localtimestamp({})", v.typmod.unwrap())
    //         }
    //         SQLValueFunctionOp::SvfopSessionUser => "session_user".into(),
    //         SQLValueFunctionOp::SvfopUser => "user".into(),
    //     }
    // }

    // //#[trace]
    // fn format_grouping_set(&mut self, gs: &GroupingSet) -> R {
    //     let is_nested = self.is_in_context(ContextType::GroupingSet);

    //     let mut formatter = new_context!(self, ContextType::GroupingSet);

    //     if let GroupingSetKind::GroupingSetEmpty = gs.kind {
    //         return Ok(String::new());
    //     }

    //     let members = gs
    //         .content
    //         .as_ref()
    //         .expect("we should always have content unless the kind if GroupingSetEmpty!");

    //     let (grouping_set, needs_parens) = match gs.kind {
    //         GroupingSetKind::GroupingSetEmpty => {
    //             panic!("we already matched GroupingSetEmpty, wtf!")
    //         }
    //         GroupingSetKind::GroupingSetSimple => (String::new(), false),
    //         GroupingSetKind::GroupingSetRollup => ("ROLLUP ".to_string(), false),
    //         GroupingSetKind::GroupingSetCube => ("CUBE ".to_string(), false),
    //         GroupingSetKind::GroupingSetSets => {
    //             if is_nested {
    //                 (String::new(), true)
    //             } else {
    //                 // If we have an unnested set with one member there's no
    //                 // need for additional parens.
    //                 ("GROUPING SETS ".to_string(), members.len() > 1)
    //             }
    //         }
    //     };

    //     let maker = |f: &mut Self| -> Result<Vec<String>, TransformerError> {
    //         Ok(f.formatted_list(&members)?
    //             .into_iter()
    //             .map(|g| {
    //                 // If the element is a RowExpr it will already have
    //                 // wrapping parens.
    //                 if needs_parens && !(g.starts_with('(') && g.ends_with(')')) {
    //                     format!("({})", g)
    //                 } else {
    //                     g
    //                 }
    //             })
    //             .collect::<Vec<String>>())
    //     };

    //     formatter.one_line_or_many(&grouping_set, false, true, true, 0, maker)
    // }

    //#[trace]
    fn from_clause(&mut self, keyword: &str, fc: &[FromClauseElement]) -> Result<Box<dyn Chunk>> {
        let mut from = ChunkList::new(Joiner::Space);
        from.push_keyword(keyword);

        let mut from_elements = ChunkList::new(Joiner::Comma);
        for (n, f) in fc.iter().enumerate() {
            from_elements.push_chunk(self.from_element(&f, n == 0)?);
        }

        from.push_chunk(Box::new(from_elements));

        Ok(Box::new(from))
    }

    //#[trace]
    fn from_element(
        &mut self,
        f: &FromClauseElement,
        is_first_from_element: bool,
    ) -> Result<Box<dyn Chunk>> {
        match f {
            FromClauseElement::JoinExpr(j) => self.join_expr(&j, is_first_from_element),
            FromClauseElement::RangeVar(r) => Ok(self.range_var(&r)),
            FromClauseElement::RangeSubselect(s) => self.subselect(&s),
            FromClauseElement::RangeFunction(f) => self.range_function(&f),
            FromClauseElement::RangeTableSample(s) => self.range_table_sample(&s),
        }
    }

    //#[trace]
    fn join_expr(&mut self, j: &JoinExpr, is_first_from_element: bool) -> Result<Box<dyn Chunk>> {
        if j.is_natural {
            if j.quals.is_some() {
                return Err(TransformerError::CannotMixJoinStrategies {
                    strat1: "NATURAL",
                    strat2: "ON",
                });
            }
            if j.using_clause.is_some() {
                return Err(TransformerError::CannotMixJoinStrategies {
                    strat1: "NATURAL",
                    strat2: "USING",
                });
            }
        }

        if j.quals.is_some() && j.using_clause.is_some() {
            return Err(TransformerError::CannotMixJoinStrategies {
                strat1: "ON",
                strat2: "USING",
            });
        }

        let mut current = j;

        let mut join_clause = JoinClause::new(is_first_from_element);
        // The right arg for the _first_ join expr is actually the last join
        // RHS in the parsed SQL. The deepest left arg in the tree is the
        // first LHS in the parsed SQL, so we push elements to the front of
        // the list as we descend the tree.
        while let Some(l) = self.left_is_join(current) {
            join_clause.push_front_join(
                self.join_element(&*l.rarg)?,
                self.join_type(&l)?,
                self.join_condition(l)?,
            );
            current = l;
        }
        join_clause.set_first(self.join_element(&current.larg)?);
        join_clause.push_back_join(
            self.join_element(&*j.rarg)?,
            self.join_type(&j)?,
            self.join_condition(j)?,
        );

        Ok(Box::new(join_clause))
    }

    fn left_is_join<'a>(&self, j: &'a JoinExpr) -> Option<&'a JoinExpr> {
        match &*j.larg {
            Node::JoinExpr(l) => Some(l),
            _ => None,
        }
    }

    //#[trace]
    fn join_element(&mut self, f: &Node) -> Result<Box<dyn Chunk>> {
        match f {
            Node::RangeFunction(f) => self.range_function(&f),
            Node::RangeSubselect(s) => self.subselect(&s),
            Node::RangeTableSample(s) => self.range_table_sample(&s),
            Node::RangeVar(r) => Ok(self.range_var(&r)),
            _ => Err(TransformerError::UnexpectedNode {
                node: f.to_string(),
                func: "join_element",
            }),
        }
    }

    fn join_condition(&mut self, j: &JoinExpr) -> Result<Option<JoinCondition>> {
        if let Some(q) = &j.quals {
            return Ok(Some(JoinCondition::On(self.node(&*q)?)));
        }
        if let Some(u) = &j.using_clause {
            let mut using = DelimitedExpression::new(Delimiter::Paren, Joiner::Comma, false);
            for StringStructWrapper::StringStruct(s) in u {
                using.push_args_chunk(Box::new(Token::new_identifier(&s.str)));
            }
            return Ok(Some(JoinCondition::Using(using)));
        }

        Ok(None)
    }

    fn join_type(&self, j: &JoinExpr) -> Result<Token> {
        let jt = match &j.jointype {
            JoinType::JoinInner => {
                if j.quals.is_some() || j.using_clause.is_some() || j.is_natural {
                    "JOIN"
                } else {
                    "CROSS JOIN"
                }
            }
            JoinType::JoinLeft => "LEFT OUTER JOIN",
            JoinType::JoinRight => "RIGHT OUTER JOIN",
            JoinType::JoinFull => "FULL OUTER JOIN",
            _ => {
                return Err(TransformerError::InexpressibleJoinType {
                    jt: j.jointype.to_string(),
                })
            }
        };

        if j.is_natural {
            return Ok(Token::new_keyword(format!("NATURAL {}", jt)));
        }

        Ok(Token::new_keyword(jt))
    }

    // //#[trace]
    // fn format_partition_elem(&mut self, p: &PartitionElem) -> R {
    //     if let Some(n) = &p.name {
    //         Ok(n.clone())
    //     } else if let Some(e) = &p.expr {
    //         self.format_node(&*e)
    //     } else {
    //         Err(TransformerError::PartitionElemWithoutNameOrExpression)
    //     }
    // }

    // fn format_partition_range_datum(&mut self, p: &PartitionRangeDatum) -> R {
    //     match &p.kind {
    //         PartitionRangeDatumKind::PartitionRangeDatumMinvalue => Ok("MINVALUE".into()),
    //         PartitionRangeDatumKind::PartitionRangeDatumMaxvalue => Ok("MAXVALUE".into()),
    //         PartitionRangeDatumKind::PartitionRangeDatumValue => {
    //             self.format_node(&*p.value.as_ref().unwrap())
    //         }
    //     }
    // }

    //#[trace]
    fn range_var(&self, r: &RangeVar) -> Box<dyn Chunk> {
        let mut names = Tokens::new(Joiner::Period);
        if let Some(c) = &r.catalogname {
            names.push_identifier(c);
        }
        if let Some(s) = &r.schemaname {
            names.push_identifier(s);
        }
        names.push_identifier(&r.relname);

        let mut rv = ChunkList::new(Joiner::Space);
        rv.push_chunk(Box::new(names));
        if let Some(AliasWrapper::Alias(a)) = &r.alias {
            let mut alias = Tokens::new(Joiner::Space);
            alias.push_keyword("AS");
            alias.push_identifier(&a.aliasname);

            rv.push_chunk(Box::new(alias));
            // XXX - do something with colnames here?
        }

        Box::new(rv)
    }

    //#[trace]
    fn group_by_clause(&mut self, g: &[Node]) -> Result<Box<dyn Chunk>> {
        let mut group_by = ChunkList::new(Joiner::Space);
        group_by.push_keyword("GROUP BY");
        for n in g {
            group_by.push_chunk(self.node(n)?);
        }
        Ok(Box::new(group_by))
    }

    //#[trace]
    fn having_clause(&mut self, h: &Node) -> Result<Box<dyn Chunk>> {
        let mut having = ChunkList::new(Joiner::Space);
        having.push_keyword("HAVING");
        having.push_chunk(self.node(h)?);
        Ok(Box::new(having))
    }

    //#[trace]
    fn window_clause(&mut self, g: &[Node]) -> Result<Box<dyn Chunk>> {
        let mut window = ChunkList::new(Joiner::Space);
        window.push_keyword("WINDOW");
        for n in g {
            window.push_chunk(self.node(n)?);
        }
        Ok(Box::new(window))
    }

    //#[trace]
    fn locking_clause(&mut self, l: &[LockingClauseWrapper]) -> Box<dyn Chunk> {
        let mut locking_clause = ChunkList::new(Joiner::Space);

        for LockingClauseWrapper::LockingClause(c) in l {
            let mut one_clause = ChunkList::new(Joiner::Space);
            one_clause.push_keyword("FOR");

            match c.strength {
                LockClauseStrength::LcsNone => panic!("got locking strength of none!"),
                LockClauseStrength::LcsForkeyshare => one_clause.push_keyword("KEY SHARE"),
                LockClauseStrength::LcsForshare => one_clause.push_keyword("SHARE"),
                LockClauseStrength::LcsFornokeyupdate => one_clause.push_keyword("NO KEY UPDATE"),
                LockClauseStrength::LcsForupdate => one_clause.push_keyword("UPDATE"),
            };

            if let Some(lr) = &c.locked_rels {
                one_clause.push_keyword("OF");
                for RangeVarWrapper::RangeVar(r) in lr {
                    one_clause.push_chunk(self.range_var(r));
                }
            }

            if let Some(w) = &c.wait_policy {
                match w {
                    LockWaitPolicy::Lockwaitblock => (),
                    LockWaitPolicy::Lockwaitskip => one_clause.push_keyword("SKIP LOCKED"),
                    LockWaitPolicy::Lockwaiterror => one_clause.push_keyword("NOWAIT"),
                }
            }

            locking_clause.push_chunk(Box::new(one_clause));
        }

        Box::new(locking_clause)
    }

    //#[trace]
    fn order_by_clause(&mut self, o: &[SortByWrapper]) -> Result<Box<dyn Chunk>> {
        let mut order_by = ChunkList::new(Joiner::Space);
        order_by.push_chunk(Box::new(Token::new_keyword("ORDER BY")));

        let mut elts = ChunkList::new(Joiner::Comma);
        for SortByWrapper::SortBy(s) in o {
            let mut el = ChunkList::new(Joiner::Space);
            el.push_chunk(self.node(&*s.node)?);
            match s.sortby_dir {
                SortByDir::SortbyDefault => (),
                SortByDir::SortbyAsc => el.push_chunk(Box::new(Token::new_keyword("ASC"))),
                SortByDir::SortbyDesc => el.push_chunk(Box::new(Token::new_keyword("DESC"))),
                SortByDir::SortbyUsing => {
                    el.push_chunk(Box::new(Token::new_keyword("USING")));
                    match &s.use_op {
                        Some(using) => {
                            for u in using {
                                el.push_chunk(self.node(u)?);
                            }
                        }
                        None => return Err(TransformerError::OrderByUsingWithoutOp),
                    }
                }
            };
            elts.push_chunk(Box::new(el));
            if let Some(sb) = self.sort_by_nulls_ordering(&s.sortby_nulls) {
                elts.push_chunk(Box::new(sb));
            }
        }
        order_by.push_chunk(Box::new(elts));

        Ok(Box::new(order_by))
    }

    fn sort_by_nulls_ordering(&self, o: &SortByNulls) -> Option<Token> {
        match o {
            SortByNulls::SortbyNullsDefault => None,
            SortByNulls::SortbyNullsFirst => Some(Token::new_keyword("NULLS FIRST")),
            SortByNulls::SortbyNullsLast => Some(Token::new_keyword("NULLS LAST")),
        }
    }

    // //#[trace]
    // fn format_type_cast(&mut self, tc: &TypeCast) -> R {
    //     let mut type_cast = self.format_node(&*tc.arg)?;
    //     type_cast.push_str("::");
    //     let TypeNameWrapper::TypeName(tn) = &tc.type_name;
    //     type_cast.push_str(&self.format_type_name(&tn)?);

    //     // This is some oddity of the parser. It turns TRUE and FALSE literals
    //     // into this cast expression.
    //     if type_cast == "'t'::bool" {
    //         return Ok("TRUE".to_string());
    //     } else if type_cast == "'f'::bool" {
    //         return Ok("FALSE".to_string());
    //     }

    //     Ok(type_cast)
    // }

    //#[trace]
    fn type_name(&mut self, tn: &TypeName) -> Result<Box<dyn Chunk>> {
        let mut type_name = ChunkList::new(Joiner::Space);
        if tn.setof {
            type_name.push_keyword("SET OF");
        }

        let n = match &tn.names {
            Some(names) => {
                let mut joined = Tokens::new(Joiner::Period);
                let renamed = names
                    .iter()
                    // Is this clone necessary? It feels like there should be a
                    // way to work with the original reference until the join.
                    .map(|StringStructWrapper::StringStruct(n)| {
                        self.type_renaming.get(&n.str).unwrap_or(&n.str).clone()
                    })
                    .filter(|n| n != "pg_catalog");
                for r in renamed {
                    joined.push_identifier(r);
                }
                joined
            }
            None => panic!("not sure how to handle a nameless type"),
        };
        let last_token = n.last_token().unwrap();
        type_name.push_chunk(Box::new(n));

        if let Some(m) = &tn.typemod {
            // A -1 means it has no modifier.
            if *m != -1 {
                let mut typemod = DelimitedExpression::new(Delimiter::Paren, Joiner::None, false);
                typemod.push_args_chunk(Box::new(Token::new_number(m.to_string().as_ref())));
            }
        }
        if let Some(m) = &tn.typmods {
            // XXX - I think interval may also have a special case for two?
            if last_token.to_uppercase().eq("INTERVAL") {
                if m.len() == 1 {
                    // If the type is INTERVAL and we have one node, then this
                    // is an integer constant. That constant is actually an
                    // INTERVAL_MASK flag representing an interval precision.
                    let mask = match &m[0] {
                        Node::AConst(AConst {
                            val: Value::Integer(Integer { ival: i }),
                            location: _,
                        }) => IntervalMask::new_from_i64(*i)?.type_modifiers()?,
                        _ => panic!("argh"),
                    };
                    let mut mods = ChunkList::new(Joiner::Space);
                    mods.push_keyword(mask[0]);
                    if mask.len() > 1 {
                        mods.push_keyword("TO");
                        mods.push_keyword(mask[1]);
                    }
                    type_name.push_chunk(Box::new(mods));
                } else {
                    panic!("need to handle interval typmods.len > 1");
                }
            } else {
                let mut mods = DelimitedExpression::new(Delimiter::Paren, Joiner::Comma, false);
                mods.push_args_chunk(self.node(&m[0])?);
                type_name.push_chunk(Box::new(mods));
            }
        }

        if let Some(ab) = &tn.array_bounds {
            // XXX - is ", " the right joiner here? Can this ever have
            // multiple elements inside.
            let mut bounds = DelimitedExpression::new(Delimiter::Square, Joiner::Comma, false);
            for e in ab {
                match e {
                    // If the element is -1 then this is an unbounded array,
                    // so there is no inside to append.
                    Node::Integer(Integer { ival: -1 }) => (),
                    _ => bounds.push_args_chunk(self.node(e)?),
                }
            }
            type_name.push_chunk(Box::new(bounds));
        }

        Ok(Box::new(type_name))
    }

    //#[trace]
    fn subselect(&mut self, s: &RangeSubselect) -> Result<Box<dyn Chunk>> {
        let mut subselect = ChunkList::new(Joiner::Space);
        if s.lateral {
            subselect.push_keyword("LATERAL");
        }

        let SelectStmtWrapper::SelectStmt(stmt) = &*s.subquery;
        let select = SubStatement::new(self.select_stmt(&stmt)?, false);
        subselect.push_chunk(Box::new(select));

        if let Some(AliasWrapper::Alias(a)) = &s.alias {
            subselect.push_keyword("AS");
            subselect.push_identifier(&a.aliasname);
        }

        Ok(Box::new(subselect))
    }

    //#[trace]
    fn range_function(&mut self, rf: &RangeFunction) -> Result<Box<dyn Chunk>> {
        if rf.functions.is_empty() {
            return Err(TransformerError::RangeFunctionDoesNotHaveAnyFunctions);
        }

        let mut range_func = ChunkList::new(Joiner::Space);
        if rf.lateral {
            range_func.push_keyword("LATERAL");
        }

        let elts = self.range_func_elements(&rf.functions)?;
        let elements: Box<dyn Chunk> = if elts.len() == 1 {
            let mut container = ChunkList::new(Joiner::Space);
            for e in elts {
                container.push_chunk(e);
            }
            Box::new(container)
        } else {
            range_func.push_keyword("ROWS FROM");
            let mut container = DelimitedExpression::new(Delimiter::Paren, Joiner::Comma, true);
            for e in elts {
                container.push_args_chunk(e);
            }
            Box::new(container)
        };
        range_func.push_chunk(elements);

        if rf.alias.is_some() || rf.coldeflist.is_some() {
            range_func.push_keyword("AS");

            if let Some(AliasWrapper::Alias(a)) = &rf.alias {
                range_func.push_identifier(&a.aliasname);
            }
            if let Some(defs) = &rf.coldeflist {
                range_func.push_chunk(self.column_def_list(&defs)?);
            }
        }

        Ok(Box::new(range_func))
    }

    //#[trace]
    fn range_func_elements(
        &mut self,
        elts: &[RangeFunctionElement],
    ) -> Result<Vec<Box<dyn Chunk>>> {
        let mut elements: Vec<Box<dyn Chunk>> = vec![];
        for e in elts {
            match &e.0 {
                Node::FuncCall(fc) => {
                    let mut call = self.func_call(fc)?;
                    if let Some(defs) = &e.1 {
                        call.push_chunk(Box::new(Token::new_keyword("AS")));
                        call.push_chunk(self.column_def_list(defs)?);
                    }
                    elements.push(Box::new(call));
                }
                _ => return Err(TransformerError::RangeFunctionHasNonFuncCallElement),
            }
        }

        Ok(elements)
    }

    //#[trace]
    fn range_table_sample(&mut self, rts: &RangeTableSample) -> Result<Box<dyn Chunk>> {
        let mut table_sample = ChunkList::new(Joiner::Space);
        table_sample.push_chunk(self.node(&rts.relation)?);
        table_sample.push_keyword("TABLESAMPLE");

        let mut method = ChunkList::new(Joiner::Period);
        for m in &rts.method {
            method.push_chunk(self.node(m)?);
        }
        table_sample.push_chunk(Box::new(method));

        let mut args = DelimitedExpression::new(Delimiter::Paren, Joiner::Comma, false);
        for a in &rts.args {
            args.push_args_chunk(self.node(a)?);
        }
        table_sample.push_chunk(Box::new(args));

        if let Some(r) = &rts.repeatable {
            table_sample.push_keyword("REPEATABLE");

            let mut repeatable = DelimitedExpression::new(Delimiter::Paren, Joiner::Comma, false);
            repeatable.push_args_chunk(self.node(&*r)?);
            table_sample.push_chunk(Box::new(repeatable));
        }

        Ok(Box::new(table_sample))
    }

    //#[trace]
    fn column_def_list(&mut self, defs: &[ColumnDefWrapper]) -> Result<Box<dyn Chunk>> {
        let mut column_def_list = DelimitedExpression::new(Delimiter::Paren, Joiner::Comma, false);
        for ColumnDefWrapper::ColumnDef(d) in defs {
            column_def_list.push_args_chunk(self.column_def(d)?);
        }
        Ok(Box::new(column_def_list))
    }

    //#[trace]
    fn column_def(&mut self, def: &ColumnDef) -> Result<Box<dyn Chunk>> {
        // XXX - this might sometimes need to be rows instead, for create
        // table statements?
        let mut column_def = ChunkList::new(Joiner::Space);
        column_def.push_identifier(&def.colname);

        if let Some(TypeNameWrapper::TypeName(tn)) = &def.type_name {
            column_def.push_chunk(self.type_name(&tn)?);
        }

        Ok(Box::new(column_def))
    }

    // fn format_constraint(&mut self, c: &Constraint) -> R {
    //     match &c.contype {
    //         ConstrType::ConstrNull => Ok("NULL".into()),
    //         ConstrType::ConstrNotnull => Ok("NOT NULL".into()),
    //         ConstrType::ConstrDefault => self.format_default_constraint(c),
    //         ConstrType::ConstrIdentity => Ok(self.format_identity_constraint(c)),
    //         ConstrType::ConstrCheck => self.format_check_constraint(c),
    //         ConstrType::ConstrPrimary => self.format_unique_constraint(c, true),
    //         ConstrType::ConstrUnique => self.format_unique_constraint(c, false),
    //         ConstrType::ConstrExclusion => self.format_exclusion_constraint(c),
    //         ConstrType::ConstrForeign => Ok("FOREIGN".into()),
    //         // attributes for previous constraint node
    //         ConstrType::ConstrAttrDeferrable => Ok(String::new()),
    //         ConstrType::ConstrAttrNotDeferrable => Ok(String::new()),
    //         ConstrType::ConstrAttrDeferred => Ok(String::new()),
    //         ConstrType::ConstrAttrImmediate => Ok(String::new()),
    //     }
    // }

    // fn format_default_constraint(&mut self, c: &Constraint) -> R {
    //     let e = match &c.raw_expr {
    //         Some(e) => e,
    //         None => panic!("DEFAULT constraint without an expression"),
    //     };
    //     Ok(format!("DEFAULT {}", self.format_node(&*e)?))
    // }

    // fn format_identity_constraint(&mut self, c: &Constraint) -> String {
    //     let g = match c.generated_when() {
    //         Some(g) => g,
    //         None => panic!("IDENTITY constraint without generated_when"),
    //     };
    //     format!("GENERATED {} AS IDENTITY", g)
    // }

    // fn format_check_constraint(&mut self, c: &Constraint) -> R {
    //     let e = match &c.raw_expr {
    //         Some(e) => e,
    //         None => panic!("CHECK constraint without an expression"),
    //     };

    //     let mut check = "CHECK (".to_string();

    //     // This
    //     let expr = self.format_node(&*e)?;
    //     if expr.contains('\n') {
    //         check.push('\n');
    //         self.push_indent_one_level();
    //         check.push_str(&self.format_node(&*e)?);
    //         self.pop_indent();
    //         check.push('\n');
    //         check.push_str(&self.indent_str(")"));
    //     } else {
    //         check.push(' ');
    //         check.push_str(expr.trim());
    //         check.push_str(" )");
    //     }

    //     match &c.conname {
    //         Some(n) => Ok(format!("CONSTRAINT {} {}", n, check)),
    //         None => Ok(check),
    //     }
    // }

    // fn format_unique_constraint(&mut self, c: &Constraint, is_pk: bool) -> R {
    //     let typ = if is_pk { "PRIMARY KEY" } else { "UNIQUE" };
    //     let mut cons = match &c.conname {
    //         Some(n) => format!("CONSTRAINT {} {}", n, typ),
    //         None => typ.into(),
    //     };

    //     if let Some(k) = &c.keys {
    //         cons.push_str(&self.format_keys(k)?);
    //     }

    //     if let Some(opts) = &c.options {
    //         cons.push_str(&format!(" WITH {}", self.parenthesized_list(opts)?));
    //     }

    //     Ok(cons)
    // }

    // //#[trace]
    // fn format_exclusion_constraint(&mut self, c: &Constraint) -> R {
    //     let am = match &c.access_method {
    //         Some(a) => a,
    //         None => panic!("EXCLUDE constraint without an access_method"),
    //     };

    //     let ex = match &c.exclusions {
    //         Some(e) => e,
    //         None => panic!("EXCLUDE constraint without exclusions"),
    //     };

    //     Ok(format!(
    //         "EXCLUDE USING {} ( {} )",
    //         am,
    //         ex.iter()
    //             .map(|e| self.format_exclusion(e))
    //             .collect::<Result<Vec<_>, _>>()?
    //             .join(", "),
    //     ))
    // }

    // //#[trace]
    // fn format_exclusion(&mut self, e: &Exclusion) -> R {
    //     let mut excl = self.format_node(&e.0)?;
    //     excl.push_str(" WITH ");

    //     let mut formatter = new_context!(self, ContextType::ExclusionOperator);
    //     let oper =
    //         e.1.iter()
    //             .map(|n| formatter.format_node(n))
    //             .collect::<Result<Vec<_>, _>>()?
    //             // I'm not sure if this is right. What produces multiple
    //             // elements for the operator?
    //             .join(" ");
    //     excl.push_str(&oper);

    //     Ok(excl)
    // }

    // //#[trace]
    // fn format_keys(&mut self, k: &[Node]) -> R {
    //     let mut formatter = new_context!(self, ContextType::UniqueConstraintKeys);

    //     let mut keys = " (".to_string();

    //     let cols = formatter.formatted_list(k)?;
    //     if cols.len() > 1 {
    //         keys.push(' ');
    //     }
    //     keys.push_str(&cols.join(", "));
    //     if cols.len() > 1 {
    //         keys.push(' ');
    //     }
    //     keys.push(')');

    //     Ok(keys)
    // }

    //#[trace]
    fn insert_values(&mut self, i: &InsertStmt) -> Result<Box<dyn Chunk>> {
        match &i.select_stmt {
            Some(SelectStmtWrapper::SelectStmt(s)) => {
                if !Self::is_values_insert(s) {
                    let select = self.select_stmt(s)?;
                    return Ok(Box::new(SubStatement::new(select, true)));
                }

                let mut insert_values = InsertValues::new();

                for one_list in s.values_lists.as_ref().unwrap() {
                    let mut one_values: Vec<Box<dyn Chunk>> = vec![];
                    for v in one_list {
                        one_values.push(self.node(v)?);
                    }
                    insert_values.push_values(one_values);
                }
                Ok(Box::new(insert_values))
            }
            None => {
                let mut values_clause = ChunkList::new(Joiner::Space);
                values_clause.push_keyword("DEFAULT VALUES");
                Ok(Box::new(values_clause))
            }
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
    fn on_conflict_clause(&mut self, occ: &OnConflictClause) -> Result<Box<dyn Chunk>> {
        let mut on_conflict_clause = ChunkList::new(Joiner::Space);

        let mut first_clause = ChunkList::new(Joiner::Space);
        first_clause.push_keyword("ON CONFLICT");

        if let Some(InferClauseWrapper::InferClause(i)) = &occ.infer {
            first_clause.push_chunk(self.infer_clause(&i)?);
        }

        match occ.action {
            OnConflictAction::OnconflictNothing => {
                first_clause.push_keyword("DO NOTHING");
                on_conflict_clause.push_chunk(Box::new(first_clause));
                return Ok(Box::new(on_conflict_clause));
            }
            OnConflictAction::OnconflictUpdate => (),
            OnConflictAction::OnconflictNone => (),
        }

        on_conflict_clause.push_chunk(Box::new(first_clause));

        let mut do_update = Statement::new(self.indent_width, self.max_line_len);
        do_update.push_keyword("DO UPDATE");

        match occ.target_list.as_ref() {
            Some(targets) => do_update.push_chunk(self.res_targets_for_update(targets)?),
            None => return Err(TransformerError::OnConflictUpdateWithoutTargets),
        }

        if let Some(w) = &occ.where_clause {
            do_update.push_chunk(self.where_clause(&*w)?);
        }

        on_conflict_clause.push_chunk(Box::new(SubStatement::new(do_update, true)));

        Ok(Box::new(on_conflict_clause))
    }

    //#[trace]
    fn infer_clause(&mut self, i: &InferClause) -> Result<Box<dyn Chunk>> {
        let mut infer_clause = ChunkList::new(Joiner::Space);
        if let Some(ie) = &i.index_elems {
            let mut index_elems = DelimitedExpression::new(Delimiter::Paren, Joiner::Comma, false);
            for e in ie {
                index_elems.push_args_chunk(self.node(e)?);
            }
            infer_clause.push_chunk(Box::new(index_elems));
            if let Some(w) = &i.where_clause {
                infer_clause.push_chunk(self.where_clause(&*w)?);
            }
        } else if let Some(c) = &i.conname {
            infer_clause.push_keyword("ON CONSTRAINT");
            infer_clause.push_identifier(c);
        } else {
            return Err(TransformerError::InferClauseWithoutIndexElementsOrConstraint);
        }

        Ok(Box::new(infer_clause))
    }

    //#[trace]
    fn index_elem(&mut self, i: &IndexElem) -> Result<Box<dyn Chunk>> {
        let mut elem = ChunkList::new(Joiner::Space);

        if let Some(n) = &i.name {
            elem.push_chunk(Box::new(Token::new_identifier(n)));
        } else if let Some(e) = &i.expr {
            elem.push_chunk(self.node(&*e)?);
        } else {
            return Err(TransformerError::IndexElemWithoutNameOrExpr);
        }

        if let Some(c) = &i.collation {
            elem.push_chunk(Box::new(Token::new_keyword("COLLATE")));
            // This will only ever have 1 element per gram.y. I'm not sure it
            // can ever be something other than a StringStruct.
            match &c[0] {
                Node::StringStruct(StringStruct { str: name }) => {
                    elem.push_chunk(Box::new(Token::new_identifier(name)));
                }
                _ => panic!("COLLATE name is not a string"),
            }
        }

        if let Some(o) = self.sort_by_nulls_ordering(&i.nulls_ordering) {
            elem.push_chunk(Box::new(o));
        }

        if let Some(opclass) = &i.opclass {
            // XXX - I have no idea if this is the right formatting. Maybe
            // this stuff belongs in its own separate chunk? Maybe we should
            // also split out collation and sort by nulls?
            for o in opclass {
                elem.push_chunk(self.node(o)?);
            }
        }

        Ok(Box::new(elem))
    }

    fn res_targets_for_update(&mut self, targets: &[ResTargetWrapper]) -> Result<Box<dyn Chunk>> {
        let mut res_targets = UpdateSet::new();

        let mut i = 0;
        while i < targets.len() {
            let ResTargetWrapper::ResTarget(t) = &targets[i];
            if let Node::MultiAssignRef(m) = match &t.val {
                Some(v) => &**v,
                None => return Err(TransformerError::UpdateResTargetWithoutVal),
            } {
                let mut names: Vec<&String> = vec![];
                let mut j = 1;
                while j <= m.ncolumns {
                    match &targets[i] {
                        ResTargetWrapper::ResTarget(t) => match &t.name {
                            Some(n) => names.push(n),
                            None => return Err(TransformerError::UpdateResTargetWithoutName),
                        },
                    }
                    i += 1;
                    j += 1;
                }
                res_targets.push_set_clause(self.multi_assign_ref(m, names)?);
            } else {
                res_targets.push_set_clause(self.res_target(t, false)?);
            }

            i += 1;
        }

        Ok(Box::new(res_targets))
    }

    //#[trace]
    fn multi_assign_ref(
        &mut self,
        m: &MultiAssignRef,
        names: Vec<&String>,
    ) -> Result<Box<dyn Chunk>> {
        let mut multi_assign = ChunkList::new(Joiner::Space);

        multi_assign.push_chunk(self.multi_assign_ref_names(names));
        multi_assign.push_operator("=");
        multi_assign.push_chunk(self.node(&*m.source)?);

        Ok(Box::new(multi_assign))
    }

    //#[trace]
    fn multi_assign_ref_names(&self, names: Vec<&String>) -> Box<dyn Chunk> {
        // I don't know if this can ever happen, but if it does, we can
        // simplify this to a standard "x = y" SET clause.
        if names.len() == 1 {
            return Box::new(Token::new_identifier(names[0]));
        }

        let mut left = DelimitedExpression::new(Delimiter::Paren, Joiner::Comma, false);
        for n in names {
            left.push_args_identifier(n);
        }
        Box::new(left)
    }

    //#[trace]
    fn returning_clause(&mut self, rt: &[ResTargetWrapper]) -> Result<Box<dyn Chunk>> {
        let mut returning = ChunkList::new(Joiner::Space);
        returning.push_keyword("RETURNING");

        let mut list = ChunkList::new(Joiner::Comma);
        for ResTargetWrapper::ResTarget(t) in rt {
            list.push_chunk(self.res_target(t, true)?);
        }

        returning.push_chunk(Box::new(list));

        Ok(Box::new(returning))
    }

    // fn update_stmt_res_target_lines(
    //     &mut self,
    //     list: &[Node],
    // ) -> Result<Vec<String>, TransformerError> {
    //     let mut lines: Vec<String> = vec![];

    //     let mut i = 0;
    //     while i < list.len() {
    //         if let Node::ResTarget(t) = &list[i] {
    //             let v = match &t.val {
    //                 Some(v) => &**v,
    //                 None => {
    //                     panic!("got a ResTarget in an UpdateStmt target_list which is None!")
    //                 }
    //             };
    //             let l = if let Node::MultiAssignRef(m) = v {
    //                 let mut names: Vec<String> = vec![];
    //                 let mut j = 1;
    //                 while j <= m.ncolumns {
    //                     names.push(self.res_target_name_or_panic(&list[i]));
    //                     i += 1;
    //                     j += 1;
    //                 }
    //                 self.format_multi_assign(names, m)?
    //             } else {
    //                 self.format_res_target(t)?
    //             };
    //             lines.push(l);
    //         } else {
    //             panic!("got a node that is not a ResTarget in an UpdateStmt target_list!");
    //         };

    //         i += 1;
    //     }

    //     Ok(lines)
    // }

    // fn res_target_name_or_panic(&self, node: &Node) -> String {
    //     if let Node::ResTarget(t) = node {
    //         if let Some(n) = &t.name {
    //             return n.clone();
    //         }
    //         panic!("got a ResTarget in an UpdateStmt target_list without a name!");
    //     }
    //     panic!("got a Node in an UpdateStmt target_list that is not a ResTarget!");
    // }

    // //#[trace]
    // fn format_multi_assign(&mut self, names: Vec<String>, m: &MultiAssignRef) -> R {
    //     if names.len() == 1 {
    //         return Ok(format!("{} = {}", names[0], self.format_node(&*m.source)?));
    //     }

    //     let mut left = "( ".to_string();
    //     left.push_str(&names.join(", "));
    //     left.push_str(" )");

    //     // This seems to just work if RHS is a subselect, wrapping and all.
    //     let right = self.format_node(&*m.source)?;

    //     Ok(format!("{} = {}", left, right))
    // }
}
