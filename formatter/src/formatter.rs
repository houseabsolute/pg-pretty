use log::debug;
use std::collections::VecDeque;
use std::fmt;
use strum_macros::Display;
use thiserror::Error;

type Result<T> = std::result::Result<T, FormatterError>;

#[derive(Debug, Error)]
pub enum FormatterError {
    #[error("{what} does not have any chunks")]
    NoChunks { what: &'static str },
    #[error("InsertValues does not have any values")]
    NoValues,
    #[error("JoinClause has no first join")]
    NoFirstJoin,
    #[error("current indenter is a {style} indenter, not a Gutter indenter")]
    CurrentIndenterIsNotGutter { style: String },
}

#[derive(Clone, Copy, Debug, Display, PartialEq)]
enum IndentStyle {
    Gutter,
    // Insert style is just like TabStop except that top-level chunks after
    // the first are not indented. Using Gutter for INSERT looks bizarre with
    // "ON CONFLICT" clauses. If the indenter was aware that "ON CONFLICT"
    // should be treated like a single unit, we could get rid of Insert
    // style. But right now the indenter operates on strings, not the tokens
    // or chunks.
    Insert,
    TabStop,
}

#[derive(Clone, Copy, Debug, Display, PartialEq)]
pub enum Joiner {
    Comma,
    None,
    Period,
    Space,
}

#[derive(Clone, Copy, Debug, Display, PartialEq)]
pub enum FormatContext {
    SingleLine,
    MultiLine,
}

impl Joiner {
    fn single_line_joiner(&self) -> &'static str {
        match self {
            Joiner::Comma => ", ",
            Joiner::None => "",
            Joiner::Period => ".",
            Joiner::Space => " ",
        }
    }

    fn multi_line_joiner(&self) -> &'static str {
        match self {
            Joiner::Comma => ",",
            Joiner::None => "",
            Joiner::Period => ".",
            Joiner::Space => "",
        }
    }
}

pub trait Chunk: fmt::Debug {
    fn formatted(
        &self,
        ctxt: FormatContext,
        current_len: usize,
        indenter: &Indenter,
    ) -> Result<String>;

    // fn formatted2(&self) -> Result<String>;

    fn first_keyword(&self) -> Option<&str>;
}

#[derive(Clone, Debug, Display, PartialEq)]
pub enum Token {
    // SELECT, CREATE, etc.
    Keyword(String),
    // Column, table, type, etc. names
    Identifier(String),
    // String constant
    StringConst(String),
    // Int or decimal constant
    Number(String),
    // =, <>, ~, etc.
    Operator(String),
}

impl Token {
    pub fn new_keyword<S: AsRef<str>>(kw: S) -> Self {
        Token::Keyword(kw.as_ref().into())
    }

    pub fn new_identifier(identifier: &str) -> Self {
        Token::Identifier(identifier.into())
    }

    pub fn new_string(string: &str) -> Self {
        Token::StringConst(string.into())
    }

    pub fn new_number(number: &str) -> Self {
        Token::Number(number.into())
    }

    pub fn new_operator(op: &str) -> Self {
        Token::Operator(op.into())
    }

    fn as_str(&self) -> &str {
        match self {
            Token::Keyword(s) => s,
            Token::Identifier(s) => s,
            Token::StringConst(s) => s,
            Token::Number(s) => s,
            Token::Operator(s) => s,
        }
    }
}

impl Chunk for Token {
    fn formatted(&self, _: FormatContext, _: usize, _: &Indenter) -> Result<String> {
        let t = match self {
            Token::Keyword(s) => s.to_uppercase(),
            Token::Identifier(s) => {
                if s.contains(char::is_uppercase) {
                    format!(r#""{}""#, s)
                } else {
                    s.clone()
                }
            }
            Token::StringConst(s) => format!("'{}'", s.replace("'", "''")),
            Token::Number(s) => s.clone(),
            Token::Operator(s) => s.clone(),
        };
        Ok(t)
    }

    // fn formatted2(&self) -> Result<String> {
    //     let i = Indenter::first(IndentStyle::Gutter, 0, 0, "foo", None);
    //     self.formatted(FormatContext::SingleLine, 0, &i)
    // }

    fn first_keyword(&self) -> Option<&str> {
        match self {
            Token::Keyword(s) => Some(s),
            _ => None,
        }
    }
}

#[derive(Debug)]
pub struct Tokens {
    tokens: Vec<Token>,
    joiner: Joiner,
}

impl Tokens {
    pub fn new(joiner: Joiner) -> Self {
        Self {
            tokens: vec![],
            joiner,
        }
    }

    pub fn push_keyword(&mut self, kw: &str) {
        self.tokens.push(Token::new_keyword(kw));
    }

    pub fn push_identifier<S: AsRef<str>>(&mut self, identifier: S) {
        self.tokens.push(Token::new_identifier(identifier.as_ref()));
    }

    pub fn push_operator(&mut self, operator: &str) {
        self.tokens.push(Token::new_operator(operator));
    }

    pub fn last_token(&self) -> Option<String> {
        match self.tokens.is_empty() {
            true => None,
            false => Some(self.tokens[self.tokens.len() - 1].as_str().to_string()),
        }
    }
}

impl Chunk for Tokens {
    fn formatted(&self, ctxt: FormatContext, _: usize, indenter: &Indenter) -> Result<String> {
        let mut out = start_from_context(ctxt);
        out.push_str(
            &self
                .tokens
                .iter()
                .map(|t| t.formatted(ctxt, 0, indenter))
                .collect::<Result<Vec<_>>>()?
                .join(self.joiner.single_line_joiner()),
        );
        Ok(out)
    }

    // fn formatted2(&self) -> Result<String> {
    //     let i = Indenter::first(IndentStyle::Gutter, 0, 0, "foo", None);
    //     self.formatted(FormatContext::SingleLine, 0, &i)
    // }

    fn first_keyword(&self) -> Option<&str> {
        self.tokens[0].first_keyword()
    }
}

#[derive(Debug)]
pub struct ChunkList {
    chunks: Vec<Box<dyn Chunk>>,
    joiner: Joiner,
}

impl ChunkList {
    pub fn new(joiner: Joiner) -> Self {
        Self {
            chunks: vec![],
            joiner,
        }
    }

    pub fn push_chunk(&mut self, chunk: Box<dyn Chunk>) {
        self.chunks.push(chunk);
    }

    pub fn push_keyword(&mut self, kw: &str) {
        self.chunks.push(Box::new(Token::new_keyword(kw)));
    }

    pub fn push_identifier<S: AsRef<str>>(&mut self, identifier: S) {
        self.chunks
            .push(Box::new(Token::new_identifier(identifier.as_ref())));
    }

    pub fn push_operator(&mut self, op: &str) {
        self.chunks.push(Box::new(Token::new_operator(op)));
    }

    pub fn is_empty(&self) -> bool {
        self.chunks.is_empty()
    }
}

impl Chunk for ChunkList {
    fn formatted(
        &self,
        ctxt: FormatContext,
        current_len: usize,
        indenter: &Indenter,
    ) -> Result<String> {
        if self.is_empty() {
            return Err(FormatterError::NoChunks { what: "ChunkList" });
        }

        let mut out = start_from_context(ctxt);
        let formatted = &self
            .chunks
            .iter()
            .map(|c| c.formatted(FormatContext::SingleLine, current_len, indenter))
            .collect::<Result<Vec<_>>>()?;
        for (i, f) in formatted.iter().enumerate() {
            // If one of the formatted chunks was a SubStatement, it will
            // already have its own leading newline, in which case joining it
            // with a space doesn't make any sense. This is kind of gross.
            if i > 0 {
                if f.starts_with('\n') {
                    out.push_str(self.joiner.multi_line_joiner());
                    out.push('\n');
                } else {
                    out.push_str(self.joiner.single_line_joiner());
                }
            }
            out.push_str(f);
        }

        // If one of the chunks contains newlines we only want to look at the
        // length of the first line? I suspect there's a better way to
        // approach this.
        let indent_from = out.find('\n').unwrap_or(0);
        let sl_len = out.len() - indent_from;
        debug!("SL = [{}]", out);
        debug!(
            "current_len = {}, indent_from = {}, sl_len = {}, max_line_len = {}",
            current_len, indent_from, sl_len, indenter.max_line_len
        );

        if current_len + sl_len <= indenter.max_line_len {
            return Ok(out);
        }

        out = start_from_context(ctxt);

        let mut remaining_chunk_range = 0..;
        let mut use_next_indenter = false;
        // We know we have at least one chunk because we checked is_empty()
        // earlier.
        let first = self.chunks.first().unwrap();
        // We special case the first chunk of a Gutter indented chunk if it
        // starts with a keyword.
        if first.first_keyword().is_some() && indenter.style == IndentStyle::Gutter {
            out.push_str(&indenter.indent(self.chunks.first().unwrap().formatted(
                FormatContext::MultiLine,
                current_len,
                indenter,
            )?));
            out.push_str(self.joiner.multi_line_joiner());
            out.push('\n');
            remaining_chunk_range = 1..;
            use_next_indenter = true
        }

        if let Some(rest) = self.chunks.get(remaining_chunk_range) {
            let next = indenter.next_indenter();
            let i = if use_next_indenter { &next } else { indenter };
            out.push_str(
                &rest
                    .iter()
                    .map(|c| c.formatted(FormatContext::MultiLine, current_len, i))
                    .collect::<Result<Vec<_>>>()?
                    .iter()
                    .map(|f| i.indent(f))
                    .collect::<Vec<_>>()
                    .join(&format!("{}\n", self.joiner.multi_line_joiner())),
            );
        };

        debug!("ML = [{}]", out);
        Ok(out)
    }

    fn first_keyword(&self) -> Option<&str> {
        self.chunks[0].first_keyword()
    }
}

#[derive(Clone, Copy, Debug, Display, PartialEq)]
pub enum Delimiter {
    Paren,
    Square,
}

impl Delimiter {
    fn delimiters(&self) -> (char, char) {
        match self {
            Delimiter::Paren => ('(', ')'),
            Delimiter::Square => ('[', ']'),
        }
    }
}

#[derive(Debug)]
pub struct DelimiterContents {
    prefix: Vec<Box<dyn Chunk>>,
    args: Vec<Box<dyn Chunk>>,
    suffix: Vec<Box<dyn Chunk>>,
}

pub trait Delimited {
    fn format_args_single_line(
        &self,
        current_len: usize,
        indenter: &Indenter,
        contents: &DelimiterContents,
        joiner: &Joiner,
        delimiter: &Delimiter,
    ) -> Result<String> {
        let (p, a, s) =
            self.formatted_chunks(FormatContext::SingleLine, current_len, indenter, contents)?;
        let args = a.join(joiner.single_line_joiner());

        let (left_delim, right_delim) = delimiter.delimiters();
        let space = if self.args_are_complex(&p, &args, &s) {
            " "
        } else {
            ""
        };

        Ok(format!(
            "{left_delim}{space}{prefix}{prefix_separator}{args}{suffix_separator}{suffix}{space}{right_delim}",
            left_delim = left_delim,
            space = space,
            prefix = p,
            prefix_separator = if p.is_empty() { "" } else { " " },
            args = args,
            suffix_separator = if s.is_empty() { "" } else { " " },
            suffix = s,
            right_delim = right_delim,
        ))
    }

    fn format_args_multi_line(
        &self,
        current_len: usize,
        indenter: &Indenter,
        contents: &DelimiterContents,
        joiner: &Joiner,
        delimiter: &Delimiter,
    ) -> Result<String> {
        let next = match indenter.style {
            IndentStyle::Gutter => indenter.next_indenter().next_indenter(),
            _ => indenter.next_indenter(),
        };

        let (p, a, s) =
            self.formatted_chunks(FormatContext::MultiLine, current_len, indenter, contents)?;
        let args = a
            .iter()
            // We will remove any leading newlines and add trailing ones
            // instead.
            .map(|a| next.indent(a.trim_start()))
            .collect::<Vec<_>>()
            .join(&format!("{}\n", joiner.multi_line_joiner()));

        let (left_delim, right_delim) = delimiter.delimiters();

        Ok(format!(
            "{left_delim}\n{prefix}{prefix_separator}{args}{suffix_separator}{suffix}\n{right_delim}",
            left_delim = left_delim,
            prefix = p,
            prefix_separator = if p.is_empty() { "" } else { " " },
            args = args,
            suffix_separator = if s.is_empty() { "" } else { " " },
            suffix = s,
            right_delim = indenter.indent(right_delim.to_string()),
        ))
    }

    fn formatted_chunks(
        &self,
        ctxt: FormatContext,
        current_len: usize,
        indenter: &Indenter,
        contents: &DelimiterContents,
    ) -> Result<(String, Vec<String>, String)> {
        let p = contents
            .prefix
            .iter()
            .map(|c| c.formatted(ctxt, current_len, indenter))
            .collect::<Result<Vec<_>>>()?
            .join(" ");

        let a = contents
            .args
            .iter()
            .map(|c| c.formatted(ctxt, current_len, indenter))
            .collect::<Result<Vec<_>>>()?;

        let s = contents
            .suffix
            .iter()
            .map(|c| c.formatted(ctxt, current_len, indenter))
            .collect::<Result<Vec<_>>>()?
            .join(" ");

        Ok((p, a, s))
    }

    fn args_are_complex(&self, prefix: &str, args: &str, suffix: &str) -> bool {
        !prefix.is_empty() || args.contains(&[',', '(', ' '][..]) || !suffix.is_empty()
    }
}

#[derive(Debug)]
pub struct DelimitedExpression {
    delimiter: Delimiter,
    contents: DelimiterContents,
    joiner: Joiner,
    force_multi_line: bool,
}

impl DelimitedExpression {
    pub fn new(delimiter: Delimiter, joiner: Joiner, force_multi_line: bool) -> Self {
        Self {
            delimiter,
            contents: DelimiterContents {
                prefix: vec![],
                args: vec![],
                suffix: vec![],
            },
            joiner,
            force_multi_line,
        }
    }

    // pub fn push_prefix_chunk(&mut self, chunk: Box<dyn Chunk>) {
    //     self.prefix.push(chunk);
    // }

    pub fn push_args_chunk(&mut self, chunk: Box<dyn Chunk>) {
        self.contents.args.push(chunk);
    }

    // pub fn push_keyword(&mut self, kw: &str) {
    //     self.contents.args.push(Box::new(Token::new_keyword(kw)));
    // }

    pub fn push_args_identifier<S: AsRef<str>>(&mut self, identifier: S) {
        self.contents
            .args
            .push(Box::new(Token::new_identifier(identifier.as_ref())));
    }

    // pub fn push_operator(&mut self, op: &str) {
    //     self.contents.args.push(Box::new(Token::new_operator(op)));
    // }

    // pub fn push_suffix_chunk(&mut self, chunk: Box<dyn Chunk>) {
    //     self.suffix.push(chunk);
    // }
}

impl Delimited for DelimitedExpression {}

impl Chunk for DelimitedExpression {
    fn formatted(
        &self,
        _: FormatContext,
        current_len: usize,
        indenter: &Indenter,
    ) -> Result<String> {
        if self.contents.args.is_empty() {
            return Err(FormatterError::NoChunks {
                what: "DelimitedExpression",
            });
        }

        let single = self.format_args_single_line(
            current_len,
            indenter,
            &self.contents,
            &self.joiner,
            &self.delimiter,
        )?;

        if !self.force_multi_line && current_len + single.len() <= indenter.max_line_len {
            return Ok(single);
        }

        self.format_args_multi_line(
            current_len,
            indenter,
            &self.contents,
            &self.joiner,
            &self.delimiter,
        )
    }

    fn first_keyword(&self) -> Option<&str> {
        None
    }
}

#[derive(Debug)]
pub struct Func {
    name: Tokens,
    contents: DelimiterContents,
}

impl Func {
    pub fn new(name: Tokens) -> Self {
        Self {
            name,
            contents: DelimiterContents {
                prefix: vec![],
                args: vec![],
                suffix: vec![],
            },
        }
    }

    pub fn push_args_prefix_chunk(&mut self, chunk: Box<dyn Chunk>) {
        self.contents.prefix.push(chunk);
    }

    pub fn push_args_chunk(&mut self, chunk: Box<dyn Chunk>) {
        self.contents.args.push(chunk);
    }

    // pub fn push_suffix_chunk(&mut self, chunk: Box<dyn Chunk>) {
    //     self.suffix.push(chunk);
    // }
}

impl Delimited for Func {}

impl Chunk for Func {
    fn formatted(
        &self,
        ctxt: FormatContext,
        current_len: usize,
        indenter: &Indenter,
    ) -> Result<String> {
        if self.contents.args.is_empty() {
            return Err(FormatterError::NoChunks { what: "Func" });
        }

        let name = self.name.formatted(ctxt, current_len, indenter)?;
        let single = self.format_args_single_line(
            current_len,
            indenter,
            &self.contents,
            &Joiner::Comma,
            &Delimiter::Paren,
        )?;

        if current_len + name.len() + single.len() <= indenter.max_line_len {
            let mut out = name;
            out.push_str(&single);
            return Ok(out);
        }

        let next = indenter.next_indenter();
        let mut out = name;
        out.push_str(&self.format_args_multi_line(
            current_len,
            &next,
            &self.contents,
            &Joiner::Comma,
            &Delimiter::Paren,
        )?);

        Ok(out)
    }

    fn first_keyword(&self) -> Option<&str> {
        None
    }
}

#[derive(Debug)]
pub struct SubStatement {
    stmt: Statement,
    // An inline sub statement is not wrapped in parens and is at the same
    // indentation level as its parent. An example is an "INSERT ... SELECT".
    is_inline: bool,
}

impl SubStatement {
    pub fn new(mut stmt: Statement, requires_parens: bool) -> Self {
        stmt.is_inline_sub_statement = !requires_parens;
        Self {
            stmt,
            is_inline: requires_parens,
        }
    }
}

impl Chunk for SubStatement {
    fn formatted(
        &self,
        ctxt: FormatContext,
        current_len: usize,
        indenter: &Indenter,
    ) -> Result<String> {
        let mut out = String::new();
        if self.is_inline {
            out.push('\n');
        } else {
            out.push('(');
        }

        let first_kw = self.stmt.first_keyword().unwrap();
        let next = Indenter::first(
            IndentStyle::Gutter,
            indenter.indent_width,
            indenter.max_line_len,
            first_kw,
            if self.is_inline {
                None
            } else {
                Some(indenter.current + indenter.indent_width + first_kw.len() + 1)
            },
        );

        debug!("next = {:?}", next);
        out.push_str(&self.stmt.formatted(ctxt, current_len, &next)?);

        // The sub-statement will already have a trailing newline.
        if !self.is_inline {
            out.push('\n');
            out.push_str(&indenter.indent(")"));
        }

        Ok(out)
    }

    fn first_keyword(&self) -> Option<&str> {
        match self.is_inline {
            true => self.stmt.first_keyword(),
            false => None,
        }
    }
}

#[derive(Debug)]
pub struct BoolOpChunk {
    bool_op: Token,
    chunks: Vec<Box<dyn Chunk>>,
}

impl BoolOpChunk {
    pub fn new(bool_op: Token, chunks: Vec<Box<dyn Chunk>>) -> Self {
        Self { bool_op, chunks }
    }
}

impl Chunk for BoolOpChunk {
    fn formatted(
        &self,
        ctxt: FormatContext,
        current_len: usize,
        indenter: &Indenter,
    ) -> Result<String> {
        let mut out = start_from_context(ctxt);
        out.push_str(&self.chunks[0].formatted(ctxt, current_len, indenter)?);

        let op = self.bool_op.as_str();
        for i in 1..self.chunks.len() {
            let mut next = "\n".to_string();
            next.push_str(op);
            next.push(' ');
            next.push_str(&self.chunks[i].formatted(ctxt, current_len, indenter)?);
            out.push_str(&indenter.indent(next));
        }

        Ok(out)
    }

    fn first_keyword(&self) -> Option<&str> {
        self.chunks[0].first_keyword()
    }
}

#[derive(Debug)]
pub struct InsertValues {
    values: Vec<DelimitedExpression>,
}

impl InsertValues {
    pub fn new() -> Self {
        InsertValues { values: vec![] }
    }

    pub fn push_values(&mut self, values: Vec<Box<dyn Chunk>>) {
        let mut del = DelimitedExpression::new(Delimiter::Paren, Joiner::Comma, false);
        for c in values {
            del.push_args_chunk(c);
        }
        self.values.push(del);
    }
}

impl Chunk for InsertValues {
    fn formatted(
        &self,
        _: FormatContext,
        current_len: usize,
        indenter: &Indenter,
    ) -> Result<String> {
        if self.values.is_empty() {
            return Err(FormatterError::NoValues);
        }

        let mut requires_multi_line = false;
        for v in &self.values {
            let single_line = v.formatted(
                FormatContext::SingleLine,
                current_len + "VALUES ".len(),
                indenter,
            )?;
            if current_len + single_line.len() > indenter.max_line_len {
                requires_multi_line = true;
                break;
            }
        }

        let mut formatted_values: Vec<String> = vec![];
        for (n, v) in self.values.iter().enumerate() {
            let prefix = if n == 0 {
                "VALUES "
            } else if !requires_multi_line {
                "       "
            } else {
                ""
            };

            let (ctxt, len) = if requires_multi_line {
                // Setting the len to the max line length is a hack to ensure
                // that every values list is formatted multi-line.
                (FormatContext::MultiLine, indenter.max_line_len)
            } else {
                (FormatContext::SingleLine, current_len + prefix.len())
            };

            formatted_values.push(format!("{}{}", prefix, v.formatted(ctxt, len, indenter)?,));
        }

        // If we have a multi line values list, we join that as "), (", so we
        // _don't_ need to join each list of values.
        let join = if requires_multi_line { ", " } else { ",\n" };
        Ok(format!("\n{}", formatted_values.join(join)))
    }

    fn first_keyword(&self) -> Option<&str> {
        None
    }
}

#[derive(Debug)]
pub struct UpdateSet {
    set_clauses: Vec<Box<dyn Chunk>>,
}

impl UpdateSet {
    pub fn new() -> Self {
        UpdateSet {
            set_clauses: vec![],
        }
    }

    pub fn push_set_clause(&mut self, set: Box<dyn Chunk>) {
        self.set_clauses.push(set);
    }
}

impl Chunk for UpdateSet {
    fn formatted(
        &self,
        _: FormatContext,
        current_len: usize,
        indenter: &Indenter,
    ) -> Result<String> {
        if self.set_clauses.is_empty() {
            return Err(FormatterError::NoValues);
        }

        let next = indenter.next_indenter();

        let mut lines: Vec<String> = vec![];
        for (n, s) in self.set_clauses.iter().enumerate() {
            let prefix = if n == 0 { "SET " } else { "" };
            let mut line = format!(
                "{}{}",
                prefix,
                s.formatted(
                    FormatContext::SingleLine,
                    current_len + prefix.len(),
                    indenter
                )?,
            );
            // XXX - This is a bit gross, but multi-line chunks need to do
            // their own indenting for all lines but first. I think this needs
            // some reconsideration to make indentation simpler. It should all
            // be done at the same "level". Maybe chunks need to return a Vec
            // of lines rather than strings that might contain newlines?
            if n == 0 {
                line = indenter.indent(line);
            } else {
                line = next.indent(line);
            }
            lines.push(line);
        }

        Ok(lines.join(",\n"))
    }

    fn first_keyword(&self) -> Option<&str> {
        Some("SET")
    }
}

#[derive(Debug)]
pub struct JoinClause {
    is_first_from_element: bool,
    first: Option<Box<dyn Chunk>>,
    rest: VecDeque<JoinRHS>,
}

#[derive(Debug)]
pub struct JoinRHS {
    what: Box<dyn Chunk>,
    join_type: Token,
    condition: Option<JoinCondition>,
}

#[derive(Debug)]
pub enum JoinCondition {
    On(Box<dyn Chunk>),
    Using(DelimitedExpression),
}

impl JoinClause {
    pub fn new(is_first_from_element: bool) -> Self {
        Self {
            is_first_from_element,
            first: None,
            rest: VecDeque::new(),
        }
    }

    pub fn set_first(&mut self, first: Box<dyn Chunk>) {
        self.first = Some(first);
    }

    pub fn push_front_join(
        &mut self,
        what: Box<dyn Chunk>,
        join_type: Token,
        condition: Option<JoinCondition>,
    ) {
        self.rest.push_front(JoinRHS {
            what,
            join_type,
            condition,
        });
    }

    pub fn push_back_join(
        &mut self,
        what: Box<dyn Chunk>,
        join_type: Token,
        condition: Option<JoinCondition>,
    ) {
        self.rest.push_back(JoinRHS {
            what,
            join_type,
            condition,
        });
    }
}

impl Chunk for JoinClause {
    fn formatted(
        &self,
        _: FormatContext,
        current_len: usize,
        indenter: &Indenter,
    ) -> Result<String> {
        let first = match &self.first {
            Some(f) => f,
            None => return Err(FormatterError::NoFirstJoin),
        };

        let next = indenter.next_indenter();

        let mut lines: Vec<String> = vec![];

        // We trim the indentation off this and we will add it back if this
        // join clause is not the first from element.
        let mut first_line = first
            .formatted(FormatContext::SingleLine, current_len, indenter)?
            .trim_start()
            .to_string();
        debug!("First line = [{}]", first_line);

        // We special case a single join with a USING clause and allow
        // that to be written on one line. If we have more than one join,
        // or an ON clause, we always use multiple lines.
        if self.rest.len() == 1 {
            match &self.rest[0].condition {
                None => {
                    let what = self.rest[0].what.formatted(
                        FormatContext::SingleLine,
                        indenter.current,
                        indenter,
                    )?;
                    let single = format!(
                        "{} {} {}",
                        first_line,
                        self.rest[0].join_type.as_str(),
                        what,
                    );
                    if single.len() + indenter.current <= indenter.max_line_len {
                        return match self.is_first_from_element {
                            true => Ok(single),
                            false => Ok(indenter.indent(single)),
                        };
                    }
                }
                Some(JoinCondition::Using(u)) => {
                    let what = self.rest[0].what.formatted(
                        FormatContext::SingleLine,
                        indenter.current,
                        indenter,
                    )?;
                    let using =
                        u.formatted(FormatContext::SingleLine, indenter.current, indenter)?;
                    let single = format!(
                        "{} {} {} USING {}",
                        first_line,
                        self.rest[0].join_type.as_str(),
                        what,
                        using,
                    );
                    if single.len() + indenter.current <= indenter.max_line_len {
                        return match self.is_first_from_element {
                            true => Ok(single),
                            false => Ok(indenter.indent(single)),
                        };
                    }
                }
                _ => (),
            }
        }

        if !self.is_first_from_element {
            first_line = format!("\n{}", indenter.indent(&first_line));
        }
        lines.push(first_line);

        for j in self.rest.iter() {
            let what = j
                .what
                .formatted(FormatContext::SingleLine, indenter.current, indenter)?;
            let line = next.indent(format!("{} {}", j.join_type.as_str(), what));

            let next_next = next.next_indenter();
            match &j.condition {
                Some(JoinCondition::On(o)) => {
                    let on =
                        o.formatted(FormatContext::SingleLine, next_next.current, &next_next)?;
                    let single = next.indent(format!("{} ON {}", line, on));
                    if single.len() <= next.max_line_len {
                        lines.push(single);
                    } else {
                        lines.push(line);
                        lines.push(next_next.indent(format!("ON {}", on)));
                    }
                }
                Some(JoinCondition::Using(u)) => {
                    let using =
                        u.formatted(FormatContext::SingleLine, indenter.current, indenter)?;
                    let single = next.indent(format!("{} USING {}", line, using));
                    if single.len() <= next.max_line_len {
                        lines.push(single);
                    } else {
                        lines.push(line);
                        lines.push(next_next.indent(format!("USING {}", using)));
                    }
                }
                None => (),
            }
        }

        Ok(lines.join("\n"))
    }

    fn first_keyword(&self) -> Option<&str> {
        None
    }
}

#[derive(Debug)]
pub struct Statement {
    chunks: Vec<Box<dyn Chunk>>,
    indent_width: usize,
    max_line_len: usize,
    is_inline_sub_statement: bool,
}

impl Statement {
    pub fn new(indent_width: usize, max_line_len: usize) -> Self {
        Self {
            chunks: vec![],
            indent_width,
            max_line_len,
            is_inline_sub_statement: false,
        }
    }

    fn uses_gutter_indent(&self) -> bool {
        matches!(
            self.first_keyword(),
            Some("SELECT") | Some("UPDATE") | Some("DO UPDATE") | Some("DELETE")
        )
    }

    fn is_insert(&self) -> bool {
        if let Some(kw) = self.first_keyword() {
            return kw.starts_with("INSERT ");
        }
        false
    }

    pub fn push_chunk(&mut self, chunk: Box<dyn Chunk>) {
        self.chunks.push(chunk);
    }

    pub fn push_keyword(&mut self, kw: &str) {
        self.chunks.push(Box::new(Token::new_keyword(kw)));
    }

    pub fn as_string(&self, indent: Option<usize>) -> Result<String> {
        if self.chunks.is_empty() {
            return Err(FormatterError::NoChunks { what: "Statement" });
        }

        let indent_style = if self.uses_gutter_indent() {
            IndentStyle::Gutter
        } else if self.is_insert() {
            IndentStyle::Insert
        } else {
            IndentStyle::TabStop
        };
        let indenter = Indenter::first(
            indent_style,
            self.indent_width,
            self.max_line_len,
            self.first_keyword().unwrap(),
            indent,
        );

        debug!("formatting statement: {:#?}", self.chunks);
        let mut out = self.formatted(FormatContext::SingleLine, 0, &indenter)?;
        out.push('\n');

        Ok(out)
    }
}

impl Chunk for Statement {
    fn formatted(
        &self,
        ctxt: FormatContext,
        current_len: usize,
        indenter: &Indenter,
    ) -> Result<String> {
        // DML statements are always formatted with multiple lines.
        if self.uses_gutter_indent() || self.is_insert() {
            debug!("this statement is only formatted with multiple lines");
        } else {
            debug!("trying single line for statement");
            let mut out = start_from_context(ctxt);
            out.push_str(
                &indenter.indent(
                    self.chunks
                        .iter()
                        .map(|c| c.formatted(FormatContext::SingleLine, current_len, indenter))
                        .collect::<Result<Vec<_>>>()?
                        .join(" "),
                ),
            );

            if current_len + out.len() <= indenter.max_line_len {
                return Ok(out);
            }
        }

        let mut out = start_from_context(ctxt);
        let next = indenter.next_indenter();

        for (i, c) in self.chunks.iter().enumerate() {
            // When formatting TabStop style (used for DDL statements) we
            // indent all lines after the first.
            let indenter = if i >= 1 && indenter.style == IndentStyle::TabStop {
                debug!("Using next indenter for: {:#?}", c);
                &next
            } else {
                debug!("Using current indenter for: {:#?}", c);
                indenter
            };

            let formatted = c.formatted(FormatContext::MultiLine, current_len, indenter)?;
            debug!("C = [{}]", formatted);
            if i == 0 && !self.is_inline_sub_statement {
                debug!("pushing formatted text with start trimmed");
                out.push_str(formatted.trim_start());
            } else {
                debug!("pushing formatted text with indentation");
                out.push_str(&indenter.indent(&formatted));
            }
        }

        Ok(out)
    }

    fn first_keyword(&self) -> Option<&str> {
        self.chunks[0].first_keyword()
    }
}

// pub struct DDLStatement {
//     chunks: Vec<Box<dyn Chunk>>,
// }

// impl DDLStatement {
//     fn formatted(&self, indenter: Option<Indenter>) -> Result<String> {
//         let mut out = String::new();
//         for c in self.chunks {}
//         Ok(String::new())
//     }
// }

#[derive(Debug)]
pub struct Indenter {
    style: IndentStyle,
    indent_width: usize,
    max_line_len: usize,
    current: usize,
}

impl Indenter {
    fn first<S: AsRef<str>>(
        style: IndentStyle,
        indent_width: usize,
        max_line_len: usize,
        first_kw: S,
        current: Option<usize>,
    ) -> Self {
        Self {
            style,
            indent_width,
            max_line_len,
            current: current.unwrap_or_else(|| match style {
                IndentStyle::Gutter => first_kw.as_ref().len() + 1,
                IndentStyle::Insert | IndentStyle::TabStop => 0,
            }),
        }
    }

    fn indent<S: AsRef<str>>(&self, s: S) -> String {
        let mut s = s.as_ref();
        let starts_with_nl = s.starts_with('\n');
        s = s.trim_start();

        let mut d = String::new();
        let indent = match self.style {
            IndentStyle::Gutter => {
                if s.starts_with('(') {
                    d.push_str("starts with paren");
                    // If the first character is an opening paren, we indent
                    // it so that the opening paren comes on the right side of
                    // the gutter.
                    self.current
                } else if let Some(sp) = s.find(' ') {
                    d.push_str(&format!(
                        "first space; to ({}) >= self.current ({})?",
                        sp, self.current
                    ));
                    if sp >= self.current {
                        d.push_str(" yes");
                        0
                    } else {
                        d.push_str(" no");
                        // - 1 for the gutter space itself.
                        (self.current - 1) - sp
                    }
                } else {
                    d.push_str(&format!("no space; to {}", self.current));
                    // Otherwise we just give up and won't indent this line at
                    // all.
                    self.current
                }
            }
            IndentStyle::Insert | IndentStyle::TabStop => {
                d.push_str(&format!("current = {}", self.current));
                self.current
            }
        };
        d.push_str(&format!("; indent = {}", indent));
        self.debug(s, d);

        let mut indented = if starts_with_nl {
            String::from("\n")
        } else {
            String::new()
        };
        indented.push_str(&" ".repeat(indent));
        indented.push_str(s);

        indented
    }

    fn debug(&self, s: &str, extra: String) {
        debug!("Indenting [{}]; style = {}; {}", s, self.style, extra)
    }

    fn next_indenter(&self) -> Self {
        match self.style {
            IndentStyle::Gutter => Self {
                style: IndentStyle::TabStop,
                indent_width: self.indent_width,
                max_line_len: self.max_line_len,
                current: self.current,
            },
            _ => Self {
                style: IndentStyle::TabStop,
                indent_width: self.indent_width,
                max_line_len: self.max_line_len,
                current: match self.style {
                    IndentStyle::Gutter => self.indent_width,
                    IndentStyle::Insert | IndentStyle::TabStop => self.current + self.indent_width,
                },
            },
        }
    }
}

fn start_from_context(ctxt: FormatContext) -> String {
    match ctxt {
        FormatContext::SingleLine => String::new(),
        FormatContext::MultiLine => String::from("\n"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use anyhow::{anyhow, Error};
    use prettydiff::diff_lines;

    const INDENT_WIDTH: usize = 4;
    const MAX_WIDTH: usize = 80;

    #[test]
    fn test_formatter_two_chunks() -> std::result::Result<(), Error> {
        let mut chunks = ChunkList::new(Joiner::Space);
        chunks.push_keyword("SELECT");
        chunks.push_chunk(Box::new(Token::new_number("42")));
        assert_formatted(Box::new(chunks), "SELECT", IndentStyle::Gutter, "SELECT 42")
    }

    #[test]
    fn test_formatter_two_chunklists_that_dont_wrap() -> std::result::Result<(), Error> {
        let mut stmt = Statement::new(MAX_WIDTH, INDENT_WIDTH);

        let mut select_clause = ChunkList::new(Joiner::Comma);

        let mut first_column = ChunkList::new(Joiner::Space);
        first_column.push_chunk(Box::new(Token::new_keyword("SELECT")));
        first_column.push_chunk(Box::new(Token::new_identifier("column1")));
        select_clause.push_chunk(Box::new(first_column));

        select_clause.push_chunk(Box::new(Token::new_identifier("column2")));

        stmt.push_chunk(Box::new(select_clause));

        // For real formatting we use the JoinClause struct for the FROM
        // clause, but in this case we just want to test formatting of
        // multiple ChunkLists.
        let mut from_clause = ChunkList::new(Joiner::Space);
        from_clause.push_keyword("FROM");
        from_clause.push_identifier("table");

        stmt.push_chunk(Box::new(from_clause));

        assert_formatted(
            Box::new(stmt),
            "SELECT",
            IndentStyle::Gutter,
            r#"
SELECT column1, column2
  FROM table
"#,
        )
    }

    #[test]
    fn test_formatter_one_chunklist_that_wraps() -> std::result::Result<(), Error> {
        env_logger::init();

        let mut stmt = Statement::new(MAX_WIDTH, INDENT_WIDTH);
        stmt.push_chunk(long_select_clause());

        assert_formatted(
            Box::new(stmt),
            "SELECT",
            IndentStyle::Gutter,
            r#"
SELECT column1,
       a234567890_b234567890_1,
       a234567890_b234567890_2,
       a234567890_b234567890_3,
       a234567890_b234567890_4,
       a234567890_b234567890_5
"#,
        )
    }

    #[test]
    fn test_formatter_two_chunklists_that_wrap() -> std::result::Result<(), Error> {
        let mut stmt = Statement::new(MAX_WIDTH, INDENT_WIDTH);
        stmt.push_chunk(long_select_clause());

        // For real formatting we use the JoinClause struct for the FROM
        // clause, but in this case we just want to test formatting of
        // multiple ChunkLists.
        let mut from_clause = ChunkList::new(Joiner::Comma);

        let mut first_table = ChunkList::new(Joiner::Space);
        first_table.push_keyword("FROM");
        first_table.push_identifier("table1");

        from_clause.push_chunk(Box::new(first_table));

        for x in 1..=5 {
            from_clause.push_chunk(Box::new(Token::new_identifier(&format!(
                "table_a234567890_b234567890_{}",
                x
            ))));
        }

        stmt.push_chunk(Box::new(from_clause));

        assert_formatted(
            Box::new(stmt),
            "SELECT",
            IndentStyle::Gutter,
            r#"
SELECT column1,
       a234567890_b234567890_1,
       a234567890_b234567890_2,
       a234567890_b234567890_3,
       a234567890_b234567890_4,
       a234567890_b234567890_5
  FROM table1,
       table_a234567890_b234567890_1,
       table_a234567890_b234567890_2,
       table_a234567890_b234567890_3,
       table_a234567890_b234567890_4,
       table_a234567890_b234567890_5
"#,
        )
    }

    fn long_select_clause() -> Box<ChunkList> {
        let mut select_clause = ChunkList::new(Joiner::Comma);

        let mut first_column = ChunkList::new(Joiner::Space);
        first_column.push_chunk(Box::new(Token::new_keyword("SELECT")));
        first_column.push_chunk(Box::new(Token::new_identifier("column1")));
        select_clause.push_chunk(Box::new(first_column));

        for x in 1..=5 {
            select_clause.push_chunk(Box::new(Token::new_identifier(&format!(
                "a234567890_b234567890_{}",
                x
            ))));
        }

        Box::new(select_clause)
    }

    fn assert_formatted(
        input: Box<dyn Chunk>,
        first_kw: &str,
        style: IndentStyle,
        expect: &str,
    ) -> std::result::Result<(), Error> {
        let indenter = Indenter::first(style, INDENT_WIDTH, MAX_WIDTH, first_kw, None);
        let got = input.formatted(FormatContext::SingleLine, 0, &indenter)?;

        let trimmed = expect.trim_start().trim_end();

        if trimmed != got {
            let diff = diff_lines(trimmed, &got);
            diff.names("expect", "got").prettytable();
            print!("[{}]\n[{}]\n", expect, got);
            return Err(anyhow!("did not get expected result"));
        }
        Ok(())
    }
}

// Notes on design:
//
// Original approach was inline string formatting. Very messy, needed to track
// a lot of context in formatter itself as we went. Too many special cases.
//
// Next was to turn an AST into some sort of tree thing with a lot _less_
// detail, but enough to format.
//
// Abstract is a "chunk", which can be things like a single token, a list of
// tokens, a list of chunks, and other specialized things like delimited
// expressions, function calls, etc.
//
// Each chunk has a formatted() method that knows how to format itself. The
// goal is to call formatted() recursively on each chunk that contains other
// chunks until there are no more containers, then assemble this
// backwards. The key chunk is a ChunkList. The idea is that a ChunkList is
// either formatted on a single line, or its broken up across multiple lines,
// one chunk to a line.
//
// Have to be careful when putting chunks in a list. For example, `SELECT
// col1, col2`. It's tempting to do this:

// Statement(
//     [
//         Keyword("SELECT"),
//         [
//             Identifier("col1"),
//             Identifier("col2"),
//         ],
//     ]
// )
//
// But if we had to wrap this we'd end up with this:
//
//     SELECT
//            col1,
//            col2
//
// When what we want is:
//
//     SELECT col1,
//            col2
//
// So the right arrangement of chunks is this:
//
// Statement(
//     [
//         // joiner: comma
//         [
//             // joiner: space
//             Keyword("SELECT"),
//             Identifier("col1"),
//         ],
//         Identifier("col2"),
//     ]
// )

// All wrapped or none?
//
// If a sub-chunk wraps, the container chunk should wrap, and its container
// and so on, up to the top level chunk. This avoids gross things like this:
//
//    SELECT col1, col2, some_function(
//               'long string goes here',
//               'another string goes here'
//           ), col3, col4
//
// Since the function wrapped, we should wrap the entire list of things being
// selected:
//
//    SELECT col1,
//           col2,
//           some_function(
//               'long string goes here',
//               'another string goes here'
//           ),
//           col3,
//           col4
