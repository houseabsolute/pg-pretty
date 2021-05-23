use log::debug;
use std::fmt;
use strum_macros::Display;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum FormatterError {
    #[error("{what} does not have any chunks")]
    NoChunks { what: &'static str },
    #[error("InsertValues does not have any values")]
    NoValues,
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

#[derive(Clone, Copy, Debug, PartialEq)]
pub enum Joiner {
    Comma,
    None,
    Period,
    Space,
}

#[derive(Clone, Copy, Debug, PartialEq)]
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
        len: usize,
        indenter: &Indenter,
    ) -> Result<String, FormatterError>;

    fn first_keyword(&self) -> Option<&str>;
}

#[derive(Debug)]
pub enum Token {
    // SELECT, CREATE, etc.
    Keyword(String),
    // Column, table, type, etc. names
    Name(String),
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

    pub fn new_name(name: &str) -> Self {
        Token::Name(name.into())
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
            Token::Keyword(s) => &s,
            Token::Name(s) => &s,
            Token::StringConst(s) => &s,
            Token::Number(s) => &s,
            Token::Operator(s) => &s,
        }
    }
}

impl Chunk for Token {
    fn formatted(
        &self,
        _: FormatContext,
        _: usize,
        _: &Indenter,
    ) -> Result<String, FormatterError> {
        let t = match self {
            Token::Keyword(s) => s.to_uppercase(),
            Token::Name(s) => {
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

    fn first_keyword(&self) -> Option<&str> {
        match self {
            Token::Keyword(s) => Some(&s),
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

    pub fn push_name<S: AsRef<str>>(&mut self, name: S) {
        self.tokens.push(Token::new_name(name.as_ref()));
    }

    pub fn push_operator(&mut self, operator: &str) {
        self.tokens.push(Token::new_operator(operator));
    }
}

impl Chunk for Tokens {
    fn formatted(
        &self,
        ctxt: FormatContext,
        _: usize,
        indenter: &Indenter,
    ) -> Result<String, FormatterError> {
        let mut out = start_from_context(ctxt);
        out.push_str(
            &self
                .tokens
                .iter()
                .map(|t| t.formatted(ctxt, 0, indenter))
                .collect::<Result<Vec<_>, _>>()?
                .join(self.joiner.single_line_joiner()),
        );
        Ok(out)
    }

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

    pub fn push_name<S: AsRef<str>>(&mut self, name: S) {
        self.chunks.push(Box::new(Token::new_name(name.as_ref())));
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
        len: usize,
        indenter: &Indenter,
    ) -> Result<String, FormatterError> {
        if self.is_empty() {
            return Err(FormatterError::NoChunks { what: "ChunkList" });
        }

        let mut out = start_from_context(ctxt);
        out.push_str(
            &self
                .chunks
                .iter()
                .map(|c| c.formatted(FormatContext::SingleLine, len, indenter))
                .collect::<Result<Vec<_>, _>>()?
                .join(self.joiner.single_line_joiner()),
        );

        // If one of the chunks contains newlines we only want to look at the
        // length of the first line? I suspect there's a better way to
        // approach this.
        if len + out.find('\n').unwrap_or(out.len()) <= indenter.max_line_len {
            return Ok(out);
        }

        let indenter = indenter.next_indenter();
        out = start_from_context(ctxt);
        out.push_str(
            &self
                .chunks
                .iter()
                .map(|c| c.formatted(FormatContext::MultiLine, len, &indenter))
                .collect::<Result<Vec<_>, _>>()?
                .join(self.joiner.multi_line_joiner()),
        );

        Ok(out)
    }

    fn first_keyword(&self) -> Option<&str> {
        self.chunks[0].first_keyword()
    }
}

#[derive(Debug)]
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

pub trait Delimited {
    fn format_args_single_line(
        &self,
        len: usize,
        indenter: &Indenter,
        prefix: &[Box<dyn Chunk>],
        args: &[Box<dyn Chunk>],
        suffix: &[Box<dyn Chunk>],
        joiner: &Joiner,
        delimiter: &Delimiter,
    ) -> Result<String, FormatterError> {
        let (p, a, s) = self.formatted_chunks(
            FormatContext::SingleLine,
            len,
            indenter,
            prefix,
            args,
            suffix,
        )?;
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
        len: usize,
        indenter: &Indenter,
        prefix: &[Box<dyn Chunk>],
        args: &[Box<dyn Chunk>],
        suffix: &[Box<dyn Chunk>],
        joiner: &Joiner,
        delimiter: &Delimiter,
    ) -> Result<String, FormatterError> {
        let (p, a, s) = self.formatted_chunks(
            FormatContext::MultiLine,
            len,
            &indenter,
            prefix,
            args,
            suffix,
        )?;
        let args = a
            .iter()
            // We will remove any leading newlines and add trailing ones
            // instead.
            .map(|a| indenter.indent(a.trim_start()))
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
            right_delim = right_delim,
        ))
    }

    fn formatted_chunks(
        &self,
        ctxt: FormatContext,
        len: usize,
        indenter: &Indenter,
        prefix: &[Box<dyn Chunk>],
        args: &[Box<dyn Chunk>],
        suffix: &[Box<dyn Chunk>],
    ) -> Result<(String, Vec<String>, String), FormatterError> {
        let p = prefix
            .iter()
            .map(|c| c.formatted(ctxt, len, indenter))
            .collect::<Result<Vec<_>, _>>()?
            .join(" ");

        let a = args
            .iter()
            .map(|c| c.formatted(ctxt, len, indenter))
            .collect::<Result<Vec<_>, _>>()?;

        let s = suffix
            .iter()
            .map(|c| c.formatted(ctxt, len, indenter))
            .collect::<Result<Vec<_>, _>>()?
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
    prefix: Vec<Box<dyn Chunk>>,
    args: Vec<Box<dyn Chunk>>,
    suffix: Vec<Box<dyn Chunk>>,
    joiner: Joiner,
}

impl DelimitedExpression {
    pub fn new(delimiter: Delimiter, joiner: Joiner) -> Self {
        Self {
            delimiter,
            prefix: vec![],
            args: vec![],
            suffix: vec![],
            joiner,
        }
    }

    pub fn push_prefix_chunk(&mut self, chunk: Box<dyn Chunk>) {
        self.prefix.push(chunk);
    }

    pub fn push_chunk(&mut self, chunk: Box<dyn Chunk>) {
        self.args.push(chunk);
    }

    pub fn push_keyword(&mut self, kw: &str) {
        self.args.push(Box::new(Token::new_keyword(kw)));
    }

    pub fn push_name<S: AsRef<str>>(&mut self, name: S) {
        self.args.push(Box::new(Token::new_name(name.as_ref())));
    }

    pub fn push_operator(&mut self, op: &str) {
        self.args.push(Box::new(Token::new_operator(op)));
    }

    pub fn push_suffix_chunk(&mut self, chunk: Box<dyn Chunk>) {
        self.suffix.push(chunk);
    }
}

impl Delimited for DelimitedExpression {}

impl Chunk for DelimitedExpression {
    fn formatted(
        &self,
        ctxt: FormatContext,
        len: usize,
        indenter: &Indenter,
    ) -> Result<String, FormatterError> {
        if self.args.is_empty() {
            return Err(FormatterError::NoChunks {
                what: "DelimitedExpression",
            });
        }

        let single = self.format_args_single_line(
            len,
            indenter,
            &self.prefix,
            &self.args,
            &self.suffix,
            &self.joiner,
            &self.delimiter,
        )?;

        if len + single.len() <= indenter.max_line_len {
            return Ok(single);
        }

        let next = indenter.next_indenter();
        self.format_args_multi_line(
            len,
            &next,
            &self.prefix,
            &self.args,
            &self.suffix,
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
    prefix: Vec<Box<dyn Chunk>>,
    args: Vec<Box<dyn Chunk>>,
    suffix: Vec<Box<dyn Chunk>>,
}

impl Func {
    pub fn new(name: Tokens) -> Self {
        Self {
            name,
            prefix: vec![],
            args: vec![],
            suffix: vec![],
        }
    }

    pub fn push_prefix_chunk(&mut self, chunk: Box<dyn Chunk>) {
        self.prefix.push(chunk);
    }

    pub fn push_chunk(&mut self, chunk: Box<dyn Chunk>) {
        self.args.push(chunk);
    }

    pub fn push_suffix_chunk(&mut self, chunk: Box<dyn Chunk>) {
        self.suffix.push(chunk);
    }
}

impl Delimited for Func {}

impl Chunk for Func {
    fn formatted(
        &self,
        ctxt: FormatContext,
        len: usize,
        indenter: &Indenter,
    ) -> Result<String, FormatterError> {
        if self.args.is_empty() {
            return Err(FormatterError::NoChunks { what: "Func" });
        }

        let name = self.name.formatted(ctxt, len, indenter)?;
        let single = self.format_args_single_line(
            len,
            indenter,
            &self.prefix,
            &self.args,
            &self.suffix,
            &Joiner::Comma,
            &Delimiter::Paren,
        )?;

        if len + name.len() + single.len() <= indenter.max_line_len {
            let mut out = name.clone();
            out.push_str(&single);
            return Ok(out);
        }

        let next = indenter.next_indenter();
        let mut out = name;
        out.push_str(&self.format_args_multi_line(
            len,
            &next,
            &self.prefix,
            &self.args,
            &self.suffix,
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
    pub fn new(stmt: Statement, requires_parens: bool) -> Self {
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
        len: usize,
        indenter: &Indenter,
    ) -> Result<String, FormatterError> {
        let mut out = String::new();
        if !self.is_inline {
            out.push_str("(");
        }
        out.push('\n');

        let mut next = Indenter::first(
            IndentStyle::Gutter,
            indenter.indent_width,
            indenter.max_line_len,
            self.stmt.first_keyword().unwrap(),
            None,
        );

        if !self.is_inline {
            next.current += indenter.current + indenter.indent_width;
        }
        out.push_str(&self.stmt.formatted(ctxt, len, &next)?);

        // The sub-statement will already have a trailing newline.
        if !self.is_inline {
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
    left: Box<dyn Chunk>,
    bool_op: Token,
    right: Box<dyn Chunk>,
}

impl BoolOpChunk {
    pub fn new(left: Box<dyn Chunk>, bool_op: Token, right: Box<dyn Chunk>) -> Self {
        Self {
            left,
            bool_op,
            right,
        }
    }
}

impl Chunk for BoolOpChunk {
    fn formatted(
        &self,
        ctxt: FormatContext,
        len: usize,
        indenter: &Indenter,
    ) -> Result<String, FormatterError> {
        let mut out = start_from_context(ctxt);
        out.push_str(&self.left.formatted(ctxt, len, indenter)?);
        out.push('\n');
        out.push_str(&indenter.indent(self.bool_op.as_str()));
        out.push(' ');
        out.push_str(&self.right.formatted(ctxt, len, indenter)?);
        Ok(out)
    }

    fn first_keyword(&self) -> Option<&str> {
        self.left.first_keyword()
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
        let mut del = DelimitedExpression::new(Delimiter::Paren, Joiner::Comma);
        for c in values {
            del.push_chunk(c);
        }
        self.values.push(del);
    }
}

impl Chunk for InsertValues {
    fn formatted(
        &self,
        _: FormatContext,
        len: usize,
        indenter: &Indenter,
    ) -> Result<String, FormatterError> {
        if self.values.is_empty() {
            return Err(FormatterError::NoValues);
        }

        let mut requires_multi_line = false;
        for v in &self.values {
            let single_line =
                v.formatted(FormatContext::SingleLine, len + "VALUES ".len(), indenter)?;
            if len + single_line.len() > indenter.max_line_len {
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
                (FormatContext::SingleLine, len + prefix.len())
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
        len: usize,
        indenter: &Indenter,
    ) -> Result<String, FormatterError> {
        if self.set_clauses.is_empty() {
            return Err(FormatterError::NoValues);
        }

        let mut formatted_sets: Vec<String> = vec![];
        for (n, s) in self.set_clauses.iter().enumerate() {
            let prefix = if n == 0 { "SET " } else { "" };
            let mut line = format!(
                "{}{}",
                prefix,
                s.formatted(FormatContext::SingleLine, len + prefix.len(), indenter)?,
            );
            // XXX - This is a bit gross, but multi-line chunks need to do
            // their own indenting for all lines but first. I think this needs
            // some reconsideration to make indentation simpler. It should all
            // be done at the same "level". Maybe chunks need to return a Vec
            // of lines rather than strings that might contain newlines?
            if n > 0 {
                line = indenter.indent(line);
            }
            formatted_sets.push(line);
        }

        Ok(format!("\n{}", formatted_sets.join(",\n")))
    }

    fn first_keyword(&self) -> Option<&str> {
        Some("SET")
    }
}

#[derive(Debug)]
pub struct Statement {
    chunks: Vec<Box<dyn Chunk>>,
    max_line_len: usize,
    indent_width: usize,
}

impl Statement {
    pub fn new(max_line_len: usize, indent_width: usize) -> Self {
        Self {
            chunks: vec![],
            max_line_len,
            indent_width,
        }
    }

    fn uses_gutter_indent(&self) -> bool {
        matches!(
            self.first_keyword(),
            Some("SELECT") | Some("UPDATE") | Some("DO UPDATE") | Some("DELETE")
        )
    }

    fn is_insert(&self) -> bool {
        matches!(self.first_keyword(), Some("INSERT"))
    }

    pub fn push_chunk(&mut self, chunk: Box<dyn Chunk>) {
        self.chunks.push(chunk);
    }

    pub fn push_keyword(&mut self, kw: &str) {
        self.chunks.push(Box::new(Token::new_keyword(kw)));
    }

    pub fn as_string(&self, indent: Option<usize>) -> Result<String, FormatterError> {
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

        // The string may start with a newline if this was formatted as a
        // multi-line statement.
        Ok(out)
    }
}

impl Chunk for Statement {
    fn formatted(
        &self,
        ctxt: FormatContext,
        len: usize,
        indenter: &Indenter,
    ) -> Result<String, FormatterError> {
        // DML statements are always formatted with multiple lines.
        if !(self.uses_gutter_indent() || self.is_insert()) {
            let mut out = start_from_context(ctxt);
            out.push_str(
                &indenter.indent(
                    self.chunks
                        .iter()
                        .map(|c| c.formatted(FormatContext::SingleLine, len, &indenter))
                        .collect::<Result<Vec<_>, _>>()?
                        .join(" "),
                ),
            );

            if len + out.len() <= indenter.max_line_len {
                return Ok(out);
            }
        }

        let mut out = start_from_context(ctxt);
        let next_indenter = indenter.next_indenter();

        for (i, c) in self.chunks.iter().enumerate() {
            // When formatting TabStop style (used for DDL statements) we
            // indent all lines after the first.
            let indenter = if i >= 1 && indenter.style == IndentStyle::TabStop {
                debug!("Using next indenter for: {:#?}", c);
                &next_indenter
            } else {
                debug!("Using current indenter for: {:#?}", c);
                indenter
            };

            let formatted = c.formatted(FormatContext::MultiLine, len, &indenter)?;
            if i == 0 {
                out.push_str(formatted.trim_start());
            } else {
                out.push_str(&indenter.indent(&formatted));
            }
        }

        Ok(out)
    }

    fn first_keyword(&self) -> Option<&str> {
        self.chunks[0].first_keyword()
    }
}

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

    fn indent<S: AsRef<str>>(&self, f: S) -> String {
        let mut s = f.as_ref();
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
                    d.push_str("will not indent");
                    // Otherwise we just give up and won't indent this line at
                    // all.
                    0
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
        Self {
            style: IndentStyle::TabStop,
            indent_width: self.indent_width,
            max_line_len: self.max_line_len,
            current: match self.style {
                IndentStyle::Gutter => self.indent_width,
                IndentStyle::Insert | IndentStyle::TabStop => self.current + self.indent_width,
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
