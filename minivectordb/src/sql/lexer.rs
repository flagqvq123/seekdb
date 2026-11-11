//! SQL tokenizer for the competition subset.

use crate::types::{DbError, DbResult};

#[derive(Clone, Debug, PartialEq)]
pub enum Tok {
    Ident(String),
    Number(i64),
    Str(String),
    LParen,
    RParen,
    Comma,
    Semi,
    Dot,
    Eq,
    Ne,
    Lt,
    Le,
    Gt,
    Ge,
    Plus,
    Minus,
    Star,
    Slash,
    Eof,
}

pub fn tokenize(sql: &str) -> DbResult<Vec<Tok>> {
    let chars: Vec<char> = sql.chars().collect();
    let mut out = Vec::new();
    let mut i = 0usize;
    while i < chars.len() {
        let c = chars[i];
        match c {
            ' ' | '\t' | '\r' | '\n' => {
                i += 1;
            }
            '-' if i + 1 < chars.len() && chars[i + 1] == '-' => {
                while i < chars.len() && chars[i] != '\n' {
                    i += 1;
                }
            }
            '#' => {
                while i < chars.len() && chars[i] != '\n' {
                    i += 1;
                }
            }
            '/' if i + 1 < chars.len() && chars[i + 1] == '*' => {
                i += 2;
                while i + 1 < chars.len() && !(chars[i] == '*' && chars[i + 1] == '/') {
                    i += 1;
                }
                i = (i + 2).min(chars.len());
            }
            '(' => {
                out.push(Tok::LParen);
                i += 1;
            }
            ')' => {
                out.push(Tok::RParen);
                i += 1;
            }
            ',' => {
                out.push(Tok::Comma);
                i += 1;
            }
            ';' => {
                out.push(Tok::Semi);
                i += 1;
            }
            '.' => {
                out.push(Tok::Dot);
                i += 1;
            }
            '*' => {
                out.push(Tok::Star);
                i += 1;
            }
            '/' => {
                out.push(Tok::Slash);
                i += 1;
            }
            '+' => {
                out.push(Tok::Plus);
                i += 1;
            }
            '-' => {
                out.push(Tok::Minus);
                i += 1;
            }
            '=' => {
                if i + 1 < chars.len() && chars[i + 1] == '=' {
                    i += 2;
                } else {
                    i += 1;
                }
                out.push(Tok::Eq);
            }
            '!' => {
                if i + 1 < chars.len() && chars[i + 1] == '=' {
                    out.push(Tok::Ne);
                    i += 2;
                } else {
                    return Err(DbError::syntax("unexpected '!'"));
                }
            }
            '<' => {
                if i + 1 < chars.len() && chars[i + 1] == '=' {
                    out.push(Tok::Le);
                    i += 2;
                } else if i + 1 < chars.len() && chars[i + 1] == '>' {
                    out.push(Tok::Ne);
                    i += 2;
                } else {
                    out.push(Tok::Lt);
                    i += 1;
                }
            }
            '>' => {
                if i + 1 < chars.len() && chars[i + 1] == '=' {
                    out.push(Tok::Ge);
                    i += 2;
                } else {
                    out.push(Tok::Gt);
                    i += 1;
                }
            }
            '\'' => {
                let mut s = String::new();
                i += 1;
                loop {
                    if i >= chars.len() {
                        return Err(DbError::syntax("unterminated string literal"));
                    }
                    let ch = chars[i];
                    if ch == '\'' {
                        if i + 1 < chars.len() && chars[i + 1] == '\'' {
                            s.push('\'');
                            i += 2;
                            continue;
                        }
                        i += 1;
                        break;
                    }
                    s.push(ch);
                    i += 1;
                }
                out.push(Tok::Str(s));
            }
            '"' => {
                let mut s = String::new();
                i += 1;
                loop {
                    if i >= chars.len() {
                        return Err(DbError::syntax("unterminated identifier"));
                    }
                    let ch = chars[i];
                    if ch == '"' {
                        i += 1;
                        break;
                    }
                    s.push(ch);
                    i += 1;
                }
                out.push(Tok::Ident(s));
            }
            c if c.is_ascii_digit() => {
                let start = i;
                while i < chars.len() && chars[i].is_ascii_digit() {
                    i += 1;
                }
                if i < chars.len() && chars[i] == '.' && i + 1 < chars.len() && chars[i + 1].is_ascii_digit() {
                    while i < chars.len() && (chars[i].is_ascii_digit() || chars[i] == '.') {
                        i += 1;
                    }
                    let text: String = chars[start..i].iter().collect();
                    return Err(DbError::syntax(format!("real literals are not supported: {}", text)));
                }
                let text: String = chars[start..i].iter().collect();
                match text.parse::<i64>() {
                    Ok(v) => out.push(Tok::Number(v)),
                    Err(_) => return Err(DbError::syntax(format!("integer literal out of range: {}", text))),
                }
            }
            c if c.is_alphabetic() || c == '_' || c == '$' => {
                let start = i;
                while i < chars.len()
                    && (chars[i].is_alphanumeric() || chars[i] == '_' || chars[i] == '$')
                {
                    i += 1;
                }
                let text: String = chars[start..i].iter().collect();
                out.push(Tok::Ident(text));
            }
            other => {
                return Err(DbError::syntax(format!("unexpected character '{}'", other)));
            }
        }
    }
    out.push(Tok::Eof);
    Ok(out)
}
