//! Minimal, dependency-free JSON parser/serializer (JSON Lines protocol).

#[derive(Clone, Debug, PartialEq)]
pub enum Json {
    Null,
    Bool(bool),
    Int(i64),
    Float(f64),
    Str(String),
    Arr(Vec<Json>),
    Obj(Vec<(String, Json)>),
}

#[derive(Debug)]
pub struct JsonError(pub String);

impl std::fmt::Display for JsonError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl Json {
    pub fn parse(text: &str) -> Result<Json, JsonError> {
        let mut p = Parser { bytes: text.as_bytes(), pos: 0 };
        p.skip_ws();
        let v = p.value()?;
        p.skip_ws();
        if p.pos != p.bytes.len() {
            return Err(JsonError(format!("trailing data at byte {}", p.pos)));
        }
        Ok(v)
    }

    pub fn get(&self, key: &str) -> Option<&Json> {
        match self {
            Json::Obj(items) => items.iter().find(|(k, _)| k == key).map(|(_, v)| v),
            _ => None,
        }
    }

    pub fn as_i64(&self) -> Option<i64> {
        match self {
            Json::Int(v) => Some(*v),
            _ => None,
        }
    }

    pub fn as_str(&self) -> Option<&str> {
        match self {
            Json::Str(v) => Some(v.as_str()),
            _ => None,
        }
    }

    /// Serialize into a compact single-line JSON string.
    pub fn write_to(&self, out: &mut String) {
        match self {
            Json::Null => out.push_str("null"),
            Json::Bool(true) => out.push_str("true"),
            Json::Bool(false) => out.push_str("false"),
            Json::Int(v) => out.push_str(&v.to_string()),
            Json::Float(v) => write_float(*v, out),
            Json::Str(s) => write_string(s, out),
            Json::Arr(items) => {
                out.push('[');
                for (i, item) in items.iter().enumerate() {
                    if i > 0 {
                        out.push(',');
                    }
                    item.write_to(out);
                }
                out.push(']');
            }
            Json::Obj(items) => {
                out.push('{');
                for (i, (k, v)) in items.iter().enumerate() {
                    if i > 0 {
                        out.push(',');
                    }
                    write_string(k, out);
                    out.push(':');
                    v.write_to(out);
                }
                out.push('}');
            }
        }
    }

    pub fn to_string_line(&self) -> String {
        let mut s = String::with_capacity(256);
        self.write_to(&mut s);
        s
    }
}

/// Format finite floats in a JSON-safe way (always keep a decimal point).
pub fn write_float(v: f64, out: &mut String) {
    if !v.is_finite() {
        out.push_str("0.0");
        return;
    }
    let mut buf = String::new();
    use std::fmt::Write;
    let _ = write!(buf, "{}", v);
    if buf.contains('.') || buf.contains('e') || buf.contains('E') {
        out.push_str(&buf);
    } else {
        out.push_str(&buf);
        out.push_str(".0");
    }
}

pub fn write_string(s: &str, out: &mut String) {
    out.push('"');
    for ch in s.chars() {
        match ch {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            '\u{08}' => out.push_str("\\b"),
            '\u{0c}' => out.push_str("\\f"),
            c if (c as u32) < 0x20 => {
                use std::fmt::Write;
                let _ = write!(out, "\\u{:04x}", c as u32);
            }
            c => out.push(c),
        }
    }
    out.push('"');
}

struct Parser<'a> {
    bytes: &'a [u8],
    pos: usize,
}

impl<'a> Parser<'a> {
    fn skip_ws(&mut self) {
        while self.pos < self.bytes.len() {
            match self.bytes[self.pos] {
                b' ' | b'\t' | b'\n' | b'\r' => self.pos += 1,
                _ => break,
            }
        }
    }

    fn peek(&self) -> Option<u8> {
        self.bytes.get(self.pos).copied()
    }

    fn expect(&mut self, b: u8) -> Result<(), JsonError> {
        if self.peek() == Some(b) {
            self.pos += 1;
            Ok(())
        } else {
            Err(JsonError(format!("expected byte {} at {}", b as char, self.pos)))
        }
    }

    fn value(&mut self) -> Result<Json, JsonError> {
        self.skip_ws();
        match self.peek() {
            None => Err(JsonError("unexpected end of input".into())),
            Some(b'n') => {
                self.literal("null")?;
                Ok(Json::Null)
            }
            Some(b't') => {
                self.literal("true")?;
                Ok(Json::Bool(true))
            }
            Some(b'f') => {
                self.literal("false")?;
                Ok(Json::Bool(false))
            }
            Some(b'"') => Ok(Json::Str(self.string()?)),
            Some(b'[') => {
                self.pos += 1;
                let mut items = Vec::new();
                self.skip_ws();
                if self.peek() == Some(b']') {
                    self.pos += 1;
                    return Ok(Json::Arr(items));
                }
                loop {
                    items.push(self.value()?);
                    self.skip_ws();
                    match self.peek() {
                        Some(b',') => {
                            self.pos += 1;
                        }
                        Some(b']') => {
                            self.pos += 1;
                            return Ok(Json::Arr(items));
                        }
                        _ => return Err(JsonError(format!("expected , or ] at {}", self.pos))),
                    }
                }
            }
            Some(b'{') => {
                self.pos += 1;
                let mut items: Vec<(String, Json)> = Vec::new();
                self.skip_ws();
                if self.peek() == Some(b'}') {
                    self.pos += 1;
                    return Ok(Json::Obj(items));
                }
                loop {
                    self.skip_ws();
                    let key = self.string()?;
                    self.skip_ws();
                    self.expect(b':')?;
                    let value = self.value()?;
                    items.push((key, value));
                    self.skip_ws();
                    match self.peek() {
                        Some(b',') => {
                            self.pos += 1;
                        }
                        Some(b'}') => {
                            self.pos += 1;
                            return Ok(Json::Obj(items));
                        }
                        _ => return Err(JsonError(format!("expected , or }} at {}", self.pos))),
                    }
                }
            }
            Some(_) => self.number(),
        }
    }

    fn literal(&mut self, lit: &str) -> Result<(), JsonError> {
        let end = self.pos + lit.len();
        if end <= self.bytes.len() && &self.bytes[self.pos..end] == lit.as_bytes() {
            self.pos = end;
            Ok(())
        } else {
            Err(JsonError(format!("invalid literal at {}", self.pos)))
        }
    }

    fn string(&mut self) -> Result<String, JsonError> {
        self.expect(b'"')?;
        let mut out = String::new();
        loop {
            let b = self.peek().ok_or_else(|| JsonError("unterminated string".into()))?;
            self.pos += 1;
            match b {
                b'"' => return Ok(out),
                b'\\' => {
                    let esc = self.peek().ok_or_else(|| JsonError("bad escape".into()))?;
                    self.pos += 1;
                    match esc {
                        b'"' => out.push('"'),
                        b'\\' => out.push('\\'),
                        b'/' => out.push('/'),
                        b'b' => out.push('\u{08}'),
                        b'f' => out.push('\u{0c}'),
                        b'n' => out.push('\n'),
                        b'r' => out.push('\r'),
                        b't' => out.push('\t'),
                        b'u' => {
                            let cp = self.hex4()?;
                            if (0xD800..0xDC00).contains(&cp) {
                                // surrogate pair
                                if self.peek() == Some(b'\\') {
                                    self.pos += 1;
                                    if self.peek() == Some(b'u') {
                                        self.pos += 1;
                                        let low = self.hex4()?;
                                        if (0xDC00..0xE000).contains(&low) {
                                            let c = 0x10000 + ((cp - 0xD800) << 10) + (low - 0xDC00);
                                            out.push(char::from_u32(c).unwrap_or('\u{FFFD}'));
                                        } else {
                                            out.push('\u{FFFD}');
                                        }
                                    } else {
                                        out.push('\u{FFFD}');
                                    }
                                } else {
                                    out.push('\u{FFFD}');
                                }
                            } else {
                                out.push(char::from_u32(cp).unwrap_or('\u{FFFD}'));
                            }
                        }
                        _ => return Err(JsonError(format!("bad escape byte {}", esc))),
                    }
                }
                _ => {
                    // UTF-8 continuation: copy raw bytes
                    let start = self.pos - 1;
                    let len = utf8_len(b);
                    let end = start + len;
                    if end > self.bytes.len() {
                        return Err(JsonError("truncated utf-8".into()));
                    }
                    match std::str::from_utf8(&self.bytes[start..end]) {
                        Ok(s) => {
                            out.push_str(s);
                            self.pos = end;
                        }
                        Err(_) => return Err(JsonError("invalid utf-8".into())),
                    }
                }
            }
        }
    }

    fn hex4(&mut self) -> Result<u32, JsonError> {
        if self.pos + 4 > self.bytes.len() {
            return Err(JsonError("truncated unicode escape".into()));
        }
        let mut v = 0u32;
        for i in 0..4 {
            let b = self.bytes[self.pos + i];
            let d = match b {
                b'0'..=b'9' => (b - b'0') as u32,
                b'a'..=b'f' => (b - b'a' + 10) as u32,
                b'A'..=b'F' => (b - b'A' + 10) as u32,
                _ => return Err(JsonError("bad hex digit".into())),
            };
            v = (v << 4) | d;
        }
        self.pos += 4;
        Ok(v)
    }

    fn number(&mut self) -> Result<Json, JsonError> {
        let start = self.pos;
        if self.peek() == Some(b'-') {
            self.pos += 1;
        }
        while let Some(b) = self.peek() {
            match b {
                b'0'..=b'9' => self.pos += 1,
                _ => break,
            }
        }
        let mut is_float = false;
        if self.peek() == Some(b'.') {
            is_float = true;
            self.pos += 1;
            while let Some(b) = self.peek() {
                match b {
                    b'0'..=b'9' => self.pos += 1,
                    _ => break,
                }
            }
        }
        if let Some(b'e') | Some(b'E') = self.peek() {
            is_float = true;
            self.pos += 1;
            if let Some(b'+') | Some(b'-') = self.peek() {
                self.pos += 1;
            }
            while let Some(b) = self.peek() {
                match b {
                    b'0'..=b'9' => self.pos += 1,
                    _ => break,
                }
            }
        }
        let text = std::str::from_utf8(&self.bytes[start..self.pos])
            .map_err(|_| JsonError("invalid number".into()))?;
        if text.is_empty() || text == "-" {
            return Err(JsonError(format!("invalid number at {}", start)));
        }
        if !is_float {
            if let Ok(v) = text.parse::<i64>() {
                return Ok(Json::Int(v));
            }
        }
        let v: f64 = text.parse().map_err(|_| JsonError(format!("invalid number {}", text)))?;
        if !v.is_finite() {
            return Err(JsonError("non-finite number".into()));
        }
        Ok(Json::Float(v))
    }
}

fn utf8_len(b: u8) -> usize {
    if b < 0x80 {
        1
    } else if b >> 5 == 0b110 {
        2
    } else if b >> 4 == 0b1110 {
        3
    } else {
        4
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrip_basic() {
        let v = Json::parse(r#"{"id":1,"op":"execute","sql":"SELECT 1;","search_mode":"exact"}"#).unwrap();
        assert_eq!(v.get("id").and_then(|x| x.as_i64()), Some(1));
        assert_eq!(v.get("sql").and_then(|x| x.as_str()), Some("SELECT 1;"));
    }

    #[test]
    fn big_int_precision() {
        let v = Json::parse("9007199254740993").unwrap();
        assert_eq!(v, Json::Int(9007199254740993));
    }

    #[test]
    fn escapes_and_unicode() {
        let v = Json::parse(r#""a\"b\\c\u4e2d\u6587""#).unwrap();
        assert_eq!(v.as_str().unwrap(), "a\"b\\c中文");
    }

    #[test]
    fn float_output() {
        let mut s = String::new();
        Json::Float(5.0).write_to(&mut s);
        assert_eq!(s, "5.0");
        let mut s2 = String::new();
        Json::Float(0.0).write_to(&mut s2);
        assert_eq!(s2, "0.0");
    }

    #[test]
    fn nested() {
        let v = Json::parse(r#"{"rows":[[1,"x"],[2,null]],"columns":["a","b"]}"#).unwrap();
        assert_eq!(v.to_string_line(), r#"{"rows":[[1,"x"],[2,null]],"columns":["a","b"]}"#);
    }
}