//! Pure expression evaluation, value coercion and row comparison helpers.

use crate::sql::ast::{BinOp, Expr, OrderKey, SelectItem};
use crate::types::{ColType, DbError, DbResult, Row, TableDef, Value};

/// Resolve a column name to its index in the table.
pub fn resolve_column(table: &TableDef, name: &str) -> DbResult<usize> {
    table
        .column_index(name)
        .ok_or_else(|| DbError::new(crate::types::ERR_COLUMN_NOT_FOUND, format!("unknown column {}", name)))
}

/// Coerce an evaluated literal to the storage type of a column.
pub fn coerce_to_column(ty: &ColType, value: Value, column: &str) -> DbResult<Value> {
    match ty {
        ColType::BigInt => match value {
            Value::Int(v) => Ok(Value::Int(v)),
            Value::Float(f) if f.fract() == 0.0 && f.is_finite() => Ok(Value::Int(f as i64)),
            Value::Str(s) => s
                .trim()
                .parse::<i64>()
                .map(Value::Int)
                .map_err(|_| DbError::new(crate::types::ERR_TYPE_MISMATCH, format!("invalid BIGINT for column {}: {}", column, s))),
            other => Err(DbError::new(
                crate::types::ERR_TYPE_MISMATCH,
                format!("cannot store {:?} into BIGINT column {}", other, column),
            )),
        },
        ColType::Varchar(limit) => {
            let text = match value {
                Value::Str(s) => s,
                Value::Int(v) => v.to_string(),
                Value::Float(f) => f.to_string(),
                other => {
                    return Err(DbError::new(
                        crate::types::ERR_TYPE_MISMATCH,
                        format!("cannot store {:?} into VARCHAR column {}", other, column),
                    ))
                }
            };
            // VARCHAR(n) counts Unicode scalar values, not bytes.
            let chars = text.chars().count();
            if chars > *limit as usize {
                return Err(DbError::new(
                    crate::types::ERR_STRING_TOO_LONG,
                    format!(
                        "value of {} characters exceeds VARCHAR({}) for column {}",
                        chars, limit, column
                    ),
                ));
            }
            Ok(Value::Str(text))
        }
        ColType::Vector(dim) => {
            let parsed = match value {
                Value::Str(s) => crate::vector::parse_vector(&s)?,
                Value::Vec(v) => v,
                other => {
                    return Err(DbError::new(
                        crate::types::ERR_INVALID_VECTOR,
                        format!("cannot store {:?} into VECTOR column {}", other, column),
                    ))
                }
            };
            crate::vector::validate_dim(&parsed, *dim as usize)?;
            Ok(Value::Vec(parsed))
        }
    }
}

/// Evaluate an expression against a row (or against no row for constant expressions).
pub fn eval(expr: &Expr, table: &TableDef, row: Option<&Row>) -> DbResult<Value> {
    match expr {
        Expr::Lit(v) => Ok(v.clone()),
        Expr::Column(name) => {
            let idx = resolve_column(table, name)?;
            match row {
                Some(r) => Ok(r.get(idx).cloned().unwrap_or(Value::Null)),
                None => Err(DbError::new(
                    crate::types::ERR_COLUMN_NOT_FOUND,
                    format!("column {} is not available in this context", name),
                )),
            }
        }
        Expr::L2Distance { column, query } => {
            let idx = resolve_column(table, column)?;
            let dim = table.columns[idx]
                .ty
                .vector_dim()
                .ok_or_else(|| DbError::new(crate::types::ERR_TYPE_MISMATCH, format!("{} is not a VECTOR column", column)))?;
            let stored = match row {
                Some(r) => match r.get(idx) {
                    Some(Value::Vec(v)) => v.clone(),
                    Some(Value::Null) | None => {
                        return Err(DbError::new(crate::types::ERR_INVALID_VECTOR, "vector column contains NULL"))
                    }
                    _ => return Err(DbError::new(crate::types::ERR_INVALID_VECTOR, "vector column is not a vector")),
                },
                None => return Err(DbError::new(crate::types::ERR_COLUMN_NOT_FOUND, "vector column unavailable")),
            };
            let query_value = eval(query.as_ref(), table, row)?;
            let query_vec = match query_value {
                Value::Str(s) => crate::vector::parse_vector(&s)?,
                Value::Vec(v) => v,
                other => {
                    return Err(DbError::new(
                        crate::types::ERR_INVALID_VECTOR,
                        format!("invalid L2_DISTANCE query argument {:?}", other),
                    ))
                }
            };
            crate::vector::validate_dim(&stored, dim)?;
            crate::vector::validate_dim(&query_vec, dim)?;
            Ok(Value::Float(crate::vector::l2_distance(&stored, &query_vec)))
        }
        Expr::Unary { negate, expr } => {
            let v = eval(expr, table, row)?;
            if *negate {
                match v {
                    Value::Int(i) => Ok(Value::Int(-i)),
                    Value::Float(f) => Ok(Value::Float(-f)),
                    other => Err(DbError::new(crate::types::ERR_TYPE_MISMATCH, format!("cannot negate {:?}", other))),
                }
            } else {
                Ok(v)
            }
        }
        Expr::Binary { op, left, right } => {
            if *op == BinOp::And {
                let l = eval(left, table, row)?;
                if !truthy(&l) {
                    return Ok(Value::Int(0));
                }
                let r = eval(right, table, row)?;
                return Ok(Value::Int(if truthy(&r) { 1 } else { 0 }));
            }
            if *op == BinOp::Or {
                let l = eval(left, table, row)?;
                if truthy(&l) {
                    return Ok(Value::Int(1));
                }
                let r = eval(right, table, row)?;
                return Ok(Value::Int(if truthy(&r) { 1 } else { 0 }));
            }
            let l = eval(left, table, row)?;
            let r = eval(right, table, row)?;
            apply_binary(*op, &l, &r)
        }
        Expr::Unsupported(name) => Err(DbError::unsupported(format!("unsupported function {}", name))),
    }
}

pub fn truthy(v: &Value) -> bool {
    match v {
        Value::Null => false,
        Value::Int(i) => *i != 0,
        Value::Float(f) => *f != 0.0,
        Value::Str(s) => !s.is_empty(),
        Value::Vec(v) => !v.is_empty(),
    }
}

fn apply_binary(op: BinOp, l: &Value, r: &Value) -> DbResult<Value> {
    match op {
        BinOp::And | BinOp::Or => unreachable!(),
        BinOp::Eq | BinOp::Ne | BinOp::Lt | BinOp::Le | BinOp::Gt | BinOp::Ge => {
            let cmp = compare_values(l, r);
            let result = match cmp {
                None => return Ok(Value::Int(0)),
                Some(ord) => match op {
                    BinOp::Eq => ord == std::cmp::Ordering::Equal,
                    BinOp::Ne => ord != std::cmp::Ordering::Equal,
                    BinOp::Lt => ord == std::cmp::Ordering::Less,
                    BinOp::Le => ord != std::cmp::Ordering::Greater,
                    BinOp::Gt => ord == std::cmp::Ordering::Greater,
                    BinOp::Ge => ord != std::cmp::Ordering::Less,
                    _ => unreachable!(),
                },
            };
            Ok(Value::Int(if result { 1 } else { 0 }))
        }
        BinOp::Add | BinOp::Sub | BinOp::Mul | BinOp::Div => {
            let a = l.as_f64();
            let b = r.as_f64();
            match (a, b) {
                (Some(a), Some(b)) => {
                    if matches!(l, Value::Int(_)) && matches!(r, Value::Int(_)) && op != BinOp::Div {
                        let ai = l.as_i64().unwrap();
                        let bi = r.as_i64().unwrap();
                        let v = match op {
                            BinOp::Add => ai.wrapping_add(bi),
                            BinOp::Sub => ai.wrapping_sub(bi),
                            BinOp::Mul => ai.wrapping_mul(bi),
                            _ => unreachable!(),
                        };
                        Ok(Value::Int(v))
                    } else {
                        let v = match op {
                            BinOp::Add => a + b,
                            BinOp::Sub => a - b,
                            BinOp::Mul => a * b,
                            BinOp::Div => a / b,
                            _ => unreachable!(),
                        };
                        Ok(Value::Float(v))
                    }
                }
                _ => Err(DbError::new(crate::types::ERR_TYPE_MISMATCH, "arithmetic on non-numeric values")),
            }
        }
    }
}

/// Compare two values. Returns None when either side is NULL (SQL semantics).
pub fn compare_values(l: &Value, r: &Value) -> Option<std::cmp::Ordering> {
    match (l, r) {
        (Value::Null, _) | (_, Value::Null) => None,
        (Value::Int(a), Value::Int(b)) => Some(a.cmp(b)),
        (Value::Str(a), Value::Str(b)) => Some(a.cmp(b)),
        (Value::Vec(a), Value::Vec(b)) => Some(a.len().cmp(&b.len())),
        _ => {
            let a = l.as_f64();
            let b = r.as_f64();
            match (a, b) {
                (Some(a), Some(b)) => a.partial_cmp(&b),
                _ => None,
            }
        }
    }
}

/// Total ordering used for ORDER BY; NULLS sort first, vectors are not orderable.
pub fn order_cmp(l: &Value, r: &Value) -> std::cmp::Ordering {
    match (l, r) {
        (Value::Null, Value::Null) => std::cmp::Ordering::Equal,
        (Value::Null, _) => std::cmp::Ordering::Less,
        (_, Value::Null) => std::cmp::Ordering::Greater,
        _ => compare_values(l, r).unwrap_or(std::cmp::Ordering::Equal),
    }
}

/// Output column name for a projection.
pub fn projection_name(item: &SelectItem, table: &TableDef) -> DbResult<String> {
    match item {
        SelectItem::Star => Ok("*".to_string()),
        SelectItem::Column { name } => {
            let idx = resolve_column(table, name)?;
            Ok(table.columns[idx].name.clone())
        }
        SelectItem::Expr { expr, alias } => {
            if !alias.is_empty() {
                return Ok(alias.clone());
            }
            Ok(match expr {
                Expr::Column(name) => {
                    let idx = resolve_column(table, name)?;
                    table.columns[idx].name.clone()
                }
                Expr::L2Distance { column, .. } => format!("L2_DISTANCE({})", column),
                Expr::Lit(_) => "literal".to_string(),
                _ => "expr".to_string(),
            })
        }
    }
}

/// Expand '*' into the table columns.
pub fn expand_items(items: &[SelectItem], table: &TableDef) -> Vec<SelectItem> {
    let mut out = Vec::new();
    for item in items {
        match item {
            SelectItem::Star => {
                for c in &table.columns {
                    out.push(SelectItem::Column { name: c.name.clone() });
                }
            }
            other => out.push(other.clone()),
        }
    }
    out
}

/// Resolve ORDER BY expressions that alias a projected expression.
pub fn order_value(
    key: &OrderKey,
    table: &TableDef,
    row: &Row,
    items: &[SelectItem],
    projected: &[Value],
) -> DbResult<Value> {
    // ORDER BY <ordinal> refers to the 1-based position in the SELECT list.
    if let Expr::Lit(Value::Int(position)) = &key.expr {
        let index = *position;
        if index >= 1 && (index as usize) <= items.len() {
            if let Some(v) = projected.get(index as usize - 1) {
                return Ok(v.clone());
            }
        }
    }
    if let Expr::Column(name) = &key.expr {
        for (i, item) in items.iter().enumerate() {
            if let SelectItem::Expr { alias, .. } = item {
                if !alias.is_empty() && alias.eq_ignore_ascii_case(name) {
                    if let Some(v) = projected.get(i) {
                        return Ok(v.clone());
                    }
                }
            }
        }
    }
    eval(&key.expr, table, Some(row))
}