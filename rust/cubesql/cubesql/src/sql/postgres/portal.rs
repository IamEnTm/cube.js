use datafusion::logical_plan::right;
use sqlparser::ast;

#[derive(Debug)]
enum PlaceholderValue {
    String(String),
    Int64(i64),
    UInt64(u64),
    Bool(bool),
}

#[derive(Debug)]
struct StatementBinder {
    position: usize,
    values: Vec<PlaceholderValue>,
}

trait Visitor<'ast> {
    fn visit_value(&mut self, val: &mut ast::Value) {}

    fn visit_identifier(&mut self, identifier: &mut ast::Ident) {}

    fn visit_expr(&mut self, expr: &mut ast::Expr) {
        match expr {
            ast::Expr::Value(value) => self.visit_value(value),
            ast::Expr::Identifier(identifier) => self.visit_identifier(identifier),
            ast::Expr::Nested(v) => self.visit_expr(&mut *v),
            ast::Expr::Between {
                expr,
                negated,
                low,
                high,
            } => {
                self.visit_expr(&mut *expr);
                self.visit_expr(&mut *low);
                self.visit_expr(&mut *high);
            }
            ast::Expr::BinaryOp { left, op, right } => {
                self.visit_expr(&mut *left);
                self.visit_expr(&mut *right);
            }
            _ => {}
        }
    }

    fn visit_table_factor(&mut self, factor: &mut ast::TableFactor) {
        match factor {
            ast::TableFactor::Derived { subquery, .. } => {
                self.visit_query(subquery);
            }
            _ => {}
        }
    }

    fn visit_join(&mut self, join: &mut ast::Join) {
        self.visit_table_factor(&mut join.relation);
    }

    fn visit_table_with_joins(&mut self, twj: &mut ast::TableWithJoins) {
        self.visit_table_factor(&mut twj.relation);

        for join in &mut twj.joins {
            self.visit_join(join);
        }
    }

    fn visit_select(&mut self, select: &mut Box<ast::Select>) {
        if let Some(selection) = &mut select.selection {
            self.visit_expr(selection);
        };

        for from in &mut select.from {
            self.visit_table_with_joins(from);
        }
    }

    fn visit_set_expr(&mut self, body: &mut ast::SetExpr) {
        match body {
            ast::SetExpr::Select(select) => self.visit_select(select),
            ast::SetExpr::Query(query) => self.visit_query(query),
            ast::SetExpr::SetOperation { left, right, .. } => {
                self.visit_set_expr(&mut *left);
                self.visit_set_expr(&mut *right);
            }
            _ => {}
        }
    }

    fn visit_query(&mut self, query: &mut Box<ast::Query>) {
        self.visit_set_expr(&mut query.body);
    }

    fn visit_statement(&mut self, statement: &mut ast::Statement) {
        match statement {
            ast::Statement::Query(query) => self.visit_query(query),
            _ => {}
        }
    }
}

impl StatementBinder {
    pub fn new(values: Vec<PlaceholderValue>) -> Self {
        Self {
            position: 0,
            values,
        }
    }

    pub fn bind(&mut self, stmt: &mut ast::Statement) {
        self.visit_statement(stmt);
    }
}

impl<'ast> Visitor<'ast> for StatementBinder {
    fn visit_value(&mut self, value: &mut ast::Value) {
        match &value {
            ast::Value::Placeholder(_) => {
                let to_replace = self.values.get(self.position).expect("unexpected");
                self.position += 1;

                match to_replace {
                    PlaceholderValue::String(v) => {
                        *value = ast::Value::SingleQuotedString(v.clone());
                    }
                    PlaceholderValue::Bool(v) => {
                        *value = ast::Value::Boolean(*v);
                    }
                    PlaceholderValue::UInt64(v) => {
                        *value = ast::Value::Number(v.to_string(), false);
                    }
                    PlaceholderValue::Int64(v) => {
                        *value = ast::Value::Number(v.to_string(), *v < 0_i64);
                    }
                }
            }
            _ => {}
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::CubeError;
    use sqlparser::{ast, dialect::PostgreSqlDialect, parser::Parser};

    fn test_binder(
        input: &str,
        output: &str,
        values: Vec<PlaceholderValue>,
    ) -> Result<(), CubeError> {
        let stmts = Parser::parse_sql(&PostgreSqlDialect {}, &input).unwrap();

        let mut binder = StatementBinder::new(values);
        let mut input = stmts[0].clone();
        binder.bind(&mut input);

        assert_eq!(input.to_string(), output);

        Ok(())
    }

    #[test]
    fn test_binder_named() -> Result<(), CubeError> {
        test_binder(
            r#"
                SELECT *
                FROM testdata
                WHERE fieldA = $1 AND fieldB = $2
            "#,
            "SELECT * FROM testdata WHERE fieldA = 'test' AND fieldB = 1",
            vec![
                PlaceholderValue::String("test".to_string()),
                PlaceholderValue::Int64(1),
            ],
        )?;

        test_binder(
            r#"
                SELECT *
                FROM testdata
                WHERE fieldA = $1 OR fieldB = $2
            "#,
            "SELECT * FROM testdata WHERE fieldA = 'test1' OR fieldB = 'test2'",
            vec![
                PlaceholderValue::String("test1".to_string()),
                PlaceholderValue::String("test2".to_string()),
            ],
        )?;

        test_binder(
            r#"
                SELECT *
                FROM testdata
                WHERE fieldA BETWEEN $1 AND $2
            "#,
            "SELECT * FROM testdata WHERE fieldA BETWEEN 'test1' AND 'test2'",
            vec![
                PlaceholderValue::String("test1".to_string()),
                PlaceholderValue::String("test2".to_string()),
            ],
        )?;

        test_binder(
            r#"
                SELECT *
                FROM testdata
                WHERE fieldA = $1
                UNION ALL
                SELECT *
                FROM testdata
                WHERE fieldA = $2
            "#,
            "SELECT * FROM testdata WHERE fieldA = 'test1' UNION ALL SELECT * FROM testdata WHERE fieldA = 'test2'",
            vec![
                PlaceholderValue::String(
                    "test1".to_string(),
                ),
                PlaceholderValue::String(
                    "test2".to_string(),
                ),
            ]
        )?;

        test_binder(
            r#"
                SELECT * FROM (
                    SELECT *
                    FROM testdata
                    WHERE fieldA = $1
                )
            "#,
            "SELECT * FROM (SELECT * FROM testdata WHERE fieldA = 'test1')",
            vec![PlaceholderValue::String("test1".to_string())],
        )?;

        Ok(())
    }
}
