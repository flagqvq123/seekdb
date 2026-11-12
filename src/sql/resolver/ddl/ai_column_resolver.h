#pragma once

#include "share/schema/ob_table_schema.h"
#include "sql/resolver/expr/ob_raw_expr_util.h"

namespace oceanbase
{
namespace sql
{
class AIColumnResolver
{
public:
  static int resolve_definition(common::ObIAllocator &allocator,
                                share::schema::ObColumnSchemaV2 &column,
                                const ParseNode &expression,
                                const ParseNode *options);
  static int finish_table(ObRawExprFactory &factory, const ObSQLSessionInfo &session,
                          const ObSchemaChecker *checker, share::schema::ObTableSchema &table);
  static int preserve_hidden_columns(share::schema::ObColumnSchemaV2 &column,
                                     const share::schema::ObColumnSchemaV2 &previous);
};
}
}
