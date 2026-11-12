#define USING_LOG_PREFIX SQL_RESV
#include "sql/resolver/ddl/ai_column_resolver.h"
#include "sql/engine/expr/ob_expr_ai/ob_ai_func_utils.h"
#include "query/vector/ob_vector_index_util.h"
#include <cctype>

namespace oceanbase
{
using namespace common;
using namespace share::schema;
namespace sql
{
static int check_request_argument(const ObRawExpr &expr, int64_t depth = 0)
{
  int ret = OB_SUCCESS;
  bool non_pure = false;
  if (depth >= 64 || expr.is_udf_expr() || expr.is_query_ref_expr() || expr.is_aggr_expr()
      || expr.is_win_func_expr() || expr.get_expr_type() == T_OP_GET_USER_VAR
      || expr.get_expr_type() == T_OP_ASSIGN) {
    ret = OB_NOT_SUPPORTED;
  } else if (OB_FAIL(expr.is_non_pure_sys_func_expr(non_pure))) {
  } else if (non_pure) {
    ret = OB_NOT_SUPPORTED;
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < expr.get_param_count(); ++i) {
    if (expr.get_param_expr(i) == nullptr) { ret = OB_ERR_UNEXPECTED; }
    else { ret = check_request_argument(*expr.get_param_expr(i), depth + 1); }
  }
  return ret;
}

static int make_request_expr(ObIAllocator &allocator, const ObString &source,
                             const ObIArray<ObQualifiedName> &columns, ObString &result)
{
  int ret = OB_SUCCESS;
  ObStringBuffer output(&allocator);
  int64_t begin = 0;
  while (begin < source.length() && source.ptr()[begin] != '(') { ++begin; }
  if (begin == source.length()) {
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(output.append("JSON_ARRAY("))) {
  } else if (OB_FAIL(output.append(source.ptr() + begin + 1, source.length() - begin - 1))) {
  } else {
    ObString args = output.string();
    ObStringBuffer prefixed(&allocator);
    for (int64_t i = 0; OB_SUCC(ret) && i < args.length();) {
      int64_t end = i + 1;
      const char ch = args.ptr()[i];
      if (ch == '\'' || ch == '"') {
        while (end < args.length()) {
          if (args.ptr()[end] == '\\' && end + 1 < args.length()) {
            end += 2;
          } else if (args.ptr()[end++] == ch) {
            if (end < args.length() && args.ptr()[end] == ch) { ++end; }
            else { break; }
          }
        }
      } else if (ch == '`') {
        while (end < args.length() && args.ptr()[end] != '`') { ++end; }
        if (end < args.length()) { ++end; }
      } else if (std::isalpha(static_cast<unsigned char>(ch)) || ch == '_' || static_cast<unsigned char>(ch) >= 128) {
        while (end < args.length() && (std::isalnum(static_cast<unsigned char>(args.ptr()[end]))
               || args.ptr()[end] == '_' || static_cast<unsigned char>(args.ptr()[end]) >= 128)) { ++end; }
      }
      ObString token(end - i, args.ptr() + i);
      ObString name = token;
      if (ch == '`' && token.length() >= 2) {
        name.assign_ptr(token.ptr() + 1, token.length() - 2);
      }
      bool reference = false;
      if (ch != '\'' && ch != '"') {
        for (int64_t c = 0; !reference && c < columns.count(); ++c) {
          reference = name.case_compare(columns.at(c).col_name_) == 0;
        }
      }
      if (reference && OB_FAIL(prefixed.append("NEW."))) {
      } else if (OB_FAIL(prefixed.append(token))) {
      }
      i = end;
    }
    if (OB_SUCC(ret)) { result = prefixed.string(); }
  }
  return ret;
}

int AIColumnResolver::resolve_definition(ObIAllocator &allocator, ObColumnSchemaV2 &column,
                                          const ParseNode &expression, const ParseNode *options)
{
  int ret = OB_SUCCESS;
  int64_t retries = 3;
  bool has_mode = false;
  bool has_retries = false;
  for (int64_t i = 0; OB_SUCC(ret) && options != nullptr && i < options->num_child_; ++i) {
    const ParseNode *option = options->children_[i];
    if (option == nullptr || option->num_child_ != 2 || option->children_[0] == nullptr
        || option->children_[1] == nullptr) {
      ret = OB_INVALID_ARGUMENT;
    } else {
      ObString name(option->children_[0]->str_len_, option->children_[0]->str_value_);
      const ParseNode &value = *option->children_[1];
      if (name.case_compare("sync_mode") == 0 && !has_mode) {
        has_mode = true;
        ObString mode(value.str_len_, value.str_value_);
        if (value.type_ != T_VARCHAR || mode.case_compare("async") != 0) {
          ret = OB_NOT_SUPPORTED;
        }
      } else if (name.case_compare("max_retries") == 0 && !has_retries) {
        has_retries = true;
        if (value.type_ != T_INT || value.value_ < 0 || value.value_ > 100) {
          ret = OB_INVALID_ARGUMENT;
        } else {
          retries = value.value_;
        }
      } else {
        ret = OB_INVALID_ARGUMENT;
      }
    }
  }
  ObJsonObject *config = nullptr;
  ObJsonString *text = nullptr;
  ObJsonInt *number = nullptr;
  ObString source(expression.str_len_, expression.str_value_);
  ObString serialized;
  if (OB_FAIL(ret)) {
  } else if (source.empty()) {
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(ObAIFuncJsonUtils::get_json_object(allocator, config))) {
  } else if (OB_FAIL(ObAIFuncJsonUtils::get_json_string(allocator, source, text))) {
  } else if (OB_FAIL(config->add("expression", text))) {
  } else if (OB_FAIL(ObAIFuncJsonUtils::get_json_int(allocator, retries, number))) {
  } else if (OB_FAIL(config->add("max_retries", number))) {
  } else if (OB_FAIL(ObAIFuncJsonUtils::get_json_int(allocator, ObTimeUtility::current_time(), number))) {
  } else if (OB_FAIL(config->add("definition_version", number))) {
  } else if (OB_FAIL(ObAIFuncJsonUtils::print_json_to_str(allocator, config, serialized))) {
  } else if (OB_FAIL(column.set_ai_column_config(serialized))) {
  }
  if (ret == OB_NOT_SUPPORTED) {
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "AI columns require sync_mode='async'");
  } else if (ret == OB_INVALID_ARGUMENT) {
    LOG_USER_ERROR(OB_INVALID_ARGUMENT, "AI COLUMN expression or options");
  }
  return ret;
}

int AIColumnResolver::finish_table(ObRawExprFactory &factory, const ObSQLSessionInfo &session,
                                  const ObSchemaChecker *checker, ObTableSchema &table)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator allocator("AIColumnDDL");
  ObSEArray<ObColumnSchemaV2 *, 4> ai_columns;
  for (auto it = table.column_begin(); OB_SUCC(ret) && it != table.column_end(); ++it) {
    if ((*it)->is_ai_column()) {
      ret = ai_columns.push_back(const_cast<ObColumnSchemaV2 *>(*it));
    }
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < ai_columns.count(); ++i) {
    ObColumnSchemaV2 &column = *ai_columns.at(i);
    ObJsonObject *config = nullptr;
    ObString config_text = column.get_ai_column_config();
    ObRawExpr *expr = nullptr;
    ObSEArray<ObQualifiedName, 4> names;
    ObString expression;
    if (OB_FAIL(ObAIFuncJsonUtils::get_json_object_form_str(allocator, config_text, config))) {
    } else {
      ObJsonNode *node = config->get_value("expression");
      if (node == nullptr || node->json_type() != ObJsonNodeType::J_STRING) {
        ret = OB_INVALID_ARGUMENT;
      } else {
        expression = static_cast<ObJsonString *>(node)->get_str();
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(ObRawExprUtils::build_generated_column_expr(expression, factory, session,
                      expr, names, &table, nullptr, checker, ObResolverUtils::DISABLE_CHECK))) {
    } else if (expr == nullptr || (expr->get_expr_type() != T_FUN_SYS_AI_COMPLETE
                                && expr->get_expr_type() != T_FUN_SYS_AI_EMBED)) {
      ret = OB_NOT_SUPPORTED;
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "AI columns require AI_COMPLETE or AI_EMBED as their root expression");
    } else if (expr->get_param_count() < 2 || !expr->get_param_expr(0)->is_const_raw_expr()) {
      ret = OB_INVALID_ARGUMENT;
    }
    if (OB_SUCC(ret)) {
      for (int64_t arg = 0; OB_SUCC(ret) && arg < expr->get_param_count(); ++arg) {
        ret = check_request_argument(*expr->get_param_expr(arg));
      }
      if (ret == OB_NOT_SUPPORTED) {
        LOG_USER_ERROR(OB_NOT_SUPPORTED, "non-deterministic or remote functions inside AI COLUMN arguments");
      }
    }
    if (OB_SUCC(ret)) {
      if (config->get_value("hidden_base") == nullptr) {
        ObJsonInt *base = nullptr;
        if (OB_FAIL(ObAIFuncJsonUtils::get_json_int(allocator, column.get_column_id(), base))) {
        } else if (OB_FAIL(config->add("hidden_base", base))) {
        }
      }
      const ObObj &model = static_cast<ObConstRawExpr *>(expr->get_param_expr(0))->get_value();
      ObAIFuncExprInfo *info = nullptr;
      if (!ob_is_string_type(model.get_type()) || model.is_null()) {
        ret = OB_INVALID_ARGUMENT;
      } else if (OB_FAIL(ObAIFuncUtils::get_ai_func_info(allocator, model.get_string(), info))) {
      } else if ((expr->get_expr_type() == T_FUN_SYS_AI_EMBED && info->type_ != share::EndpointType::DENSE_EMBEDDING)
                 || (expr->get_expr_type() == T_FUN_SYS_AI_COMPLETE && info->type_ != share::EndpointType::COMPLETION)) {
        ret = OB_INVALID_ARGUMENT;
      }
    }
    ObJsonArray *dependencies = nullptr;
    if (OB_SUCC(ret)) {
      ret = ObAIFuncJsonUtils::get_json_array(allocator, dependencies);
    }
    for (int64_t j = 0; OB_SUCC(ret) && j < names.count(); ++j) {
      const ObColumnSchemaV2 *dependency = table.get_column_schema(names.at(j).col_name_);
      ObJsonInt *id = nullptr;
      if (dependency == nullptr) {
        ret = OB_ERR_BAD_FIELD_ERROR;
      } else if (dependency->is_ai_column() || dependency->is_generated_column()
                 || dependency->is_hidden() || dependency->get_column_id() == column.get_column_id()) {
        ret = OB_NOT_SUPPORTED;
      } else if (OB_FAIL(ObAIFuncJsonUtils::get_json_int(allocator, dependency->get_column_id(), id))) {
      } else if (OB_FAIL(dependencies->append(id))) {
      }
    }
    if (OB_SUCC(ret) && expr->get_expr_type() == T_FUN_SYS_AI_EMBED) {
      int64_t dimension = 0;
      if (!column.is_collection()) {
        ret = OB_INVALID_ARGUMENT;
      } else if (OB_FAIL(share::ObVectorIndexUtil::get_vector_dim_from_extend_type_info(column.get_extended_type_info(), dimension))) {
      } else if (expr->get_param_count() == 3) {
        ObRawExpr *arg = expr->get_param_expr(2);
        const bool integer = arg->is_const_raw_expr()
            && ob_is_integer_type(static_cast<ObConstRawExpr *>(arg)->get_value().get_type());
        const int64_t actual = integer ? static_cast<ObConstRawExpr *>(arg)->get_value().get_int() : 0;
        if (!integer || actual != dimension) {
          ret = OB_ERR_INVALID_VECTOR_DIM;
          LOG_USER_ERROR(OB_ERR_INVALID_VECTOR_DIM, static_cast<uint32_t>(dimension), static_cast<uint32_t>(actual));
        }
      }
      if (OB_SUCC(ret)) {
        ObJsonInt *dim = nullptr;
        if (OB_FAIL(ObAIFuncJsonUtils::get_json_int(allocator, dimension, dim))) {
        } else if (OB_FAIL(config->add("dimension", dim))) {
        }
      }
    }
    if (OB_SUCC(ret) && expr->get_expr_type() == T_FUN_SYS_AI_COMPLETE
        && (column.is_collection() || column.is_geometry() || column.is_extend())) {
      ret = OB_NOT_SUPPORTED;
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "AI_COMPLETE result columns of collection, geometry or extended types");
    }
    if (OB_SUCC(ret)) {
      const char *hint = column.is_json() ? "Return exactly one valid JSON value."
          : ob_is_integer_type(column.get_data_type()) ? "Return exactly one base-10 integer."
          : ob_is_numeric_type(column.get_data_type()) ? "Return exactly one finite decimal number."
          : "Return only the requested text, without quotation marks or explanation.";
      ObString result_prompt(hint);
      ObJsonString *hint_node = nullptr;
      ObJsonString *request_node = nullptr;
      ObString request_expr;
      ObString serialized;
      if (OB_FAIL(make_request_expr(allocator, expression, names, request_expr))) {
      } else if (OB_FAIL(ObAIFuncJsonUtils::get_json_string(allocator, request_expr, request_node))) {
      } else if (OB_FAIL(config->add("request_expr", request_node))) {
      } else if (OB_FAIL(ObAIFuncJsonUtils::get_json_string(allocator, result_prompt, hint_node))) {
      } else if (OB_FAIL(config->add("result_prompt", hint_node))) {
      } else if (OB_FAIL(config->add("dependencies", dependencies))) {
      } else if (OB_FAIL(ObAIFuncJsonUtils::print_json_to_str(allocator, config, serialized))) {
      } else if (OB_FAIL(column.set_ai_column_config(serialized))) {
      }
    }
    for (int64_t hidden = 0; OB_SUCC(ret) && hidden < 3; ++hidden) {
      ObColumnSchemaV2 state;
      char name[96];
      const char *suffix[] = {"state", "request", "spec"};
      snprintf(name, sizeof(name), "__ai_%lu_%s", static_cast<uint64_t>(config->get_value("hidden_base")->get_int()), suffix[hidden]);
      const ObColumnSchemaV2 *existing = table.get_column_schema(ObString(name));
      if (existing != nullptr) {
        if (!existing->is_hidden()) {
          ret = OB_ERR_COLUMN_DUPLICATE;
          LOG_USER_ERROR(OB_ERR_COLUMN_DUPLICATE, static_cast<int>(strlen(name)), name);
        }
        continue;
      }
      state.set_table_id(table.get_table_id());
      state.set_column_id(table.get_max_used_column_id() + 1);
      state.set_is_hidden(true);
      state.set_nullable(true);
      state.set_data_type(hidden == 1 ? ObVarcharType : ObIntType);
      if (hidden == 1) {
        state.set_data_length(64);
        state.set_collation_type(CS_TYPE_UTF8MB4_BIN);
      }
      if (OB_FAIL(state.set_column_name(name))) {
      } else if (OB_FAIL(table.add_column(state))) {
      }
    }
  }
  return ret;
}

int AIColumnResolver::preserve_hidden_columns(ObColumnSchemaV2 &column, const ObColumnSchemaV2 &previous)
{
  int ret = OB_SUCCESS;
  if (column.is_ai_column() && previous.is_ai_column()) {
    ObArenaAllocator allocator("AIColumnAlter");
    ObJsonObject *config = nullptr;
    ObJsonObject *old_config = nullptr;
    ObString text = column.get_ai_column_config();
    ObString old_text = previous.get_ai_column_config();
    ObString serialized;
    if (OB_FAIL(ObAIFuncJsonUtils::get_json_object_form_str(allocator, text, config))) {
    } else if (OB_FAIL(ObAIFuncJsonUtils::get_json_object_form_str(allocator, old_text, old_config))) {
    } else if (old_config->get_value("hidden_base") == nullptr) {
      ret = OB_INVALID_DATA;
    } else if (OB_FAIL(config->add("hidden_base", old_config->get_value("hidden_base")))) {
    } else if (OB_FAIL(ObAIFuncJsonUtils::print_json_to_str(allocator, config, serialized))) {
    } else if (OB_FAIL(column.set_ai_column_config(serialized))) {
    }
  }
  return ret;
}
}
}
