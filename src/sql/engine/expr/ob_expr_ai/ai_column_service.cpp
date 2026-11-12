#define USING_LOG_PREFIX SQL_ENG
#include "sql/engine/expr/ob_expr_ai/ai_column_service.h"
#include "sql/engine/expr/ob_expr_ai/ob_ai_func_utils.h"
#include "sql/engine/expr/ob_expr_ai/ob_ai_func_client.h"
#include "share/schema/ob_multi_version_schema_service.h"
#include "share/schema/ob_trigger_info.h"
#include "share/ob_server_struct.h"
#include "query/ai/ob_ai_endpoint_resolver.h"
#include "share/rc/ob_server_runtime.h"
#include "query/session/ob_inner_sql_connection_access.h"
#include "common/mysqlclient/ob_mysql_transaction.h"
#include "common/mysqlclient/ob_mysql_result.h"
#include "lib/string/ob_sql_string.h"
#include "share/ob_dml_sql_splicer.h"
#include "share/object/ob_obj_cast.h"
#include "share/ob_lob_access_utils.h"
#include <string>
#include <mutex>
#include <map>
#include <cmath>
#include <cctype>
#include <atomic>

namespace oceanbase
{
using namespace common;
using namespace share;
using namespace share::schema;
using namespace sqlclient;
namespace sql
{
namespace
{
thread_local bool worker_write = false;
thread_local bool strict_read_query = false;
thread_local bool result_publication = false;
std::mutex endpoint_mutex;
std::map<std::string, int64_t> endpoint_active;
std::map<std::string, int64_t> endpoint_limits;
std::atomic<uint64_t> claim_sequence{0};

struct WorkerWriteGuard
{
  WorkerWriteGuard() : previous_(worker_write) { worker_write = true; }
  ~WorkerWriteGuard() { worker_write = previous_; }
  bool previous_;
};

int write_sql(ObISQLClient &client, const ObSqlString &query)
{
  int64_t affected = 0;
  if (worker_write && query.string().prefix_match("UPDATE ")) {
    ObSqlString internal;
    int ret = internal.append("UPDATE /*+ USE_PLAN_CACHE(NONE) */ ");
    if (OB_SUCC(ret)) { ret = internal.append(query.ptr() + 7, query.length() - 7); }
    return OB_SUCC(ret) ? client.write(internal.ptr(), affected) : ret;
  }
  return client.write(query.ptr(), affected);
}

int quote_name(ObSqlString &sql, const ObString &name)
{
  int ret = sql.append("`");
  for (int64_t i = 0; OB_SUCC(ret) && i < name.length(); ++i) {
    if (name.ptr()[i] == '`') {
      ret = sql.append("``");
    } else {
      ret = sql.append_fmt("%c", name.ptr()[i]);
    }
  }
  if (OB_SUCC(ret)) { ret = sql.append("`"); }
  return ret;
}

int literal(ObSqlString &sql, const ObString &value)
{
  return sql_append_hex_escape_str(value, sql);
}

ObString string_member(ObJsonObject &object, const char *name)
{
  ObJsonNode *node = object.get_value(name);
  return node != nullptr && node->json_type() == ObJsonNodeType::J_STRING
      ? static_cast<ObJsonString *>(node)->get_str() : ObString();
}

int url_concurrency_limit(const ObAiModelEndpointInfo &endpoint, int64_t &limit)
{
  int ret = OB_SUCCESS;
  limit = endpoint.get_max_concurrency();
  ObSqlString query;
  if (OB_FAIL(query.append("SELECT MIN(max_concurrency) AS concurrency_limit FROM oceanbase.__all_ai_model_endpoint WHERE url="))) {
  } else if (OB_FAIL(literal(query, endpoint.get_url()))) {
  } else {
    SMART_VAR(ObMySQLProxy::MySQLResult, result) {
      if (OB_FAIL(GCTX.sql_proxy_->read(result, query.ptr()))) {
      } else if (result.get_result() == nullptr) { ret = OB_ERR_UNEXPECTED; }
      else if (OB_FAIL(result.get_result()->next())) {
      } else if (OB_FAIL(result.get_result()->get_int("concurrency_limit", limit))) {
      } else if (limit < 1 || limit > 1024) { ret = OB_INVALID_DATA; }
    }
  }
  return ret;
}

int parse_config(ObIAllocator &allocator, const ObColumnSchemaV2 &column, ObJsonObject *&config)
{
  ObString text = column.get_ai_column_config();
  return ObAIFuncJsonUtils::get_json_object_form_str(allocator, text, config);
}

int read_task_text(ObMySQLResult &result, const char *name, ObIAllocator &allocator, ObString &text)
{
  int ret = OB_SUCCESS;
  ObObj value;
  ObString data;
  if (OB_FAIL(result.get_obj(name, value))) {
  } else if (value.is_null()) { ret = OB_INVALID_DATA; }
  else if (value.is_lob_storage()) {
    auto service = share::server_service<common::ObILobReadService>();
    if (service == nullptr) { ret = OB_NOT_INIT; }
    else {
      const ObLobReadOptions options(*service, ObTimeUtility::current_time() + 10000000);
      ret = lob_helper::read_real_string_data(&allocator, value, data, &options);
    }
  } else { ret = value.get_string(data); }
  if (OB_SUCC(ret)) { ret = ob_write_string(allocator, data, text); }
  return ret;
}

uint64_t hidden_base(const ObJsonObject &config, uint64_t column_id)
{
  const ObJsonNode *base = config.get_value("hidden_base");
  return base != nullptr && base->json_type() == ObJsonNodeType::J_INT
      ? static_cast<uint64_t>(base->get_int()) : column_id;
}

int vector_expression(ObIAllocator &allocator, const ObString &text, const ObJsonObject &config, ObString &expression)
{
  int ret = OB_SUCCESS;
  ObIJsonBase *vector = nullptr;
  ObJsonArray *batch = nullptr;
  const ObJsonNode *dimension = config.get_value("dimension");
  if (dimension == nullptr || dimension->json_type() != ObJsonNodeType::J_INT || dimension->get_int() <= 0) {
    ret = OB_INVALID_DATA;
  } else if (OB_FAIL(ObJsonBaseFactory::get_json_base(&allocator, text, ObJsonInType::JSON_TREE, ObJsonInType::JSON_TREE, vector))) {
  } else if (vector == nullptr) { ret = OB_INVALID_DATA; }
  else if (OB_FAIL(ObAIFuncJsonUtils::get_json_array(allocator, batch))) {
  } else if (OB_FAIL(batch->append(static_cast<ObJsonNode *>(vector)))) {
  } else if (OB_FAIL(ObAIFuncUtils::validate_embedding_array(batch, 1, dimension->get_int()))) {
  } else {
    ret = ObAIFuncJsonUtils::print_json_to_str(allocator, vector, expression);
  }
  return ret;
}

int append_rowkey(ObSqlString &sql, const ObTableSchema &table, const char *prefix)
{
  int ret = sql.append("CONCAT(");
  for (int64_t i = 0; OB_SUCC(ret) && i < table.get_rowkey_column_num(); ++i) {
    uint64_t column_id = OB_INVALID_ID;
    if (OB_FAIL(table.get_rowkey_info().get_column_id(i, column_id))) { break; }
    const ObColumnSchemaV2 *column = table.get_column_schema(column_id);
    ObSqlString name;
    if (column == nullptr) {
      ret = OB_ERR_UNEXPECTED;
    } else if (OB_FAIL(quote_name(name, column->get_column_name_str()))) {
    } else {
      if (i > 0) { ret = sql.append(", ' AND ', "); }
      if (OB_SUCC(ret)) {
        ObSqlString label;
        if (OB_FAIL(label.append_fmt("%s = ", name.ptr()))) {
        } else if (OB_FAIL(literal(sql, label.string()))) {
        } else if (ob_is_integer_type(column->get_data_type())) {
          ret = sql.append_fmt(", CAST(%s.%s AS CHAR)", prefix, name.ptr());
        } else if (OB_FAIL(sql.append_fmt(", CONCAT('X''', HEX(%s.%s), '''')", prefix, name.ptr()))) {
        }
      }
    }
  }
  if (OB_SUCC(ret)) { ret = sql.append(")"); }
  return ret;
}

int changed_condition(ObSqlString &sql, const ObTableSchema &table, const ObJsonObject &config)
{
  int ret = sql.append("(");
  ObJsonNode *node = config.get_value("dependencies");
  if (node == nullptr || node->json_type() != ObJsonNodeType::J_ARRAY) {
    ret = OB_INVALID_DATA;
  } else {
    ObJsonArray *ids = static_cast<ObJsonArray *>(node);
    bool first = true;
    for (int64_t i = 0; OB_SUCC(ret) && i < ids->element_count() + table.get_rowkey_column_num(); ++i) {
      uint64_t id = OB_INVALID_ID;
      if (i < ids->element_count()) { id = ids->get_value(i)->get_int(); }
      else if (OB_FAIL(table.get_rowkey_info().get_column_id(i - ids->element_count(), id))) { break; }
      const ObColumnSchemaV2 *column = table.get_column_schema(id);
      ObSqlString name;
      if (column == nullptr) { ret = OB_ERR_UNEXPECTED; }
      else if (OB_FAIL(quote_name(name, column->get_column_name_str()))) {}
      else if (OB_FAIL(sql.append_fmt("%s NOT (NEW.%s <=> OLD.%s)", first ? "" : " OR", name.ptr(), name.ptr()))) {}
      else { first = false; }
    }
    if (first && OB_SUCC(ret)) { ret = sql.append("FALSE"); }
  }
  if (OB_SUCC(ret)) { ret = sql.append(")"); }
  return ret;
}

int generate_trigger(ObISQLClient &client, const ObString &database, const ObTableSchema &table,
                     const ObIArray<ObColumnSchemaV2 *> &columns, int kind)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator allocator("AIColumnTrig");
  ObSqlString query;
  ObSqlString qualified;
  const char *suffix[] = {"bi", "bu", "ai", "au", "ad"};
  const char *timing[] = {"BEFORE INSERT", "BEFORE UPDATE", "AFTER INSERT", "AFTER UPDATE", "AFTER DELETE"};
  if (OB_FAIL(quote_name(qualified, database))) {
  } else if (OB_FAIL(qualified.append("."))) {
  } else if (OB_FAIL(quote_name(qualified, table.get_table_name_str()))) {
  } else if (OB_FAIL(query.append("CREATE TRIGGER "))) {
  } else if (OB_FAIL(quote_name(query, database))) {
  } else if (OB_FAIL(query.append_fmt(".`__ai_%lu_%s` %s ON %s FOR EACH ROW BEGIN ",
                 table.get_table_id(), suffix[kind], timing[kind], qualified.ptr()))) {
  }
  if (OB_SUCC(ret) && kind < 2) {
    ret = query.append("DECLARE ai_request VARCHAR(64) DEFAULT UUID(); ");
  }
  ObSqlString rowkey;
  if (OB_SUCC(ret) && kind >= 2) { ret = append_rowkey(rowkey, table, kind == 4 ? "OLD" : "NEW"); }
  for (int64_t i = 0; OB_SUCC(ret) && i < columns.count(); ++i) {
    const ObColumnSchemaV2 &column = *columns.at(i);
    const uint64_t column_id = column.get_column_id();
    uint64_t id = column_id;
    ObJsonObject *config = nullptr;
    ObSqlString name;
    if (OB_FAIL(parse_config(allocator, column, config))) {
    } else if (OB_FALSE_IT(id = hidden_base(*config, column_id))) {
    } else if (OB_FAIL(quote_name(name, column.get_column_name_str()))) {
    } else if (kind < 2) {
      if (kind == 1) {
        if (OB_FAIL(query.append("IF "))) {
        } else if (OB_FAIL(changed_condition(query, table, *config))) {
        } else if (OB_FAIL(query.append(" THEN "))) {
        }
      }
      if (OB_SUCC(ret)) {
        ret = query.append_fmt("SET NEW.%s=NULL, NEW.`__ai_%lu_state`=0, "
              "NEW.`__ai_%lu_request`=ai_request, NEW.`__ai_%lu_spec`=%ld; ", name.ptr(), id, id, id,
              config->get_value("definition_version")->get_int());
      }
      if (kind == 1 && OB_SUCC(ret)) { ret = query.append("END IF; "); }
    } else if (kind == 4) {
      ret = query.append_fmt("DELETE FROM oceanbase.__all_ai_column_task WHERE table_id=%lu AND column_id=%lu AND rowkey_hash=SHA2(%s,256); ", table.get_table_id(), column_id, rowkey.ptr());
    } else {
      if (kind == 3) {
        if (OB_FAIL(query.append_fmt("IF NOT (NEW.`__ai_%lu_request` <=> OLD.`__ai_%lu_request`) THEN ", id, id))) {
        } else {
          ObSqlString oldkey;
          if (OB_FAIL(append_rowkey(oldkey, table, "OLD"))) {
          } else if (OB_FAIL(query.append_fmt("DELETE FROM oceanbase.__all_ai_column_task WHERE table_id=%lu AND column_id=%lu AND rowkey_hash=SHA2(%s,256); ", table.get_table_id(), column_id, oldkey.ptr()))) {
          }
        }
      }
      ObString request = string_member(*config, "request_expr");
      if (OB_SUCC(ret)) {
        ret = query.append_fmt("REPLACE INTO oceanbase.__all_ai_column_task(table_id,column_id,rowkey_hash,request_id,definition_version,rowkey_predicate,input_expr,state,retry_count,eligible_ts) "
            "VALUES(%lu,%lu,SHA2(%s,256),NEW.`__ai_%lu_request`,%ld,%s,%.*s,0,0,NOW(6)); ",
            table.get_table_id(), column_id, rowkey.ptr(), id, config->get_value("definition_version")->get_int(),
            rowkey.ptr(), request.length(), request.ptr());
      }
      if (kind == 3 && OB_SUCC(ret)) { ret = query.append("END IF; "); }
    }
  }
  if (OB_SUCC(ret)) { ret = query.append("END"); }
  if (OB_SUCC(ret)) { ret = write_sql(client, query); }
  return ret;
}
}

AIColumnService &AIColumnService::instance()
{
  static AIColumnService service;
  return service;
}
bool AIColumnService::is_worker_write() { return worker_write; }
bool AIColumnService::is_strict_read_query() { return strict_read_query; }
bool AIColumnService::is_result_publication() { return result_publication; }

int AIColumnService::remove_definition(uint64_t table_id)
{
  int ret = OB_SUCCESS;
  if (GCTX.sql_proxy_ == nullptr) { return OB_NOT_INIT; }
  ObMySQLTransaction trans;
  if (OB_FAIL(trans.start(GCTX.sql_proxy_))) {
  } else {
    ObSqlString tasks;
    ObSqlString columns;
    if (OB_FAIL(tasks.append_fmt("DELETE FROM oceanbase.__all_ai_column_task WHERE table_id=%lu", table_id))) {
    } else if (OB_FAIL(columns.append_fmt("DELETE FROM oceanbase.__all_ai_column WHERE table_id=%lu", table_id))) {
    } else if (OB_FAIL(write_sql(trans, tasks))) {
    } else if (OB_FAIL(write_sql(trans, columns))) {
    }
    const int end_ret = trans.end(OB_SUCC(ret));
    if (OB_SUCC(ret)) { ret = end_ret; }
  }
  return ret;
}

int AIColumnService::install(ObExecContext &ctx, uint64_t table_id)
{
  int ret = OB_SUCCESS;
  WorkerWriteGuard write_guard;
  ObArenaAllocator allocator("AIColumnMeta");
  ObSchemaGetterGuard guard;
  const ObTableSchema *table = nullptr;
  const ObDatabaseSchema *database = nullptr;
  ObSEArray<ObColumnSchemaV2 *, 4> columns;
  if (GCTX.schema_service_ == nullptr || GCTX.sql_proxy_ == nullptr) {
    ret = OB_NOT_INIT;
  } else if (OB_FAIL(GCTX.schema_service_->get_runtime_schema_guard(guard))) {
  } else if (OB_FAIL(guard.get_table_schema(table_id, table))) {
  } else if (table == nullptr) {
    ret = OB_TABLE_NOT_EXIST;
  } else if (OB_FAIL(guard.get_database_schema(table->get_database_id(), database))) {
  } else if (database == nullptr) {
    ret = OB_ERR_UNEXPECTED;
  }
  if (OB_SUCC(ret)) {
    for (auto it = table->column_begin(); OB_SUCC(ret) && it != table->column_end(); ++it) {
      if ((*it)->is_ai_column()) { ret = columns.push_back(*it); }
    }
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < columns.count(); ++i) {
    ObColumnSchemaV2 &column = *columns.at(i);
    ObJsonObject *config = nullptr;
    ObSqlString normalized;
    ObString dependencies;
    if (OB_FAIL(parse_config(allocator, column, config))) {
    } else if (OB_FAIL(normalized.append(string_member(*config, "expression")))) {
    } else if (OB_FAIL(normalized.append(" /* "))) {
    } else if (OB_FAIL(normalized.append(string_member(*config, "result_prompt")))) {
    } else if (OB_FAIL(normalized.append(" */"))) {
    } else if (OB_FAIL(ObAIFuncJsonUtils::print_json_to_str(allocator, config->get_value("dependencies"), dependencies))) {
    } else {
      ObDMLSqlSplicer splicer;
      ObSqlString query;
      const uint64_t id = column.get_column_id();
      const uint64_t base = hidden_base(*config, id);
      char state[96], request[96], spec[96];
      snprintf(state, sizeof(state), "__ai_%lu_state", base);
      snprintf(request, sizeof(request), "__ai_%lu_request", base);
      snprintf(spec, sizeof(spec), "__ai_%lu_spec", base);
      if (OB_FAIL(splicer.add_pk_column("table_id", table_id))) {
      } else if (OB_FAIL(splicer.add_pk_column("column_id", id))) {
      } else if (OB_FAIL(splicer.add_column("column_name", ObHexEscapeSqlStr(column.get_column_name_str())))) {
      } else if (OB_FAIL(splicer.add_column("normalized_expr", ObHexEscapeSqlStr(normalized.string())))) {
      } else if (OB_FAIL(splicer.add_column("dependency_column_ids", ObHexEscapeSqlStr(dependencies)))) {
      } else if (OB_FAIL(splicer.add_column("definition_version", config->get_value("definition_version")->get_int()))) {
      } else if (OB_FAIL(splicer.add_column("max_retries", config->get_value("max_retries")->get_int()))) {
      } else if (OB_FAIL(splicer.add_column("state_column_id", table->get_column_schema(ObString(state))->get_column_id()))) {
      } else if (OB_FAIL(splicer.add_column("request_column_id", table->get_column_schema(ObString(request))->get_column_id()))) {
      } else if (OB_FAIL(splicer.add_column("result_spec_column_id", table->get_column_schema(ObString(spec))->get_column_id()))) {
      } else if (OB_FAIL(splicer.add_column("config", ObHexEscapeSqlStr(column.get_ai_column_config())))) {
      } else if (OB_FAIL(splicer.splice_replace_sql("oceanbase.__all_ai_column", query))) {
      } else if (OB_FAIL(write_sql(*GCTX.sql_proxy_, query))) {
      }
    }
  }
  for (int kind = 0; OB_SUCC(ret) && columns.count() > 0 && kind < 5; ++kind) {
    ObSqlString remove;
    const char *suffix[] = {"bi", "bu", "ai", "au", "ad"};
    char trigger_name[96];
    snprintf(trigger_name, sizeof(trigger_name), "__ai_%lu_%s", table_id, suffix[kind]);
    const ObTriggerInfo *existing = nullptr;
    if (OB_FAIL(guard.get_trigger_info(table->get_database_id(), ObString(trigger_name), existing))) {
    } else if (existing == nullptr) {
    } else if (OB_FAIL(remove.append("DROP TRIGGER "))) {
    } else if (OB_FAIL(quote_name(remove, database->get_database_name_str()))) {
    } else if (OB_FAIL(remove.append_fmt(".`__ai_%lu_%s`", table_id, suffix[kind]))) {
    } else if (OB_FAIL(write_sql(*GCTX.sql_proxy_, remove))) {
    } else {
    }
    if (OB_SUCC(ret)) { ret = generate_trigger(*GCTX.sql_proxy_, database->get_database_name_str(), *table, columns, kind); }
  }
  if (OB_SUCC(ret) && !columns.empty()) {
    ObSqlString name;
    ObSqlString update;
    ObSqlString condition;
    ObSqlString key;
    if (OB_FAIL(quote_name(name, database->get_database_name_str()))) {}
    else if (OB_FAIL(name.append("."))) {}
    else if (OB_FAIL(quote_name(name, table->get_table_name_str()))) {}
    else if (OB_FAIL(append_rowkey(key, *table, "ai_row"))) {}
    else if (OB_FAIL(update.append_fmt("UPDATE %s AS ai_row SET ", name.ptr()))) {}
    const int64_t epoch = ObTimeUtility::current_time();
    for (int64_t i = 0; OB_SUCC(ret) && i < columns.count(); ++i) {
      const ObColumnSchemaV2 &column = *columns.at(i);
      ObJsonObject *config = nullptr;
      ObSqlString column_name;
      if (OB_FAIL(parse_config(allocator, column, config))) {}
      else if (OB_FAIL(quote_name(column_name, column.get_column_name_str()))) {}
      else {
        const uint64_t id = hidden_base(*config, column.get_column_id());
        const int64_t version = config->get_value("definition_version")->get_int();
        ObSqlString changed;
        if (OB_FAIL(changed.append_fmt("(`__ai_%lu_spec` IS NULL OR `__ai_%lu_spec`<>%ld)", id, id, version))) {}
        else if (OB_FAIL(condition.append_fmt("%s%s", i == 0 ? "" : " OR ", changed.ptr()))) {}
        else if (OB_FAIL(update.append_fmt("%s%s=IF(%s,NULL,%s),`__ai_%lu_state`=IF(%s,0,`__ai_%lu_state`),"
            "`__ai_%lu_request`=IF(%s,SHA2(CONCAT(%s,':%ld'),256),`__ai_%lu_request`),`__ai_%lu_spec`=%ld",
            i == 0 ? "" : ",", column_name.ptr(), changed.ptr(), column_name.ptr(), id, changed.ptr(), id,
            id, changed.ptr(), key.ptr(), epoch, id, id, version))) {}
      }
    }
    if (OB_SUCC(ret) && OB_FAIL(update.append_fmt(" WHERE %s", condition.ptr()))) {}
    else if (OB_SUCC(ret)) { ret = write_sql(*GCTX.sql_proxy_, update); }
  }
  return ret;
}

int AIColumnService::process_one()
{
  int ret = OB_SUCCESS;
  if (GCTX.sql_proxy_ == nullptr || GCTX.schema_service_ == nullptr || GCTX.status_ != SS_SERVING) {
    return OB_ITER_END;
  }
  WorkerWriteGuard write_guard;
  ObArenaAllocator allocator("AIColumnTask");
  ObMySQLTransaction claim;
  uint64_t table_id = 0;
  ObString request_id;
  ObString rowkey_hash;
  ObString predicate;
  ObSqlString group;
  char owner_buffer[96];
  snprintf(owner_buffer, sizeof(owner_buffer), "%ld-%lu", ObTimeUtility::current_time(), claim_sequence.fetch_add(1));
  ObString owner(owner_buffer);
  if (OB_FAIL(claim.start(GCTX.sql_proxy_))) {
  } else {
    SMART_VAR(ObMySQLProxy::MySQLResult, result) {
      if (OB_FAIL(claim.read(result, "SELECT table_id,request_id,rowkey_hash,rowkey_predicate FROM oceanbase.__all_ai_column_task "
          "WHERE state IN (0,3,4,5) AND eligible_ts <= NOW(6) AND (lease_until IS NULL OR lease_until < NOW(6)) "
          "ORDER BY eligible_ts LIMIT 1 FOR UPDATE SKIP LOCKED"))) {
      } else if (result.get_result() == nullptr) { ret = OB_ERR_UNEXPECTED; }
      else if (OB_FAIL(result.get_result()->next())) {}
      else {
        ObString value;
        int64_t id = 0;
        if (OB_FAIL(result.get_result()->get_int("table_id", id))) {}
        else if (id <= 0) { ret = OB_INVALID_DATA; }
        else if (OB_FALSE_IT(table_id = static_cast<uint64_t>(id))) {}
        else if (OB_FAIL(result.get_result()->get_varchar("request_id", value))) {}
        else if (OB_FAIL(ob_write_string(allocator, value, request_id))) {}
        else if (OB_FAIL(result.get_result()->get_varchar("rowkey_hash", value))) {}
        else if (OB_FAIL(ob_write_string(allocator, value, rowkey_hash))) {}
        else if (OB_FAIL(read_task_text(*result.get_result(), "rowkey_predicate", allocator, predicate))) {}
      }
      result.reset();
    }
    if (OB_SUCC(ret)) {
      ObSqlString update;
      if (OB_FAIL(group.append_fmt("table_id=%lu AND rowkey_hash=", table_id))) {}
      else if (OB_FAIL(literal(group, rowkey_hash))) {}
      else if (OB_FAIL(group.append(" AND request_id="))) {}
      else if (OB_FAIL(literal(group, request_id))) {}
      else if (OB_FAIL(update.append("UPDATE oceanbase.__all_ai_column_task SET lease_owner="))) {}
      else if (OB_FAIL(literal(update, owner))) {}
      else if (OB_FAIL(update.append_fmt(",lease_until=DATE_ADD(NOW(6),INTERVAL 300 SECOND) WHERE %s", group.ptr()))) {}
      else if (OB_FAIL(write_sql(claim, update))) {}
    }
    int end_ret = claim.end(OB_SUCC(ret));
    if (OB_SUCC(ret)) { ret = end_ret; }
  }
  if (OB_FAIL(ret)) { return ret; }
  if (OB_FAIL(group.append(" AND lease_owner="))) { return ret; }
  if (OB_FAIL(literal(group, owner))) { return ret; }
  ObSchemaGetterGuard schema_guard;
  const ObTableSchema *table = nullptr;
  const ObDatabaseSchema *database = nullptr;
  ObSqlString qualified;
  if (OB_FAIL(GCTX.schema_service_->get_runtime_schema_guard(schema_guard))) {}
  else if (OB_FAIL(schema_guard.get_table_schema(table_id, table))) {}
  else if (table == nullptr) {
    return remove_definition(table_id);
  } else if (OB_FAIL(schema_guard.get_database_schema(table->get_database_id(), database))) {}
  else if (database == nullptr) { ret = OB_ERR_UNEXPECTED; }
  else if (OB_FAIL(quote_name(qualified, database->get_database_name_str()))) {}
  else if (OB_FAIL(qualified.append("."))) {}
  else if (OB_FAIL(quote_name(qualified, table->get_table_name_str()))) {}
  struct Task {
    uint64_t column_id;
    ObString input;
    int64_t retries;
    int64_t state;
    ObString result;
    int64_t definition_version;
    int64_t result_is_null;
  };
  ObSEArray<Task, 4> tasks;
  if (OB_SUCC(ret)) {
    ObSqlString query;
    if (OB_FAIL(query.append_fmt("SELECT column_id,input_expr,retry_count,state,result_value,definition_version,result_value IS NULL AS result_is_null FROM oceanbase.__all_ai_column_task WHERE %s ORDER BY column_id", group.ptr()))) {}
    else {
      SMART_VAR(ObMySQLProxy::MySQLResult, result) {
        if (OB_FAIL(GCTX.sql_proxy_->read(result, query.ptr()))) {}
        else if (result.get_result() == nullptr) { ret = OB_ERR_UNEXPECTED; }
        else {
          while (OB_SUCC(ret) && OB_SUCC(result.get_result()->next())) {
            Task task{};
            ObString value;
            int64_t id = 0;
            if (OB_FAIL(result.get_result()->get_int("column_id", id))) {}
            else if (id <= 0) { ret = OB_INVALID_DATA; }
            else if (OB_FALSE_IT(task.column_id = static_cast<uint64_t>(id))) {}
            else if (OB_FAIL(result.get_result()->get_int("definition_version", task.definition_version))) {}
            else if (OB_FAIL(result.get_result()->get_int("retry_count", task.retries))) {}
            else if (OB_FAIL(result.get_result()->get_int("state", task.state))) {}
            else if (OB_FAIL(result.get_result()->get_int("result_is_null", task.result_is_null))) {}
            else if (OB_FAIL(read_task_text(*result.get_result(), "input_expr", allocator, task.input))) {}
            else if (task.state == 4 && !task.result_is_null && OB_FAIL(read_task_text(*result.get_result(), "result_value", allocator, task.result))) {}
            else if (OB_FAIL(tasks.push_back(task))) {}
          }
          if (ret == OB_ITER_END) { ret = OB_SUCCESS; }
        }
      }
    }
  }
  bool terminal = true;
  for (int64_t i = 0; OB_SUCC(ret) && i < tasks.count(); ++i) {
    Task &task = tasks.at(i);
    if (task.state == 4 || task.state == 5) { continue; }
    task.result_is_null = false;
    ObSqlString heartbeat;
    int64_t owned = 0;
    if (OB_FAIL(heartbeat.append_fmt("UPDATE oceanbase.__all_ai_column_task SET lease_until=DATE_ADD(NOW(6),INTERVAL 300 SECOND) WHERE %s", group.ptr()))) {
    } else if (OB_FAIL(GCTX.sql_proxy_->write(heartbeat.ptr(), owned))) {
    } else if (owned == 0) { ret = OB_STATE_NOT_MATCH; }
    if (OB_FAIL(ret)) { break; }
    const ObColumnSchemaV2 *column = table->get_column_schema(task.column_id);
    ObJsonObject *config = nullptr;
    ObIJsonBase *input = nullptr;
    int call_ret = OB_SUCCESS;
    if (column == nullptr || !column->is_ai_column()) { call_ret = OB_STATE_NOT_MATCH; }
    else if (OB_SUCCESS != (call_ret = parse_config(allocator, *column, config))) {}
    else if (config->get_value("definition_version")->get_int() != task.definition_version) { call_ret = OB_STATE_NOT_MATCH; }
    else if (OB_SUCCESS != (call_ret = ObJsonBaseFactory::get_json_base(&allocator, task.input,
                 ObJsonInType::JSON_TREE, ObJsonInType::JSON_TREE, input))) {}
    else if (input == nullptr || input->json_type() != ObJsonNodeType::J_ARRAY || input->element_count() < 2) {
      call_ret = OB_INVALID_DATA;
    } else {
      ObJsonArray *args = static_cast<ObJsonArray *>(input);
      ObJsonNode *model_node = args->get_value(0);
      ObJsonNode *prompt_node = args->get_value(1);
      if (model_node->json_type() != ObJsonNodeType::J_STRING) {
        call_ret = OB_INVALID_ARGUMENT;
      } else if (prompt_node->json_type() == ObJsonNodeType::J_NULL) {
        if (config->get_value("dimension") != nullptr) {
          task.result_is_null = true;
        } else {
          call_ret = OB_INVALID_ARGUMENT;
        }
      } else {
        ObString model_name = static_cast<ObJsonString *>(model_node)->get_str();
        ObString prompt;
        if (prompt_node->json_type() == ObJsonNodeType::J_STRING) {
          prompt = static_cast<ObJsonString *>(prompt_node)->get_str();
        } else if (prompt_node->json_type() == ObJsonNodeType::J_OBJECT) {
          call_ret = ObAIFuncPromptObjectUtils::replace_all_str_args_in_template(allocator, static_cast<ObJsonObject *>(prompt_node), prompt);
        } else { call_ret = OB_INVALID_ARGUMENT; }
        ObAIFuncExprInfo *info = nullptr;
        ObAiModelEndpointInfo endpoint;
        auto resolver = share::server_service<query::ObIAiEndpointResolver>();
        if (call_ret != OB_SUCCESS) {}
        else if (resolver == nullptr) { call_ret = OB_NOT_INIT; }
        else if (OB_SUCCESS != (call_ret = ObAIFuncUtils::get_ai_func_info(allocator, model_name, info))) {}
        else if (OB_SUCCESS != (call_ret = resolver->resolve_by_model_name(model_name, allocator, endpoint))) {}
        else {
          std::string url(endpoint.get_url().ptr(), endpoint.get_url().length());
          bool acquired = false;
          int64_t effective_limit = 0;
          call_ret = url_concurrency_limit(endpoint, effective_limit);
          const int64_t wait_deadline = ObTimeUtility::current_time() + 30000000;
          while (call_ret == OB_SUCCESS && !has_set_stop() && !acquired && ObTimeUtility::current_time() < wait_deadline) {
            {
              std::lock_guard<std::mutex> lock(endpoint_mutex);
              auto limit = endpoint_limits.find(url);
              if (limit == endpoint_limits.end()) { endpoint_limits[url] = effective_limit; }
              else { limit->second = effective_limit; }
              if (endpoint_active[url] < endpoint_limits[url]) { ++endpoint_active[url]; acquired = true; }
            }
            if (!acquired) { ob_usleep(10000); }
          }
          if (!acquired) {
            if (call_ret == OB_SUCCESS) { call_ret = has_set_stop() ? OB_CANCELED : OB_TIMEOUT; }
          } else {
            if (info->type_ == EndpointType::DENSE_EMBEDDING) {
              ObJsonObject *dimensions = nullptr;
              ObJsonInt *dim = nullptr;
              ObJsonNode *dimension = config->get_value("dimension");
              if (dimension == nullptr) { call_ret = OB_INVALID_ARGUMENT; }
              else if (OB_SUCCESS != (call_ret = ObAIFuncJsonUtils::get_json_object(allocator, dimensions))) {}
              else if (OB_SUCCESS != (call_ret = ObAIFuncJsonUtils::get_json_int(allocator, dimension->get_int(), dim))) {}
              else if (OB_SUCCESS != (call_ret = dimensions->add("dimensions", dim))) {}
              else {
                ObAIFuncIEmbed *provider = nullptr;
                ObArray<ObString> headers;
                ObArray<ObString> inputs;
                ObString key;
                ObString requested_model = endpoint.get_request_model_name().empty() ? info->model_ : endpoint.get_request_model_name();
                ObJsonObject *body = nullptr;
                ObJsonObject *response = nullptr;
                ObIJsonBase *parsed = nullptr;
                ObAIFuncClient client;
                client.set_timeout_sec(2);
                client.set_max_retry_times(0);
                if (OB_SUCCESS != (call_ret = inputs.push_back(prompt))) {}
                else if (OB_SUCCESS != (call_ret = ObAIFuncUtils::get_embed_provider(allocator, endpoint.get_provider(), provider))) {}
                else if (OB_SUCCESS != (call_ret = endpoint.get_unencrypted_access_key(allocator, key))) {}
                else if (OB_SUCCESS != (call_ret = provider->get_header(allocator, key, headers))) {}
                else if (OB_SUCCESS != (call_ret = provider->get_body(allocator, requested_model, inputs, dimensions, body))) {}
                else if (OB_SUCCESS != (call_ret = client.send_post(allocator, endpoint.get_url(), headers, body, response))) {}
                else if (OB_SUCCESS != (call_ret = provider->parse_output(allocator, response, parsed))) {}
                else if (OB_SUCCESS != (call_ret = ObAIFuncUtils::validate_embedding_array(parsed, 1, dimension->get_int()))) {}
                else { call_ret = ObAIFuncJsonUtils::print_json_to_str(allocator, static_cast<ObJsonArray *>(parsed)->get_value(0), task.result); }
              }
            } else if (info->type_ == EndpointType::COMPLETION) {
              ObAIFuncIComplete *provider = nullptr;
              ObJsonObject *body = nullptr;
              ObJsonObject *response = nullptr;
              ObJsonObject *user_config = nullptr;
              ObIJsonBase *value = nullptr;
              ObString key;
              ObArray<ObString> headers;
              ObString system_prompt = string_member(*config, "result_prompt");
              ObString requested_model = endpoint.get_request_model_name().empty() ? info->model_ : endpoint.get_request_model_name();
              ObAIFuncClient client;
              client.set_timeout_sec(2);
              client.set_max_retry_times(0);
              if (args->element_count() == 3 && args->get_value(2)->json_type() != ObJsonNodeType::J_NULL) {
                ObJsonNode *user = args->get_value(2);
                if (user->json_type() == ObJsonNodeType::J_OBJECT) { user_config = static_cast<ObJsonObject *>(user); }
                else if (user->json_type() == ObJsonNodeType::J_STRING) {
                  ObString text = static_cast<ObJsonString *>(user)->get_str();
                  call_ret = ObAIFuncJsonUtils::get_json_object_form_str(allocator, text, user_config);
                } else { call_ret = OB_INVALID_ARGUMENT; }
              }
              if (call_ret != OB_SUCCESS) {}
              else if (OB_SUCCESS != (call_ret = ObAIFuncUtils::get_complete_provider(allocator, endpoint.get_provider(), provider))) {}
              else if (OB_SUCCESS != (call_ret = endpoint.get_unencrypted_access_key(allocator, key))) {}
              else if (OB_SUCCESS != (call_ret = provider->get_header(allocator, key, headers))) {}
              else if (OB_SUCCESS != (call_ret = provider->get_body(allocator, requested_model, system_prompt, prompt, user_config, body))) {}
              else if (OB_SUCCESS != (call_ret = client.send_post(allocator, endpoint.get_url(), headers, body, response))) {}
              else if (OB_SUCCESS != (call_ret = provider->parse_output(allocator, response, value))) {}
              else if (value == nullptr) { call_ret = OB_INVALID_DATA; }
              else if (value->json_type() == ObJsonNodeType::J_STRING) {
                ObString text(value->get_data_length(), value->get_data());
                call_ret = ob_write_string(allocator, text, task.result);
              } else { call_ret = ObAIFuncJsonUtils::print_json_to_str(allocator, value, task.result); }
            } else { call_ret = OB_INVALID_ARGUMENT; }
            std::lock_guard<std::mutex> lock(endpoint_mutex);
            if (--endpoint_active[url] == 0) {
              endpoint_active.erase(url);
              endpoint_limits.erase(url);
            }
          }
        }
      }
    }
    if (call_ret == OB_SUCCESS && column != nullptr && !task.result_is_null) {
      if (column->is_json() || ob_is_numeric_type(column->get_data_type())) {
        ObIJsonBase *value = nullptr;
        call_ret = ObJsonBaseFactory::get_json_base(&allocator, task.result, ObJsonInType::JSON_TREE, ObJsonInType::JSON_TREE, value);
        if (call_ret == OB_SUCCESS && !column->is_json()) {
          if (!ObJsonHelper::is_number_type(value)) { call_ret = OB_INVALID_DATA; }
          else if (ob_is_integer_type(column->get_data_type()) && value->json_type() != ObJsonNodeType::J_INT) { call_ret = OB_INVALID_DATA; }
        }
      }
      if (call_ret == OB_SUCCESS && !column->is_json() && !column->is_collection()) {
        ObObj input_value;
        ObObj converted;
        input_value.set_varchar(task.result);
        input_value.set_collation_type(CS_TYPE_UTF8MB4_BIN);
        ObCastCtx cast_ctx(&allocator, nullptr, CM_STRICT_MODE | CM_ERROR_ON_SCALE_OVER | CM_COLUMN_CONVERT,
                           column->get_collation_type());
        ObAccuracy accuracy = column->get_accuracy();
        cast_ctx.res_accuracy_ = &accuracy;
        cast_ctx.dest_max_length_ = column->get_data_length();
        call_ret = ObObjCaster::to_type(column->get_data_type(), cast_ctx, input_value, converted);
        if (call_ret == OB_SUCCESS) {
          call_ret = ob_obj_accuracy_check_only(column->get_accuracy(), column->get_collation_type(), converted);
        }
      }
    }
    ObSqlString update;
    const int64_t max_retries = config == nullptr ? 0 : config->get_value("max_retries")->get_int();
    task.state = call_ret == OB_SUCCESS ? 4 : task.retries < max_retries ? 3 : 5;
    if (task.state == 3) { terminal = false; }
    if (OB_FAIL(update.append_fmt("UPDATE oceanbase.__all_ai_column_task SET state=%ld,retry_count=%ld,last_error=%d,eligible_ts=DATE_ADD(NOW(6),INTERVAL 1 SECOND),result_value=",
                   task.state, task.retries + (call_ret != OB_SUCCESS ? 1 : 0), call_ret))) {}
    else if (call_ret == OB_SUCCESS && !task.result_is_null && OB_FAIL(literal(update, task.result))) {}
    else if ((call_ret != OB_SUCCESS || task.result_is_null) && OB_FAIL(update.append("NULL"))) {}
    else if (OB_FAIL(update.append_fmt(" WHERE %s AND column_id=%lu", group.ptr(), task.column_id))) {}
    else {
      int64_t affected = 0;
      if (OB_FAIL(GCTX.sql_proxy_->write(update.ptr(), affected))) {}
      else if (affected != 1) { ret = OB_STATE_NOT_MATCH; }
    }
  }
  if (OB_SUCC(ret) && !terminal) {
    ObSqlString release;
    if (OB_FAIL(release.append_fmt("UPDATE oceanbase.__all_ai_column_task SET lease_until=NULL,eligible_ts=DATE_ADD(NOW(6),INTERVAL 1 SECOND) WHERE %s", group.ptr()))) {
    } else {
      ret = write_sql(*GCTX.sql_proxy_, release);
    }
  }
  if (OB_SUCC(ret) && terminal && !tasks.empty()) {
    struct PublicationGuard {
      bool previous_;
      PublicationGuard() : previous_(result_publication) { result_publication = true; }
      ~PublicationGuard() { result_publication = previous_; }
    } publication_guard;
    ObMySQLTransaction publish;
    ObSqlString update;
    ObSqlString remove;
    if (OB_FAIL(publish.start(GCTX.sql_proxy_))) {}
    else {
      if (OB_FAIL(update.append_fmt("UPDATE %s SET ", qualified.ptr()))) {}
      bool has_assignment = false;
      for (int64_t i = 0; OB_SUCC(ret) && i < tasks.count(); ++i) {
        const Task &task = tasks.at(i);
        const ObColumnSchemaV2 *column = table->get_column_schema(task.column_id);
        ObSqlString name;
        ObSqlString request_literal;
        ObString collection_expression;
        if (column == nullptr || !column->is_ai_column()) { continue; }
        ObJsonObject *config = nullptr;
        uint64_t base = task.column_id;
        if (OB_FAIL(parse_config(allocator, *column, config))) {}
        else if (column->is_collection() && task.state == 4 && !task.result_is_null
                 && OB_FAIL(vector_expression(allocator, task.result, *config, collection_expression))) {}
        else if (OB_FALSE_IT(base = hidden_base(*config, task.column_id))) {}
        else if (OB_FAIL(quote_name(name, column->get_column_name_str()))) {}
        else if (OB_FAIL(literal(request_literal, request_id))) {}
        else if (OB_FAIL(update.append_fmt("%s%s=IF(`__ai_%lu_request`=%s AND `__ai_%lu_spec`=%ld,", has_assignment ? "," : "", name.ptr(), base, request_literal.ptr(), base, task.definition_version))) {}
        else if (task.state == 4 && !task.result_is_null && column->is_collection()
                 && OB_FAIL(update.append(collection_expression))) {}
        else if (task.state == 4 && !task.result_is_null && !column->is_collection()
                 && OB_FAIL(literal(update, task.result))) {}
        else if ((task.state != 4 || task.result_is_null) && OB_FAIL(update.append("NULL"))) {}
        else if (OB_FAIL(update.append_fmt(",%s),`__ai_%lu_state`=IF(`__ai_%lu_request`=%s AND `__ai_%lu_spec`=%ld,%d,`__ai_%lu_state`)",
            name.ptr(), base, base, request_literal.ptr(), base, task.definition_version,
            task.state == 4 ? 1 : 2, base))) {}
        if (OB_SUCC(ret)) { has_assignment = true; }
      }
      if (OB_SUCC(ret) && OB_FAIL(update.append_fmt(" WHERE %.*s", predicate.length(), predicate.ptr()))) {}
      if (OB_SUCC(ret) && has_assignment) {
        if (tasks.count() == 1 && table->get_column_schema(tasks.at(0).column_id) != nullptr
            && table->get_column_schema(tasks.at(0).column_id)->is_collection() && update.length() < 2048) {
          const bool publication = is_result_publication();
          LOG_ERROR("AI vector publication SQL diagnostic", K(publication), K(table_id), "sql", update.string());
        }
        ret = write_sql(publish, update);
      }
      if (OB_SUCC(ret) && OB_FAIL(remove.append_fmt("DELETE FROM oceanbase.__all_ai_column_task WHERE %s", group.ptr()))) {}
      else if (OB_SUCC(ret)) { ret = write_sql(publish, remove); }
      int end_ret = publish.end(OB_SUCC(ret));
      if (OB_SUCC(ret)) { ret = end_ret; }
    }
    if (OB_FAIL(ret)) {
      const int publish_error = ret;
      LOG_WARN("AI column group publication failed", K(publish_error), K(table_id), K(request_id));
      for (int64_t i = 0; i < tasks.count(); ++i) {
        const Task &task = tasks.at(i);
        const ObColumnSchemaV2 *column = table->get_column_schema(task.column_id);
        ObJsonObject *config = nullptr;
        ObSqlString recovery;
        int recovery_ret = column == nullptr || !column->is_ai_column() ? OB_STATE_NOT_MATCH : parse_config(allocator, *column, config);
        if (recovery_ret == OB_SUCCESS) {
          recovery_ret = recovery.append_fmt("UPDATE oceanbase.__all_ai_column_task SET retry_count=retry_count+1,"
              "state=IF(state=4 AND retry_count>%ld,5,state),result_value=IF(state=5,NULL,result_value),"
              "last_error=%d,lease_until=NULL,eligible_ts=DATE_ADD(NOW(6),INTERVAL 1 SECOND) WHERE %s AND column_id=%lu",
              config->get_value("max_retries")->get_int(), publish_error, group.ptr(), task.column_id);
        }
        if (recovery_ret == OB_SUCCESS) { recovery_ret = write_sql(*GCTX.sql_proxy_, recovery); }
        if (recovery_ret != OB_SUCCESS && recovery_ret != OB_STATE_NOT_MATCH) {
          LOG_WARN("failed to release AI column publication lease", K(recovery_ret), K(table_id), K(request_id));
        }
      }
      ret = publish_error;
    }
  }
  return ret;
}

int AIColumnService::retry(ObExecContext &ctx, uint64_t table_id, uint64_t column_id, const ObString &predicate)
{
  int ret = OB_SUCCESS;
  WorkerWriteGuard write_guard;
  ObSchemaGetterGuard guard;
  const ObTableSchema *table = nullptr;
  const ObDatabaseSchema *database = nullptr;
  ObSqlString name;
  ObSqlString query;
  if (GCTX.schema_service_ == nullptr || GCTX.sql_proxy_ == nullptr) { ret = OB_NOT_INIT; }
  else if (OB_FAIL(GCTX.schema_service_->get_runtime_schema_guard(guard))) {}
  else if (OB_FAIL(guard.get_table_schema(table_id, table))) {}
  else if (table == nullptr) { ret = OB_TABLE_NOT_EXIST; }
  else if (OB_FAIL(guard.get_database_schema(table->get_database_id(), database))) {}
  else if (database == nullptr) { ret = OB_ERR_UNEXPECTED; }
  else if (OB_FAIL(quote_name(name, database->get_database_name_str()))) {}
  else if (OB_FAIL(name.append("."))) {}
  else if (OB_FAIL(quote_name(name, table->get_table_name_str()))) {}
  const ObColumnSchemaV2 *column = table == nullptr ? nullptr : table->get_column_schema(column_id);
  if (OB_SUCC(ret) && (column == nullptr || !column->is_ai_column())) { ret = OB_INVALID_ARGUMENT; }
  ObArenaAllocator allocator("AIColumnRetry");
  ObJsonObject *config = nullptr;
  uint64_t base = column_id;
  if (OB_SUCC(ret)) {
    if (OB_FAIL(parse_config(allocator, *column, config))) {}
    else { base = hidden_base(*config, column_id); }
  }
  ObSqlString condition;
  for (int64_t i = 0; OB_SUCC(ret) && i < predicate.length();) {
    int64_t end = i + 1;
    char ch = predicate.ptr()[i];
    if (ch == '\'' || ch == '"' || ch == '`') {
      while (end < predicate.length()) {
        if (predicate.ptr()[end] == '\\' && end + 1 < predicate.length()) { end += 2; }
        else if (predicate.ptr()[end++] == ch) {
          if (end < predicate.length() && predicate.ptr()[end] == ch) { ++end; }
          else { break; }
        }
      }
    } else if (std::isalpha(static_cast<unsigned char>(ch)) || ch == '_') {
      while (end < predicate.length() && (std::isalnum(static_cast<unsigned char>(predicate.ptr()[end]))
             || predicate.ptr()[end] == '_')) { ++end; }
    }
    ObString token(end - i, predicate.ptr() + i);
    if (ch != '`' && ch != '\'' && ch != '"' && token.case_compare("status") == 0) {
      ret = condition.append_fmt("(CASE `__ai_%lu_state` WHEN 1 THEN 'READY' WHEN 2 THEN 'FAILED' ELSE 'PENDING' END)", base);
    } else { ret = condition.append(token); }
    i = end;
  }
  ObSqlString result_column;
  if (OB_FAIL(ret)) {}
  else if (OB_FAIL(quote_name(result_column, column->get_column_name_str()))) {}
  else if (OB_FAIL(query.append_fmt("UPDATE %s SET %s=NULL,`__ai_%lu_state`=0,`__ai_%lu_request`=UUID(),`__ai_%lu_spec`=%ld WHERE ",
                      name.ptr(), result_column.ptr(), base, base, base, config->get_value("definition_version")->get_int()))) {}
  else if (predicate.empty()) { ret = query.append("TRUE"); }
  else { ret = query.append(condition.string()); }
  if (OB_SUCC(ret)) { ret = write_sql(*GCTX.sql_proxy_, query); }
  return ret;
}

void AIColumnService::run1()
{
  lib::set_thread_name("AIColumn");
  while (!has_set_stop()) {
    const int ret = process_one();
    if (ret != OB_SUCCESS && ret != OB_ITER_END) {
      LOG_WARN("AI column task failed", K(ret));
    }
    if (ret != OB_SUCCESS) { ob_usleep(100000); }
  }
}

int AIColumnService::check_strict_read(ObExecContext &ctx, uint64_t table_id, uint64_t column_id)
{
  int ret = OB_SUCCESS;
  const char *stage = "schema";
  ObSQLSessionInfo *session = ctx.get_my_session();
  ObSchemaGetterGuard *guard = ctx.get_sql_ctx() == nullptr ? nullptr : ctx.get_sql_ctx()->schema_guard_;
  const ObTableSchema *table = nullptr;
  const ObDatabaseSchema *database = nullptr;
  ObArenaAllocator allocator("AIColumnStrict");
  ObJsonObject *config = nullptr;
  uint64_t base = column_id;
  ObSqlString query;
  ObSqlString name;
  if (session == nullptr || guard == nullptr) { ret = OB_NOT_INIT; }
  else if (OB_FAIL(guard->get_table_schema(table_id, table))) {}
  else if (table == nullptr) { ret = OB_TABLE_NOT_EXIST; }
  else if (OB_FAIL(guard->get_database_schema(table->get_database_id(), database))) {}
  else if (database == nullptr) { ret = OB_ERR_UNEXPECTED; }
  else if (table->get_column_schema(column_id) == nullptr) { ret = OB_ERR_BAD_FIELD_ERROR; }
  else if (OB_FAIL(parse_config(allocator, *table->get_column_schema(column_id), config))) {}
  else if (OB_FALSE_IT(base = hidden_base(*config, column_id))) {}
  else if (OB_FAIL(quote_name(name, database->get_database_name_str()))) {}
  else if (OB_FAIL(name.append("."))) {}
  else if (OB_FAIL(quote_name(name, table->get_table_name_str()))) {}
  else if (OB_FAIL(query.append_fmt("SELECT /*+ USE_PLAN_CACHE(NONE) */ 1 FROM %s WHERE `__ai_%lu_state` IS NULL OR `__ai_%lu_state` <> 1 LIMIT 1", name.ptr(), base, base))) {}
  else {
    struct StrictReadGuard {
      bool previous_;
      StrictReadGuard() : previous_(strict_read_query) { strict_read_query = true; }
      ~StrictReadGuard() { strict_read_query = previous_; }
    } strict_guard;
    ObISQLConnectionGuard connection;
    stage = "connect";
    if (OB_FAIL(query::ObInnerSQLConnectionAccess::create_connection_with_external_session(session, connection))) {
    } else if (connection.get_ptr() == nullptr) { ret = OB_ERR_UNEXPECTED; }
    else {
      ObSQLSessionInfo::StmtSavedValue saved_session;
      stage = "begin_nested";
      if (OB_FAIL(session->begin_nested_session(saved_session, true))) {
      } else {
        stage = "execute_read";
        SMART_VAR(ObISQLClient::ReadResult, result) {
          if (OB_FAIL(connection.get_ptr()->execute_read(query.string(), result))) {}
          else if (result.get_result() == nullptr) { ret = OB_ERR_UNEXPECTED; stage = "get_result"; }
          else {
            stage = "next";
            if (OB_FAIL(result.get_result()->next())) { if (ret == OB_ITER_END) { ret = OB_SUCCESS; } }
            else {
              ret = OB_AI_COLUMN_NOT_READY;
              LOG_USER_ERROR(OB_AI_COLUMN_NOT_READY, table_id, column_id);
            }
          }
          result.reset();
        }
        const int restore_ret = session->end_nested_session(saved_session);
        if (restore_ret != OB_SUCCESS) {
          LOG_ERROR("failed to restore AI strict read parent session", K(restore_ret), K(table_id), K(column_id));
          if (OB_SUCC(ret)) { ret = restore_ret; stage = "end_nested"; }
        }
      }
    }
  }
  if (ret != OB_SUCCESS && ret != OB_AI_COLUMN_NOT_READY) {
    LOG_ERROR("AI strict read precheck failed", K(ret), K(stage), K(table_id), K(column_id));
  }
  return ret;
}
}
}
