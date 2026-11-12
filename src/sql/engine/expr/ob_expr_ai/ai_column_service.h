#pragma once

#include "share/ob_thread_pool.h"
#include "share/schema/ob_table_schema.h"
#include "sql/engine/ob_exec_context.h"

namespace oceanbase
{
namespace sql
{
class AIColumnService : public share::ObThreadPool
{
public:
  static AIColumnService &instance();
  static int install(ObExecContext &ctx, uint64_t table_id);
  static int check_strict_read(ObExecContext &ctx, uint64_t table_id, uint64_t column_id);
  static int retry(ObExecContext &ctx, uint64_t table_id, uint64_t column_id, const common::ObString &predicate);
  static int remove_definition(uint64_t table_id);
  static bool is_worker_write();
  static bool is_strict_read_query();
  static bool is_result_publication();
  void run1() override;
private:
  AIColumnService() : share::ObThreadPool(4) {}
  int process_one();
};
}
}
