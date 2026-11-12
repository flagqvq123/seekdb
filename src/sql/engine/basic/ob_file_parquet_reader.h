#pragma once
#include "sql/engine/basic/ob_file_scan_utils.h"
#include <memory>
namespace oceanbase { namespace sql {
class ParquetReader {
public:
  ParquetReader();
  ~ParquetReader();
  int open(FILE *file, FileSqlConfig &config, bool infer, const std::vector<int64_t> &projection);
  int next(std::vector<FileSqlCell> &row);
private:
  struct State;
  std::unique_ptr<State> state_;
};
} }
