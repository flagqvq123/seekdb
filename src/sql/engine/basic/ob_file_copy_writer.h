#pragma once
#include "sql/engine/basic/ob_file_scan_utils.h"
namespace oceanbase { namespace sql {
class FileCopyWriter {
public:
  FileCopyWriter()=default;
  ~FileCopyWriter();
  int open(const FileSqlConfig &config);
  int row(const std::vector<FileSqlCell> &row);
  int finish();
  void abort();
private:
  FILE *file_=nullptr;
  int directory_=-1;
  std::string filename_;
  std::string temporary_;
  FileSqlConfig config_;
};
} }
