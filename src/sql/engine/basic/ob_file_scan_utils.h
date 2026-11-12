#pragma once

#include <cstdio>
#include <string>
#include <vector>
#include "common/object/ob_object.h"

namespace oceanbase { namespace sql {
class ObSQLSessionInfo;

enum class FileSqlType : int64_t { UNKNOWN, INTEGER, DOUBLE, BOOLEAN, VARCHAR, DATE, DATETIME };
struct FileSqlColumn {
  std::string name_;
  FileSqlType type_ = FileSqlType::UNKNOWN;
  bool nullable_ = false;
  int64_t length_ = 0;
};
struct FileSqlCell {
  std::string text_;
  bool null_ = true;
  bool json_string_ = false;
};
struct FileSqlConfig {
  enum Format : int64_t { CSV, JSONL, PARQUET, GLOB };
  Format format_ = CSV;
  std::string path_;
  bool header_ = true;
  char delimiter_ = ',';
  char quote_ = '"';
  char escape_ = '"';
  std::string nullstr_;
  std::string secure_root_;
  int64_t device_ = 0, inode_ = 0, size_ = 0, mtime_ = 0, ctime_ = 0;
  std::vector<FileSqlColumn> columns_;
  int encode(common::ObIAllocator &allocator, common::ObString &wire) const;
  int decode(const common::ObString &wire);
};

class FileScanner {
public:
  FileScanner();
  ~FileScanner();
  int open(const FileSqlConfig &config, const std::vector<int64_t> &projection);
  int next(std::vector<FileSqlCell> &row);
  int rescan();
  int check_unchanged() const;
  void close();
private:
  struct State;
  State *state_;
  FileScanner(const FileScanner &) = delete;
  FileScanner &operator=(const FileScanner &) = delete;
};

class FileSqlUtils {
public:
  static constexpr int64_t MAX_RECORD_BYTES = 16 * 1024 * 1024;
  static constexpr int64_t MAX_COLUMNS = 4096;
  static int check_access(ObSQLSessionInfo &session, const std::string &path,
                         std::string &canonical, bool pattern = false, std::string *secure_root = nullptr);
  static int infer(FileSqlConfig &config);
  static common::ObObjType obj_type(FileSqlType type);
  static const char *type_name(FileSqlType type);
  static int cell_to_obj(const FileSqlCell &cell, FileSqlType type, common::ObObj &obj);
  static FileSqlType infer_type(const std::string &value);
};
} }
