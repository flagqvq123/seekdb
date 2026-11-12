#define USING_LOG_PREFIX SQL_ENG
#include "sql/engine/basic/ob_file_scan_utils.h"

#if defined(__linux__)
#include "sql/engine/basic/ob_file_parquet_reader.h"
#include "sql/session/ob_sql_session_info.h"
#include "sql/resolver/ob_resolver_utils.h"
#include "share/schema/ob_schema_utils.h"
#include "share/ob_server_struct.h"
#include "common/json_type/ob_json_tree.h"
#include "common/json_type/ob_json_base.h"
#include "lib/string/ob_sql_string.h"
#include "lib/worker.h"
#include <algorithm>
#include <charconv>
#include <cmath>
#include <cstring>
#include <cerrno>
#include <cstdlib>
#include <limits>
#include <sys/stat.h>
#include <unistd.h>
#include <fcntl.h>
#include <dirent.h>
#include <fnmatch.h>

namespace oceanbase { namespace sql {
using namespace common;
namespace {
FILE *open_regular(const std::string &path)
{
  int descriptor=::open(path.c_str(),O_RDONLY|O_NONBLOCK|O_NOFOLLOW|O_CLOEXEC);
  if(descriptor<0) {return nullptr;}
  struct stat status;
  if(fstat(descriptor,&status)!=0 || !S_ISREG(status.st_mode)) {::close(descriptor);errno=EINVAL;return nullptr;}
  FILE *file=fdopen(descriptor,"rb");
  if(file==nullptr) {::close(descriptor);}
  return file;
}
int bad_file(const char *reason, int64_t line = 0)
{
  char message[384];
  snprintf(message, sizeof(message), "file_sql: %s (record %ld)", reason, line);
  LOG_USER_ERROR(OB_INVALID_ARGUMENT, message);
  return OB_INVALID_ARGUMENT;
}
bool integer(const std::string &s, int64_t &value)
{
  if (s.empty()) { return false; }
  const char *begin = s.data();
  if (*begin == '+') { ++begin; }
  auto result = std::from_chars(begin, s.data() + s.size(), value);
  return result.ec == std::errc() && result.ptr == s.data() + s.size();
}
bool real(const std::string &s, double &value)
{
  if (s.empty() || s.find('\0') != std::string::npos) { return false; }
  char *end = nullptr;
  errno = 0;
  value = strtod(s.c_str(), &end);
  return end == s.data() + s.size() && (errno != ERANGE || value != 0) && std::isfinite(value)
      && s.find_first_not_of("0123456789+-.eE") == std::string::npos;
}
bool boolean(const std::string &s)
{
  return s == "true" || s == "false" || s == "TRUE" || s == "FALSE";
}
bool date_time(const std::string &s, int64_t &days, int64_t &micros)
{
  if (s.size() != 10 && s.size() != 19 && (s.size() < 21 || s.size() > 26)) { return false; }
  if (s[4] != '-' || s[7] != '-') { return false; }
  auto number = [&](size_t start, size_t length, int &v) {
    v = 0;
    for (size_t i = start; i < start + length; ++i) {
      if (s[i] < '0' || s[i] > '9') { return false; }
      v = v * 10 + s[i] - '0';
    }
    return true;
  };
  int year, month, day, hour = 0, minute = 0, second = 0;
  if (!number(0,4,year) || !number(5,2,month) || !number(8,2,day)
      || year < 1 || month < 1 || month > 12) { return false; }
  const int lengths[] = {31,28,31,30,31,30,31,31,30,31,30,31};
  int maximum = lengths[month - 1] + (month == 2 && year % 4 == 0 && (year % 100 != 0 || year % 400 == 0));
  if (day < 1 || day > maximum) { return false; }
  if (s.size() > 10 && ((s[10] != ' ' && s[10] != 'T') || s[13] != ':' || s[16] != ':'
      || !number(11,2,hour) || !number(14,2,minute) || !number(17,2,second)
      || hour > 23 || minute > 59 || second > 59)) { return false; }
  int fraction = 0;
  if (s.size() > 19) {
    if (s[19] != '.' || !number(20,s.size()-20,fraction)) { return false; }
    for (size_t i = s.size() - 20; i < 6; ++i) { fraction *= 10; }
  }
  year -= month <= 2;
  const int era = year / 400;
  const unsigned yoe = year - era * 400;
  const unsigned doy = (153 * (month + (month > 2 ? -3 : 9)) + 2) / 5 + day - 1;
  days = static_cast<int64_t>(era) * 146097 + yoe * 365 + yoe / 4 - yoe / 100 + doy - 719468;
  micros = (days * 86400 + hour * 3600 + minute * 60 + second) * 1000000 + fraction;
  return true;
}
FileSqlType widen(FileSqlType a, FileSqlType b)
{
  if (a == FileSqlType::UNKNOWN) { return b; }
  if (b == FileSqlType::UNKNOWN || a == b) { return a; }
  auto numeric = [](FileSqlType t) { return t == FileSqlType::INTEGER || t == FileSqlType::DOUBLE || t == FileSqlType::BOOLEAN; };
  if (numeric(a) && numeric(b)) { return a == FileSqlType::DOUBLE || b == FileSqlType::DOUBLE ? FileSqlType::DOUBLE : FileSqlType::INTEGER; }
  if ((a == FileSqlType::DATE && b == FileSqlType::DATETIME) || (b == FileSqlType::DATE && a == FileSqlType::DATETIME)) { return FileSqlType::DATETIME; }
  return FileSqlType::VARCHAR;
}
int csv_record(FILE *file, const FileSqlConfig &config, std::vector<FileSqlCell> &row, int64_t &line)
{
  row.clear();
  FileSqlCell cell;
  bool quoted = false, closed = false, at_start = true, any = false, was_quoted = false;
  int64_t bytes = 0;
  auto finish = [&]() {
    cell.null_ = !was_quoted && cell.text_ == config.nullstr_;
    row.push_back(std::move(cell));
    cell = FileSqlCell();
    at_start = true; closed = false; was_quoted = false;
  };
  for (;;) {
    int ch = fgetc(file);
    if (ch == EOF) {
      if (ferror(file)) { return bad_file("read failed", line + 1); }
      if (!any) { return OB_ITER_END; }
      if (quoted) { return bad_file("unterminated CSV quote", line + 1); }
      finish(); ++line;
      for(const auto &value:row) {int64_t valid=0;if(ObCharset::well_formed_len(CS_TYPE_UTF8MB4_BIN,value.text_.data(),value.text_.size(),valid)!=OB_SUCCESS || valid!=value.text_.size()) {return bad_file("invalid UTF-8",line);}}
      return OB_SUCCESS;
    }
    any = true;
    if (++bytes > FileSqlUtils::MAX_RECORD_BYTES) { return bad_file("record exceeds 16 MiB", line + 1); }
    if((bytes&65535)==0) {int status=THIS_WORKER.check_status();if(status!=OB_SUCCESS) {return status;}}
    if (quoted) {
      if (ch == config.escape_ && config.escape_ != config.quote_) {
        int next = fgetc(file);
        if (next == EOF) { return bad_file("unterminated CSV escape", line + 1); }
        cell.text_.push_back(static_cast<char>(next));
      } else if (ch == config.quote_) {
        int next = fgetc(file);
        if (next == config.quote_) { cell.text_.push_back(config.quote_); }
        else { quoted = false; closed = true; if (next != EOF) { ungetc(next, file); } }
      } else if (ch == '\r') {
        int next = fgetc(file);
        cell.text_.push_back(next == '\n' ? '\n' : '\r');
        if (next != '\n' && next != EOF) { ungetc(next,file); }
      } else { cell.text_.push_back(static_cast<char>(ch)); }
    } else if (ch == config.delimiter_) {
      finish();
      if (row.size() >= FileSqlUtils::MAX_COLUMNS) { return bad_file("too many columns",line+1); }
    } else if (ch == '\n' || ch == '\r') {
      if (ch == '\r') { int next = fgetc(file); if (next != '\n' && next != EOF) { ungetc(next,file); } }
      finish(); ++line;
      for(const auto &value:row) {int64_t valid=0;if(ObCharset::well_formed_len(CS_TYPE_UTF8MB4_BIN,value.text_.data(),value.text_.size(),valid)!=OB_SUCCESS || valid!=value.text_.size()) {return bad_file("invalid UTF-8",line);}}
      return OB_SUCCESS;
    } else if (at_start && ch == config.quote_) {
      quoted = true; was_quoted = true; at_start = false;
    } else if (closed || ch == config.quote_) {
      return bad_file("unexpected byte after CSV quote", line + 1);
    } else { cell.text_.push_back(static_cast<char>(ch)); at_start = false; }
  }
}
int json_record(FILE *file, std::vector<std::string> &names, std::vector<FileSqlCell> &row, int64_t &line)
{
  std::string text;
  for (;;) {
    int ch = fgetc(file);
    if (ch == EOF && text.empty()) { return ferror(file) ? bad_file("read failed",line+1) : OB_ITER_END; }
    if (ch == EOF || ch == '\n') { break; }
    text.push_back(static_cast<char>(ch));
    if (text.size() > FileSqlUtils::MAX_RECORD_BYTES) { return bad_file("record exceeds 16 MiB",line+1); }
    if((text.size()&65535)==0) {int status=THIS_WORKER.check_status();if(status!=OB_SUCCESS) {return status;}}
  }
  ++line;
  if (!text.empty() && text.back() == '\r') { text.pop_back(); }
  if (text.find_first_not_of(" \t\r") == std::string::npos) { return bad_file("empty JSONL record",line); }
  bool quoted=false,escaped=false;int depth=0;int64_t fields=0;
  for(char byte:text) {
    if(quoted) {if(escaped) {escaped=false;}else if(byte=='\\') {escaped=true;}else if(byte=='"') {quoted=false;}}
    else if(byte=='"') {quoted=true;}
    else if(byte=='[' || (byte=='{' && ++depth>1)) {return bad_file("nested JSON is not supported",line);}
    else if(byte=='}') {--depth;}
    else if(byte==':' && ++fields>FileSqlUtils::MAX_COLUMNS) {return bad_file("too many JSON fields",line);}
  }
  int64_t utf8=0;
  if(ObCharset::well_formed_len(CS_TYPE_UTF8MB4_BIN,text.data(),text.size(),utf8)!=OB_SUCCESS || utf8!=text.size()) {return bad_file("invalid UTF-8",line);}
  ObArenaAllocator allocator;
  ObIJsonBase *base = nullptr;
  ObString input(text.size(),text.data());
  int ret = ObJsonBaseFactory::get_json_base(&allocator,input,ObJsonInType::JSON_TREE,ObJsonInType::JSON_TREE,base);
  if (OB_FAIL(ret) || base == nullptr || base->json_type() != ObJsonNodeType::J_OBJECT) { return bad_file("JSONL requires a flat object",line); }
  ObJsonObject *object = static_cast<ObJsonObject *>(base);
  if (object->element_count() > FileSqlUtils::MAX_COLUMNS) { return bad_file("too many JSON fields",line); }
  names.clear(); row.clear();
  for (int64_t i = 0; OB_SUCC(ret) && i < object->element_count(); ++i) {
    ObString key;
    ObJsonNode *value = object->get_value(i);
    if (OB_FAIL(object->get_key(i,key))) { return ret; }
    std::string field_name(key.ptr(),key.length());
    if(field_name.empty() || field_name.find('\0')!=std::string::npos) {return bad_file("invalid JSON field name",line);}
    for(const auto &prior:names) {if(strcasecmp(prior.c_str(),field_name.c_str())==0) {return bad_file("duplicate JSON column name",line);}}
    if (value == nullptr || value->json_type() == ObJsonNodeType::J_OBJECT || value->json_type() == ObJsonNodeType::J_ARRAY) { return bad_file("nested JSON is not supported",line); }
    names.push_back(std::move(field_name));
    FileSqlCell cell;
    cell.null_ = value->json_type() == ObJsonNodeType::J_NULL;
    if (!cell.null_) {
      if (value->json_type() == ObJsonNodeType::J_STRING) { cell.json_string_=true; cell.text_.assign(value->get_data(),value->get_data_length()); }
      else { ObJsonBuffer buffer(&allocator); if (OB_FAIL(value->print(buffer,0))) { return ret; } cell.text_.assign(buffer.ptr(),buffer.length()); }
    }
    row.push_back(std::move(cell));
  }
  return ret;
}
int64_t modified(const struct stat &s) { return static_cast<int64_t>(s.st_mtim.tv_sec) * 1000000000 + s.st_mtim.tv_nsec; }
int64_t changed(const struct stat &s) { return static_cast<int64_t>(s.st_ctim.tv_sec) * 1000000000 + s.st_ctim.tv_nsec; }
void snapshot(FileSqlConfig &config,const struct stat &s) {
  config.device_=s.st_dev; config.inode_=s.st_ino; config.size_=s.st_size; config.mtime_=modified(s); config.ctime_=changed(s);
}
}

FileSqlType FileSqlUtils::infer_type(const std::string &value)
{
  int64_t i, days, micros; double d;
  if (boolean(value)) { return FileSqlType::BOOLEAN; }
  if (integer(value,i)) { return FileSqlType::INTEGER; }
  if (date_time(value,days,micros)) { return value.size()==10 ? FileSqlType::DATE : FileSqlType::DATETIME; }
  // Integral overflow must remain text rather than silently losing precision in DOUBLE.
  if (value.find_first_not_of("+-0123456789") == std::string::npos) { return FileSqlType::VARCHAR; }
  if (real(value,d)) { return FileSqlType::DOUBLE; }
  return FileSqlType::VARCHAR;
}
ObObjType FileSqlUtils::obj_type(FileSqlType type)
{
  switch(type) {
    case FileSqlType::UNKNOWN: return ObNullType;
    case FileSqlType::INTEGER: return ObIntType;
    case FileSqlType::DOUBLE: return ObDoubleType;
    case FileSqlType::BOOLEAN: return ObTinyIntType;
    case FileSqlType::DATE: return ObDateType;
    case FileSqlType::DATETIME: return ObDateTimeType;
    default: return ObVarcharType;
  }
}
const char *FileSqlUtils::type_name(FileSqlType type)
{
  switch(type) {
    case FileSqlType::UNKNOWN: return "NULL";
    case FileSqlType::INTEGER: return "BIGINT";
    case FileSqlType::DOUBLE: return "DOUBLE";
    case FileSqlType::BOOLEAN: return "BOOLEAN";
    case FileSqlType::DATE: return "DATE";
    case FileSqlType::DATETIME: return "DATETIME";
    default: return "VARCHAR";
  }
}
int FileSqlUtils::cell_to_obj(const FileSqlCell &cell,FileSqlType type,ObObj &obj)
{
  if (cell.null_) { obj.set_null(); return OB_SUCCESS; }
  int64_t i,days,micros; double d;
  switch(type) {
    case FileSqlType::INTEGER:
      if (boolean(cell.text_)) { obj.set_int(cell.text_=="true" || cell.text_=="TRUE"); }
      else if (!integer(cell.text_,i)) { return bad_file("integer changed after schema inference"); }
      else { obj.set_int(i); } break;
    case FileSqlType::DOUBLE:
      if (boolean(cell.text_)) { obj.set_double(cell.text_=="true" || cell.text_=="TRUE"); }
      else if (!real(cell.text_,d)) { return bad_file("invalid finite number"); }
      else { obj.set_double(d); } break;
    case FileSqlType::BOOLEAN: obj.set_tinyint(cell.text_=="true" || cell.text_=="TRUE"); break;
    case FileSqlType::DATE:
    case FileSqlType::DATETIME:
      if (!date_time(cell.text_,days,micros)) { return bad_file("invalid date"); }
      if (type==FileSqlType::DATE) { obj.set_date(days); } else { obj.set_datetime(micros); } break;
    default: obj.set_varchar(ObString(cell.text_.size(),cell.text_.data())); obj.set_collation_type(CS_TYPE_UTF8MB4_BIN); break;
  }
  return OB_SUCCESS;
}

int FileSqlUtils::check_access(ObSQLSessionInfo &session,const std::string &path,std::string &canonical,bool pattern,std::string *secure_root)
{
  int ret=OB_SUCCESS;
  ObObj enabled;
  ObArenaAllocator allocator;
  ObString secure;
  if (OB_FAIL(session.get_sys_variable(share::SYS_VAR_ENABLE_FILE_SQL,enabled))) { return ret; }
  if (!enabled.get_bool()) { LOG_USER_ERROR(OB_NOT_SUPPORTED,"file SQL is disabled; enable enable_file_sql"); return OB_NOT_SUPPORTED; }
  if (path.empty() || path.size()>=PATH_MAX || path.find('\0')!=std::string::npos || path.find("://")!=std::string::npos) { return bad_file("invalid local path"); }
  std::string checked=path;
  if (pattern) {
    size_t wildcard=checked.find_first_of("*?[");
    size_t slash=wildcard==std::string::npos?checked.rfind('/'):checked.rfind('/',wildcard);
    checked=slash==std::string::npos ? "." : checked.substr(0,slash+1);
  }
  char resolved[PATH_MAX];
  if (::realpath(checked.c_str(),resolved)==nullptr) { return OB_FILE_NOT_EXIST; }
  if (OB_ISNULL(GCTX.schema_service_)) { return OB_ERR_UNEXPECTED; }
  if (OB_FAIL(share::schema::ObSchemaUtils::get_runtime_varchar_variable(*GCTX.schema_service_,share::SYS_VAR_SECURE_FILE_PRIV,allocator,secure))) { return ret; }
  std::string secure_text(secure.ptr()==nullptr?"":secure.ptr(),secure.length());
  char secure_resolved[PATH_MAX];struct stat secure_stat;
  if(secure.empty() || secure.case_compare("NULL")==0 || realpath(secure_text.c_str(),secure_resolved)==nullptr
      || stat(secure_resolved,&secure_stat)!=0 || !S_ISDIR(secure_stat.st_mode)) {return OB_ERR_NO_PRIVILEGE;}
  if (OB_FAIL(ObResolverUtils::check_secure_path(ObString(secure_resolved),ObString(resolved)))) { return ret; }
  if(secure_root!=nullptr) {*secure_root=secure_resolved;}
  canonical=pattern ? path : std::string(resolved);
  return ret;
}

int FileSqlConfig::encode(ObIAllocator &allocator,ObString &wire) const
{
  std::string data;
  auto number=[&](int64_t n){ data+=std::to_string(n); data+='\n'; };
  auto string=[&](const std::string &s){ number(s.size()); data+=s; };
  number(format_); number(header_); number(static_cast<unsigned char>(delimiter_)); number(static_cast<unsigned char>(quote_)); number(static_cast<unsigned char>(escape_));
  string(path_); string(nullstr_); string(secure_root_); number(device_); number(inode_); number(size_); number(mtime_); number(ctime_); number(columns_.size());
  for (const auto &column:columns_) { string(column.name_); number(static_cast<int64_t>(column.type_)); number(column.nullable_); number(column.length_); }
  if (data.size()>FileSqlUtils::MAX_RECORD_BYTES) { return OB_SIZE_OVERFLOW; }
  return ob_write_string(allocator,ObString(data.size(),data.data()),wire);
}
int FileSqlConfig::decode(const ObString &wire)
{
  if(wire.empty()) {return OB_INVALID_DATA;}
  const char *p=wire.ptr(),*end=p+wire.length();
  bool valid=true;
  auto number=[&](){ const char *start=p; while(p<end && *p!='\n') { ++p; } int64_t n=0; auto r=std::from_chars(start,p,n); if(p==end || r.ec!=std::errc() || r.ptr!=p) { valid=false; } if(p<end) { ++p; } return n; };
  auto string=[&](std::string &s){ int64_t size=number(); if(!valid || size<0 || size>end-p) { valid=false; } else { s.assign(p,size); p+=size; } };
  int64_t format=number(); if(format<CSV || format>GLOB) { return OB_INVALID_DATA; } format_=static_cast<Format>(format);
  header_=number(); delimiter_=number(); quote_=number(); escape_=number(); string(path_); string(nullstr_); string(secure_root_);
  device_=number(); inode_=number(); size_=number(); mtime_=number(); ctime_=number(); int64_t count=number();
  if (!valid || count<0 || count>FileSqlUtils::MAX_COLUMNS) { return OB_INVALID_DATA; }
  columns_.clear();
  for(int64_t i=0;valid && i<count;++i) { FileSqlColumn column; string(column.name_); int64_t type=number(); if(type<0 || type>static_cast<int64_t>(FileSqlType::DATETIME)) {valid=false;} column.type_=static_cast<FileSqlType>(type); column.nullable_=number(); column.length_=number(); columns_.push_back(std::move(column)); }
  return valid && p==end ? OB_SUCCESS : OB_INVALID_DATA;
}

struct FileScanner::State {
  FileSqlConfig config_;
  FILE *file_=nullptr;
  int64_t line_=0;
  std::vector<std::string> names_;
  std::vector<std::string> files_;
  size_t file_index_=0;
  std::vector<int64_t> projection_;
  std::unique_ptr<ParquetReader> parquet_;
};
FileScanner::FileScanner():state_(nullptr) {}
FileScanner::~FileScanner() { close(); }
void FileScanner::close() { if(state_!=nullptr) { if(state_->file_!=nullptr) { fclose(state_->file_); } delete state_; state_=nullptr; } }
int FileScanner::check_unchanged() const
{
  if (state_==nullptr) { return OB_NOT_INIT; }
  if (state_->config_.format_==FileSqlConfig::GLOB) { return OB_SUCCESS; }
  if(state_->file_==nullptr) {return OB_NOT_INIT;}
  struct stat s,path;
  const auto &c=state_->config_;
  if(fstat(fileno(state_->file_),&s)!=0 || stat(c.path_.c_str(),&path)!=0
      || !S_ISREG(s.st_mode) || s.st_dev!=path.st_dev || s.st_ino!=path.st_ino
      || static_cast<int64_t>(s.st_dev)!=c.device_ || static_cast<int64_t>(s.st_ino)!=c.inode_
      || s.st_size!=c.size_ || modified(s)!=c.mtime_ || changed(s)!=c.ctime_) { return bad_file("file changed during query"); }
  return OB_SUCCESS;
}
int FileScanner::open(const FileSqlConfig &config,const std::vector<int64_t> &projection)
{
  close();
  state_=new(std::nothrow) State();
  if(state_==nullptr) { return OB_ALLOCATE_MEMORY_FAILED; }
  state_->config_=config;
  state_->projection_=projection;
  if(config.format_==FileSqlConfig::GLOB) {
    std::vector<std::string> parts;
    for(size_t start=0;start<config.path_.size();) {
      size_t end=config.path_.find('/',start);if(end==std::string::npos) {end=config.path_.size();}
      if(end>start) {parts.push_back(config.path_.substr(start,end-start));}start=end+1;
    }
    if(parts.size()>64) {return bad_file("glob path is too deep");}
    std::vector<std::string> candidates{config.path_[0]=='/'?"/":"."};
    for(size_t part=0;part<parts.size();++part) {
      std::vector<std::string> next;
      bool wildcard=parts[part].find_first_of("*?[")!=std::string::npos;
      for(const auto &base:candidates) {
        if(!wildcard) {next.push_back(base+(base=="/"?"":"/")+parts[part]);continue;}
        char canonical[PATH_MAX];
        if(realpath(base.c_str(),canonical)==nullptr) {continue;}
        ObString secure(config.secure_root_.size(),config.secure_root_.data());
        int access=ObResolverUtils::check_secure_path(secure,ObString(canonical));
        if(access!=OB_SUCCESS) {return access;}
        DIR *directory=opendir(canonical);if(directory==nullptr) {continue;}
        int64_t visited=0;
        while(auto *entry=readdir(directory)) {
          if(++visited>1000000) {closedir(directory);return bad_file("glob directory entry limit exceeded");}
          if((visited&255)==0) {int status=THIS_WORKER.check_status();if(status!=OB_SUCCESS) {closedir(directory);return status;}}
          if(fnmatch(parts[part].c_str(),entry->d_name,FNM_PERIOD)==0) {
            std::string candidate=std::string(canonical)+(std::string(canonical)=="/"?"":"/")+entry->d_name;
            struct stat status;
            if(stat(candidate.c_str(),&status)==0 && (part+1==parts.size()?S_ISREG(status.st_mode):S_ISDIR(status.st_mode))) {next.push_back(std::move(candidate));}
            if(next.size()>10000) {closedir(directory);return bad_file("glob exceeds 10000 paths");}
          }
        }
        closedir(directory);
      }
      if(next.size()>10000) {return bad_file("glob exceeds 10000 paths");}
      candidates=std::move(next);
    }
    std::sort(candidates.begin(),candidates.end());
    for(const auto &candidate:candidates) {
      struct stat status;char canonical[PATH_MAX];
      if(stat(candidate.c_str(),&status)!=0 || !S_ISREG(status.st_mode)) {continue;}
      if(realpath(candidate.c_str(),canonical)==nullptr) {return OB_FILE_NOT_EXIST;}
      ObString secure(config.secure_root_.size(),config.secure_root_.data());
      int access=ObResolverUtils::check_secure_path(secure,ObString(canonical));if(access!=OB_SUCCESS) {return access;}
      state_->files_.push_back(candidate);
    }
    return OB_SUCCESS;
  }
  state_->file_=open_regular(config.path_);
  if(state_->file_==nullptr) { return OB_FILE_NOT_EXIST; }
  int ret=check_unchanged();
  if(OB_SUCC(ret) && config.format_==FileSqlConfig::CSV && config.header_) {std::vector<FileSqlCell> header;ret=csv_record(state_->file_,config,header,state_->line_);}
  if(OB_SUCC(ret) && config.format_==FileSqlConfig::PARQUET) {
    state_->parquet_.reset(new(std::nothrow) ParquetReader());
    if(!state_->parquet_) {ret=OB_ALLOCATE_MEMORY_FAILED;}
    else {ret=state_->parquet_->open(state_->file_,state_->config_,false,projection);}
  }
  return ret;
}
int FileScanner::rescan() { if(state_==nullptr) {return OB_NOT_INIT;} FileSqlConfig config=state_->config_;auto projection=state_->projection_;return open(config,projection); }
int FileScanner::next(std::vector<FileSqlCell> &row)
{
  if(state_==nullptr) {return OB_NOT_INIT;}
  int ret=check_unchanged();
  if(OB_FAIL(ret)) {return ret;}
  if(state_->config_.format_==FileSqlConfig::GLOB) {if(state_->file_index_>=state_->files_.size()) {return OB_ITER_END;} row.assign(1,FileSqlCell()); row[0].null_=false; row[0].text_=state_->files_[state_->file_index_++];return OB_SUCCESS;}
  if(state_->config_.format_==FileSqlConfig::PARQUET) {ret=state_->parquet_->next(row);if(ret==OB_SUCCESS || ret==OB_ITER_END) {int unchanged=check_unchanged();if(unchanged!=OB_SUCCESS) {ret=unchanged;}}return ret;}
  if(state_->config_.format_==FileSqlConfig::CSV) {
    ret=csv_record(state_->file_,state_->config_,row,state_->line_);
    if(OB_SUCC(ret) && row.size()!=state_->config_.columns_.size()) {return bad_file("CSV field count differs from schema",state_->line_);}
  } else {
    std::vector<FileSqlCell> values;
    ret=json_record(state_->file_,state_->names_,values,state_->line_);
    if(OB_SUCC(ret)) {row.assign(state_->config_.columns_.size(),FileSqlCell());for(size_t i=0;i<values.size();++i) {auto col=std::find_if(state_->config_.columns_.begin(),state_->config_.columns_.end(),[&](const FileSqlColumn &c){return c.name_==state_->names_[i];});if(col==state_->config_.columns_.end()) {return bad_file("JSONL schema changed",state_->line_);}row[col-state_->config_.columns_.begin()]=std::move(values[i]);}}
  }
  if(ret==OB_SUCCESS || ret==OB_ITER_END) {int unchanged=check_unchanged();if(unchanged!=OB_SUCCESS) {ret=unchanged;}}
  return ret;
}
int FileSqlUtils::infer(FileSqlConfig &config)
{
  config.columns_.clear();
  if(config.format_==FileSqlConfig::GLOB) {FileSqlColumn c;c.name_="file";c.type_=FileSqlType::VARCHAR;c.length_=PATH_MAX;config.columns_.push_back(c);return OB_SUCCESS;}
  FILE *file=open_regular(config.path_);
  if(file==nullptr) {return OB_FILE_NOT_EXIST;}
  struct stat before,after;
  if(fstat(fileno(file),&before)!=0 || !S_ISREG(before.st_mode)) {fclose(file);return bad_file("input must be a regular file");}
  if(!config.secure_root_.empty()) {
    char descriptor[64],canonical[PATH_MAX];
    snprintf(descriptor,sizeof(descriptor),"/proc/self/fd/%d",fileno(file));
    ObString secure(config.secure_root_.size(),config.secure_root_.data());
    if(realpath(descriptor,canonical)==nullptr) {fclose(file);return OB_FILE_NOT_EXIST;}
    int access=ObResolverUtils::check_secure_path(secure,ObString(canonical));
    if(access!=OB_SUCCESS) {fclose(file);return access;}
  }
  snapshot(config,before);
  int ret=OB_SUCCESS;int64_t line=0,records=0;
  std::vector<FileSqlCell> row;std::vector<std::string> names;
  if(config.format_==FileSqlConfig::PARQUET) {
    ParquetReader reader;ret=reader.open(file,config,true,{});
    if(OB_SUCC(ret) && (fstat(fileno(file),&after)!=0 || before.st_size!=after.st_size || modified(before)!=modified(after) || changed(before)!=changed(after))) {ret=bad_file("file changed during schema inference");}
    fclose(file);return ret;
  }
  while(OB_SUCC(ret)) {
    if((line&255)==0 && OB_FAIL(THIS_WORKER.check_status())) {break;}
    ret=config.format_==FileSqlConfig::CSV ? csv_record(file,config,row,line) : json_record(file,names,row,line);
    if(ret==OB_ITER_END) {ret=OB_SUCCESS;break;}
    if(OB_FAIL(ret)) {break;}
    if(config.format_==FileSqlConfig::CSV && config.columns_.empty()) {
      for(size_t i=0;i<row.size();++i) {
        FileSqlColumn column;column.name_=config.header_ ? row[i].text_ : "column"+std::to_string(i);
        if(i==0 && column.name_.compare(0,3,"\xef\xbb\xbf")==0) {column.name_.erase(0,3);}
        if(column.name_.empty()) {column.name_="column"+std::to_string(i);}
        std::string original=column.name_;int duplicate=2;
        while(std::any_of(config.columns_.begin(),config.columns_.end(),[&](const FileSqlColumn &c){return strcasecmp(c.name_.c_str(),column.name_.c_str())==0;})) {column.name_=original+"__"+std::to_string(duplicate++);}
        config.columns_.push_back(std::move(column));
      }
      if(config.header_) {continue;}
    }
    if(config.format_==FileSqlConfig::JSONL) {
      for(size_t i=0;i<names.size();++i) {auto existing=std::find_if(config.columns_.begin(),config.columns_.end(),[&](const FileSqlColumn &c){return strcasecmp(c.name_.c_str(),names[i].c_str())==0;});if(existing!=config.columns_.end() && existing->name_!=names[i]) {ret=bad_file("JSON column names differ only by case",line);break;}if(existing==config.columns_.end()) {FileSqlColumn column;column.name_=names[i];column.nullable_=records>0;config.columns_.push_back(std::move(column));}}
      if(OB_FAIL(ret)) {break;}
      std::vector<FileSqlCell> aligned(config.columns_.size());
      for(size_t i=0;i<names.size();++i) {auto it=std::find_if(config.columns_.begin(),config.columns_.end(),[&](const FileSqlColumn &c){return c.name_==names[i];});aligned[it-config.columns_.begin()]=std::move(row[i]);}row=std::move(aligned);
    }
    if(row.size()!=config.columns_.size() || config.columns_.size()>MAX_COLUMNS) {ret=bad_file("field count differs from header",line);break;}
    for(size_t i=0;i<row.size();++i) {auto &column=config.columns_[i];if(row[i].null_) {column.nullable_=true;}else {FileSqlType type=infer_type(row[i].text_);if(row[i].json_string_ && type!=FileSqlType::DATE && type!=FileSqlType::DATETIME) {type=FileSqlType::VARCHAR;}column.type_=widen(column.type_,type);column.length_=std::max<int64_t>(column.length_,row[i].text_.size());}}
    ++records;
  }
  if(OB_SUCC(ret) && (fstat(fileno(file),&after)!=0 || before.st_size!=after.st_size || modified(before)!=modified(after) || changed(before)!=changed(after))) {ret=bad_file("file changed during schema inference");}
  fclose(file);
  if(OB_SUCC(ret) && config.columns_.empty()) {ret=bad_file("cannot infer schema from empty input");}
  return ret;
}
} }

#else

namespace oceanbase { namespace sql {
using namespace common;
struct FileScanner::State {};
FileScanner::FileScanner():state_(nullptr) {}
FileScanner::~FileScanner()=default;
void FileScanner::close() {}
int FileScanner::open(const FileSqlConfig &,const std::vector<int64_t> &) {return OB_NOT_SUPPORTED;}
int FileScanner::next(std::vector<FileSqlCell> &) {return OB_NOT_SUPPORTED;}
int FileScanner::rescan() {return OB_NOT_SUPPORTED;}
int FileScanner::check_unchanged() const {return OB_NOT_SUPPORTED;}
int FileSqlConfig::encode(ObIAllocator &,ObString &) const {return OB_NOT_SUPPORTED;}
int FileSqlConfig::decode(const ObString &) {return OB_NOT_SUPPORTED;}
int FileSqlUtils::check_access(ObSQLSessionInfo &,const std::string &,std::string &,bool,std::string *) {return OB_NOT_SUPPORTED;}
int FileSqlUtils::infer(FileSqlConfig &) {return OB_NOT_SUPPORTED;}
ObObjType FileSqlUtils::obj_type(FileSqlType) {return ObNullType;}
const char *FileSqlUtils::type_name(FileSqlType) {return "NULL";}
int FileSqlUtils::cell_to_obj(const FileSqlCell &,FileSqlType,ObObj &) {return OB_NOT_SUPPORTED;}
FileSqlType FileSqlUtils::infer_type(const std::string &) {return FileSqlType::UNKNOWN;}
} }
#endif
