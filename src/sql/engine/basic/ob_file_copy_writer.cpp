#define USING_LOG_PREFIX SQL_ENG
#include "sql/engine/basic/ob_file_copy_writer.h"

#if defined(__linux__)
#include "sql/resolver/ob_resolver_utils.h"
#include <unistd.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <cerrno>
#include <limits.h>
#include <ctime>
#include <cstring>
namespace oceanbase { namespace sql {
using namespace common;
FileCopyWriter::~FileCopyWriter() {abort();}
void FileCopyWriter::abort() {
  if(file_!=nullptr) {fclose(file_);file_=nullptr;}
  if(!temporary_.empty() && directory_>=0) {unlinkat(directory_,temporary_.c_str(),0);temporary_.clear();}
  if(directory_>=0) {close(directory_);directory_=-1;}
}
int FileCopyWriter::open(const FileSqlConfig &config) {
  abort();config_=config;
  struct stat existing;
  if(lstat(config.path_.c_str(),&existing)==0) {LOG_USER_ERROR(OB_FILE_ALREADY_EXIST);return OB_FILE_ALREADY_EXIST;}
  if(errno!=ENOENT) {return OB_IO_ERROR;}
  std::string parent=config.path_.substr(0,config.path_.rfind('/')+1);char real[PATH_MAX];
  if(realpath(parent.c_str(),real)==nullptr) {return OB_FILE_NOT_EXIST;}
  ObString secure(config.secure_root_.size(),config.secure_root_.data());
  int ret=ObResolverUtils::check_secure_path(secure,ObString(real));if(ret!=OB_SUCCESS) {return ret;}
  directory_=::open(real,O_DIRECTORY|O_RDONLY|O_NOFOLLOW|O_CLOEXEC);
  if(directory_<0) {return OB_IO_ERROR;}
  char descriptor[64],opened[PATH_MAX];snprintf(descriptor,sizeof(descriptor),"/proc/self/fd/%d",directory_);
  if(realpath(descriptor,opened)==nullptr || (ret=ObResolverUtils::check_secure_path(secure,ObString(opened)))!=OB_SUCCESS) {abort();return ret==OB_SUCCESS?OB_IO_ERROR:ret;}
  filename_=config.path_.substr(config.path_.rfind('/')+1);
  if(fstatat(directory_,filename_.c_str(),&existing,AT_SYMLINK_NOFOLLOW)==0) {abort();LOG_USER_ERROR(OB_FILE_ALREADY_EXIST);return OB_FILE_ALREADY_EXIST;}
  if(errno!=ENOENT) {abort();return OB_IO_ERROR;}
  int fd=-1;
  for(int i=0;i<100 && fd<0;++i) {
    temporary_=".seekdb-copy-"+std::to_string(getpid())+"-"+std::to_string(std::time(nullptr))+"-"+std::to_string(i);
    fd=openat(directory_,temporary_.c_str(),O_WRONLY|O_CREAT|O_EXCL|O_NOFOLLOW|O_CLOEXEC,0600);
    if(fd<0 && errno!=EEXIST) {temporary_.clear();abort();return OB_IO_ERROR;}
  }
  if(fd<0) {temporary_.clear();abort();return OB_IO_ERROR;}
  file_=fdopen(fd,"wb");
  if(file_==nullptr) {close(fd);abort();return OB_IO_ERROR;}
  if(config.header_) {std::vector<FileSqlCell> header;for(const auto &column:config.columns_) {FileSqlCell cell;cell.null_=false;cell.text_=column.name_;header.push_back(std::move(cell));}ret=row(header);}
  return ret;
}
int FileCopyWriter::row(const std::vector<FileSqlCell> &row) {
  if(file_==nullptr) {return OB_NOT_INIT;}
  std::string output;
  const size_t payload_limit=FileSqlUtils::MAX_RECORD_BYTES-1;
  for(size_t i=0;i<row.size();++i) {
    if(i) {if(output.size()>=payload_limit) {return OB_SIZE_OVERFLOW;}output.push_back(config_.delimiter_);}
    const size_t available=payload_limit-output.size();
    if(row[i].null_) {if(config_.nullstr_.size()>available) {return OB_SIZE_OVERFLOW;}output+=config_.nullstr_;continue;}
    const auto &text=row[i].text_;
    if(text.size()>available) {return OB_SIZE_OVERFLOW;}
    bool quote=text.empty() || text==config_.nullstr_ || text.find(config_.delimiter_)!=std::string::npos || text.find(config_.quote_)!=std::string::npos || text.find('\r')!=std::string::npos || text.find('\n')!=std::string::npos;
    size_t extra=text.size()+(quote?2:0);
    if(quote) {for(char c:text) {extra+=c==config_.quote_ || (config_.escape_!=config_.quote_ && c==config_.escape_);}}
    if(extra>available) {return OB_SIZE_OVERFLOW;}
    if(quote) {output.push_back(config_.quote_);}
    for(char c:text) {if(quote && (c==config_.quote_ || (config_.escape_!=config_.quote_ && c==config_.escape_))) {output.push_back(config_.escape_);}output.push_back(c);}
    if(quote) {output.push_back(config_.quote_);}
  }
  output.push_back('\n');
  return fwrite(output.data(),1,output.size(),file_)==output.size()?OB_SUCCESS:OB_IO_ERROR;
}
int FileCopyWriter::finish() {
  if(file_==nullptr) {return OB_NOT_INIT;}
  int ret=OB_SUCCESS;
  struct stat written{};
  auto check_directory=[&]() -> int {
    char descriptor[64],current[PATH_MAX],requested[PATH_MAX];
    snprintf(descriptor,sizeof(descriptor),"/proc/self/fd/%d",directory_);
    std::string parent=config_.path_.substr(0,config_.path_.rfind('/')+1);
    struct stat opened,at_path;
    if(realpath(descriptor,current)==nullptr || realpath(parent.c_str(),requested)==nullptr
        || strcmp(current,requested)!=0 || fstat(directory_,&opened)!=0 || stat(requested,&at_path)!=0
        || opened.st_dev!=at_path.st_dev || opened.st_ino!=at_path.st_ino) {
      LOG_USER_ERROR(OB_INVALID_ARGUMENT,"file_sql: COPY directory changed during query");
      return OB_INVALID_ARGUMENT;
    }
    ObString secure(config_.secure_root_.size(),config_.secure_root_.data());
    return ObResolverUtils::check_secure_path(secure,ObString(current));
  };
  if(fflush(file_)!=0 || fsync(fileno(file_))!=0) {ret=OB_IO_ERROR;}
  if(fstat(fileno(file_),&written)!=0) {ret=OB_IO_ERROR;}
  if(fclose(file_)!=0) {ret=OB_IO_ERROR;}file_=nullptr;
  if(ret==OB_SUCCESS) {ret=check_directory();}
  bool published=false;
  if(ret==OB_SUCCESS) {
    if(linkat(directory_,temporary_.c_str(),directory_,filename_.c_str(),0)!=0) {ret=errno==EEXIST?OB_FILE_ALREADY_EXIST:OB_IO_ERROR;if(ret==OB_FILE_ALREADY_EXIST) {LOG_USER_ERROR(OB_FILE_ALREADY_EXIST);}}
    else {published=true;}
  }
  if(published && (ret=check_directory())!=OB_SUCCESS) {
    struct stat target;
    if(fstatat(directory_,filename_.c_str(),&target,AT_SYMLINK_NOFOLLOW)==0
        && target.st_dev==written.st_dev && target.st_ino==written.st_ino) {unlinkat(directory_,filename_.c_str(),0);}
  }
  unlinkat(directory_,temporary_.c_str(),0);temporary_.clear();close(directory_);directory_=-1;return ret;
}
} }

#else

namespace oceanbase { namespace sql {
FileCopyWriter::~FileCopyWriter()=default;
void FileCopyWriter::abort() {}
int FileCopyWriter::open(const FileSqlConfig &) {return common::OB_NOT_SUPPORTED;}
int FileCopyWriter::row(const std::vector<FileSqlCell> &) {return common::OB_NOT_SUPPORTED;}
int FileCopyWriter::finish() {return common::OB_NOT_SUPPORTED;}
} }
#endif
