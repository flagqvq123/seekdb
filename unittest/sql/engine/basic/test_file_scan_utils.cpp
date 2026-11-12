#include "sql/engine/basic/ob_file_scan_utils.h"
#include "sql/engine/basic/ob_file_copy_writer.h"
#include <gtest/gtest.h>
#include <unistd.h>
#include <sys/stat.h>
#include <dirent.h>
#include <fstream>
#include <iterator>
#include "lib/encode/ob_base64_encode.h"

namespace oceanbase { namespace sql {
using namespace common;
namespace {
class FileScanTest : public ::testing::Test {
protected:
  void SetUp() override {
    char pattern[]="/tmp/seekdb-file-scan-XXXXXX";
    int descriptor=mkstemp(pattern);
    ASSERT_GE(descriptor,0);
    path_=pattern;
    close(descriptor);
  }
  void TearDown() override {unlink(path_.c_str());}
  void write(const std::string &text) {
    FILE *file=fopen(path_.c_str(),"wb");ASSERT_NE(nullptr,file);
    ASSERT_EQ(text.size(),fwrite(text.data(),1,text.size(),file));
    ASSERT_EQ(0,fclose(file));
  }
  std::string path_;
};
TEST_F(FileScanTest, CsvInferenceAndRescan) {
  write("id,id,active,date,note,empty\r\n1,101,true,2024-02-29,\"a,\r\nb\",\r\n2,102,false,2024-03-01,\"say \"\"hi\"\"\",\r\n");
  FileSqlConfig config;config.path_=path_;
  ASSERT_EQ(OB_SUCCESS,FileSqlUtils::infer(config));
  ASSERT_EQ(6,config.columns_.size());
  EXPECT_EQ("id__2",config.columns_[1].name_);
  EXPECT_EQ(FileSqlType::BOOLEAN,config.columns_[2].type_);
  EXPECT_EQ(FileSqlType::DATE,config.columns_[3].type_);
  EXPECT_EQ(FileSqlType::UNKNOWN,config.columns_[5].type_);
  EXPECT_TRUE(config.columns_[5].nullable_);
  FileScanner scanner;ASSERT_EQ(OB_SUCCESS,scanner.open(config,{0,4}));
  std::vector<FileSqlCell> row;
  ASSERT_EQ(OB_SUCCESS,scanner.next(row));EXPECT_EQ("a,\nb",row[4].text_);
  ASSERT_EQ(OB_SUCCESS,scanner.next(row));EXPECT_EQ("say \"hi\"",row[4].text_);
  EXPECT_EQ(OB_ITER_END,scanner.next(row));
  ASSERT_EQ(OB_SUCCESS,scanner.rescan());ASSERT_EQ(OB_SUCCESS,scanner.next(row));EXPECT_EQ("1",row[0].text_);
}
TEST_F(FileScanTest, JsonUnionPreservesStrings) {
  write("{\"id\":\"001\",\"flag\":\"true\",\"empty\":null}\n{\"id\":\"002\",\"extra\":4}\n");
  FileSqlConfig config;config.path_=path_;config.format_=FileSqlConfig::JSONL;
  ASSERT_EQ(OB_SUCCESS,FileSqlUtils::infer(config));
  ASSERT_EQ(4,config.columns_.size());
  EXPECT_EQ(FileSqlType::VARCHAR,config.columns_[0].type_);
  EXPECT_EQ(FileSqlType::VARCHAR,config.columns_[1].type_);
  EXPECT_EQ(FileSqlType::UNKNOWN,config.columns_[2].type_);
  EXPECT_TRUE(config.columns_[1].nullable_);EXPECT_TRUE(config.columns_[3].nullable_);
  FileScanner scanner;ASSERT_EQ(OB_SUCCESS,scanner.open(config,{}));std::vector<FileSqlCell> row;
  ASSERT_EQ(OB_SUCCESS,scanner.next(row));EXPECT_EQ("001",row[0].text_);EXPECT_TRUE(row[3].null_);
}
TEST_F(FileScanTest, RejectsChangedFileAndInvalidRecords) {
  write("id\n1\n");FileSqlConfig config;config.path_=path_;
  ASSERT_EQ(OB_SUCCESS,FileSqlUtils::infer(config));
  FileScanner scanner;ASSERT_EQ(OB_SUCCESS,scanner.open(config,{}));
  write("id\n22\n");std::vector<FileSqlCell> row;
  EXPECT_NE(OB_SUCCESS,scanner.next(row));scanner.close();
  write("id,note\n1,\"unterminated\n");EXPECT_NE(OB_SUCCESS,FileSqlUtils::infer(config));
  config.format_=FileSqlConfig::JSONL;write("{\"nested\":[]}\n");EXPECT_NE(OB_SUCCESS,FileSqlUtils::infer(config));
  config.path_+="-missing";EXPECT_EQ(OB_FILE_NOT_EXIST,scanner.open(config,{}));EXPECT_EQ(OB_NOT_INIT,scanner.next(row));
}
TEST_F(FileScanTest, RejectsFifoWithoutWaitingForAWriter) {
  ASSERT_EQ(0,unlink(path_.c_str()));ASSERT_EQ(0,mkfifo(path_.c_str(),0600));
  FileSqlConfig config;config.path_=path_;
  EXPECT_NE(OB_SUCCESS,FileSqlUtils::infer(config));
  FileScanner scanner;EXPECT_NE(OB_SUCCESS,scanner.open(config,{}));
}
TEST_F(FileScanTest, ParquetSnappyAndProjection) {
  std::ifstream input(std::string(FILE_SQL_TEST_DATA_DIR)+"/simple.parquet.b64");
  ASSERT_TRUE(input.good());
  std::string encoded((std::istreambuf_iterator<char>(input)),std::istreambuf_iterator<char>());
  std::vector<uint8_t> decoded(encoded.size());int64_t size=0;
  ASSERT_EQ(OB_SUCCESS,ObBase64Encoder::decode(encoded.data(),encoded.size(),decoded.data(),decoded.size(),size,true));
  write(std::string(reinterpret_cast<const char*>(decoded.data()),size));
  FileSqlConfig config;config.path_=path_;config.format_=FileSqlConfig::PARQUET;
  ASSERT_EQ(OB_SUCCESS,FileSqlUtils::infer(config));ASSERT_EQ(1,config.columns_.size());
  EXPECT_EQ("c1",config.columns_[0].name_);EXPECT_EQ(FileSqlType::INTEGER,config.columns_[0].type_);
  FileScanner scanner;ASSERT_EQ(OB_SUCCESS,scanner.open(config,{0}));std::vector<FileSqlCell> row;
  for(int i=0;i<7;++i) {ASSERT_EQ(OB_SUCCESS,scanner.next(row));ASSERT_EQ(1,row.size());EXPECT_EQ(std::to_string(i),row[0].text_);}
  EXPECT_EQ(OB_ITER_END,scanner.next(row));ASSERT_EQ(OB_SUCCESS,scanner.rescan());ASSERT_EQ(OB_SUCCESS,scanner.next(row));EXPECT_EQ("0",row[0].text_);
  ASSERT_EQ(OB_SUCCESS,scanner.open(config,{}));int count=0;while(scanner.next(row)==OB_SUCCESS) {++count;}EXPECT_EQ(7,count);
  scanner.close();
  decoded[0]='X';write(std::string(reinterpret_cast<const char*>(decoded.data()),size));EXPECT_NE(OB_SUCCESS,FileSqlUtils::infer(config));
}
TEST_F(FileScanTest, ParquetMultipleGroupsNullsAndDictionary) {
  std::ifstream input(std::string(FILE_SQL_UNIT_DATA_DIR)+"/file_sql_multigroup.parquet.b64");ASSERT_TRUE(input.good());
  std::string encoded((std::istreambuf_iterator<char>(input)),std::istreambuf_iterator<char>());
  std::vector<uint8_t> decoded(encoded.size());int64_t size=0;
  ASSERT_EQ(OB_SUCCESS,ObBase64Encoder::decode(encoded.data(),encoded.size(),decoded.data(),decoded.size(),size,true));
  write(std::string(reinterpret_cast<const char*>(decoded.data()),size));
  FileSqlConfig config;config.path_=path_;config.format_=FileSqlConfig::PARQUET;
  ASSERT_EQ(OB_SUCCESS,FileSqlUtils::infer(config));ASSERT_EQ(6,config.columns_.size());
  EXPECT_EQ(FileSqlType::DATE,config.columns_[4].type_);EXPECT_EQ(FileSqlType::DATETIME,config.columns_[5].type_);
  FileScanner scanner;ASSERT_EQ(OB_SUCCESS,scanner.open(config,{0,1,2,3,4,5}));std::vector<FileSqlCell> row;
  const char *names[]={"甲","乙","甲","乙","","甲"};
  for(int i=0;i<6;++i) {
    ASSERT_EQ(OB_SUCCESS,scanner.next(row));ASSERT_EQ(6,row.size());
    EXPECT_EQ(i==1,row[0].null_);if(i!=1) {EXPECT_EQ(std::to_string(i+1),row[0].text_);}
    EXPECT_EQ(i==4,row[1].null_);if(i!=4) {EXPECT_EQ(names[i],row[1].text_);}
    EXPECT_EQ(i%2==0?"true":"false",row[2].text_);
    if(i==0) {EXPECT_EQ("1970-01-01",row[4].text_);EXPECT_EQ("1970-01-01 00:00:00.000000",row[5].text_);}
    if(i==3) {EXPECT_EQ("1969-12-31 23:59:59.999999",row[5].text_);ObObj value;ASSERT_EQ(OB_SUCCESS,FileSqlUtils::cell_to_obj(row[5],FileSqlType::DATETIME,value));EXPECT_EQ(-1,value.get_datetime());}
  }
  EXPECT_EQ(OB_ITER_END,scanner.next(row));ASSERT_EQ(OB_SUCCESS,scanner.rescan());ASSERT_EQ(OB_SUCCESS,scanner.next(row));EXPECT_EQ("甲",row[1].text_);
}
TEST(FileSqlTypes, BoundariesAndCalendar) {
  FileSqlConfig config;EXPECT_EQ(OB_INVALID_DATA,config.decode(ObString()));
  EXPECT_EQ(FileSqlType::INTEGER,FileSqlUtils::infer_type("9223372036854775807"));
  EXPECT_EQ(FileSqlType::INTEGER,FileSqlUtils::infer_type("-9223372036854775808"));
  EXPECT_EQ(FileSqlType::VARCHAR,FileSqlUtils::infer_type("9223372036854775808"));
  EXPECT_EQ(FileSqlType::DATE,FileSqlUtils::infer_type("2000-02-29"));
  EXPECT_EQ(FileSqlType::VARCHAR,FileSqlUtils::infer_type("1900-02-29"));
  EXPECT_EQ(FileSqlType::VARCHAR,FileSqlUtils::infer_type("2026-04-31"));
  EXPECT_EQ(FileSqlType::VARCHAR,FileSqlUtils::infer_type("1e999"));
  EXPECT_EQ(FileSqlType::DOUBLE,FileSqlUtils::infer_type("1e-308"));
  EXPECT_EQ(FileSqlType::DATETIME,FileSqlUtils::infer_type("2026-08-03T12:13:14"));
  FileSqlCell cell;cell.null_=false;cell.text_="1969-12-31 23:59:59.999999";ObObj value;
  ASSERT_EQ(OB_SUCCESS,FileSqlUtils::cell_to_obj(cell,FileSqlType::DATETIME,value));EXPECT_EQ(-1,value.get_datetime());
  cell.text_="1969-12-31";ASSERT_EQ(OB_SUCCESS,FileSqlUtils::cell_to_obj(cell,FileSqlType::DATE,value));EXPECT_EQ(-1,value.get_date());
}
TEST(FileSqlCopy, RecordLimitIncludesNewlineAndSeparators) {
  char directory[]="/tmp/seekdb-file-copy-XXXXXX";ASSERT_NE(nullptr,mkdtemp(directory));
  struct Cleanup {
    const char *directory_;
    ~Cleanup() {unlink((std::string(directory_)+"/export.csv").c_str());rmdir(directory_);}
  } cleanup{directory};
  FileSqlConfig config;config.path_=std::string(directory)+"/export.csv";config.secure_root_=directory;config.header_=false;
  FileCopyWriter writer;ASSERT_EQ(OB_SUCCESS,writer.open(config));
  FileSqlCell cell;cell.null_=false;cell.text_.assign(FileSqlUtils::MAX_RECORD_BYTES-1,'x');
  ASSERT_EQ(OB_SUCCESS,writer.row({cell}));ASSERT_EQ(OB_SUCCESS,writer.finish());
  FileSqlConfig input;input.path_=config.path_;input.header_=false;
  ASSERT_EQ(OB_SUCCESS,FileSqlUtils::infer(input));FileScanner scanner;ASSERT_EQ(OB_SUCCESS,scanner.open(input,{0}));
  std::vector<FileSqlCell> result;ASSERT_EQ(OB_SUCCESS,scanner.next(result));EXPECT_EQ(FileSqlUtils::MAX_RECORD_BYTES-1,result[0].text_.size());
  scanner.close();ASSERT_EQ(0,unlink(config.path_.c_str()));
  ASSERT_EQ(OB_SUCCESS,writer.open(config));
  EXPECT_EQ(OB_SIZE_OVERFLOW,writer.row({cell,FileSqlCell()}));writer.abort();
  cell.text_.push_back('x');ASSERT_EQ(OB_SUCCESS,writer.open(config));EXPECT_EQ(OB_SIZE_OVERFLOW,writer.row({cell}));writer.abort();
  EXPECT_NE(0,access(config.path_.c_str(),F_OK));
  DIR *dir=opendir(directory);ASSERT_NE(nullptr,dir);int entries=0;
  while(auto *entry=readdir(dir)) {if(strcmp(entry->d_name,".") && strcmp(entry->d_name,"..")) {++entries;}}
  closedir(dir);EXPECT_EQ(0,entries);
}
TEST(FileSqlCopy, RejectsDirectoryMovedOutsideSecureRootBeforePublish) {
  char directory[]="/tmp/seekdb-copy-move-XXXXXX";ASSERT_NE(nullptr,mkdtemp(directory));
  const std::string base(directory),secure=base+"/secure",outside=base+"/outside";
  struct Cleanup {
    std::string base_;
    ~Cleanup() {
      for(const char *parent:{"/secure/sub","/outside/sub"}) {unlink((base_+parent+"/export.csv").c_str());rmdir((base_+parent).c_str());}
      rmdir((base_+"/secure").c_str());rmdir((base_+"/outside").c_str());rmdir(base_.c_str());
    }
  } cleanup{base};
  ASSERT_EQ(0,mkdir(secure.c_str(),0700));ASSERT_EQ(0,mkdir(outside.c_str(),0700));ASSERT_EQ(0,mkdir((secure+"/sub").c_str(),0700));
  FileSqlConfig config;config.path_=secure+"/sub/export.csv";config.secure_root_=secure;config.header_=false;
  FileCopyWriter writer;ASSERT_EQ(OB_SUCCESS,writer.open(config));
  ASSERT_EQ(0,rename((secure+"/sub").c_str(),(outside+"/sub").c_str()));
  FileSqlCell cell;cell.null_=false;cell.text_="value";ASSERT_EQ(OB_SUCCESS,writer.row({cell}));
  EXPECT_NE(OB_SUCCESS,writer.finish());
  EXPECT_NE(0,access(config.path_.c_str(),F_OK));EXPECT_NE(0,access((outside+"/sub/export.csv").c_str(),F_OK));
  DIR *dir=opendir((outside+"/sub").c_str());ASSERT_NE(nullptr,dir);int entries=0;
  while(auto *entry=readdir(dir)) {if(strcmp(entry->d_name,".") && strcmp(entry->d_name,"..")) {++entries;}}
  closedir(dir);EXPECT_EQ(0,entries);
}
} } }
