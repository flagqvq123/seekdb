#define USING_LOG_PREFIX SQL_ENG
#include "sql/engine/basic/ob_file_parquet_reader.h"

#if defined(__linux__)
#include <algorithm>
#include <array>
#include <cmath>
#include <cstring>
#include <limits>
#include <map>
#include <sstream>
#include <iomanip>
#include "lib/worker.h"

namespace oceanbase { namespace sql {
using namespace common;
namespace {
constexpr size_t MAX_PAGE=64*1024*1024;
struct CompactValue {
  int64_t number_=0;
  std::string text_;
  std::map<int,CompactValue> fields_;
  std::vector<CompactValue> list_;
  const CompactValue &field(int id) const {static const CompactValue empty;auto it=fields_.find(id);return it==fields_.end()?empty:it->second;}
  bool has(int id) const {return fields_.find(id)!=fields_.end();}
};
class CompactReader {
public:
  CompactReader(const unsigned char *data,size_t size):data_(data),size_(size){}
  uint8_t byte() {if(pos_>=size_) {valid_=false;return 0;}return data_[pos_++];}
  uint64_t varint() {uint64_t v=0;for(int shift=0;shift<64;shift+=7) {uint8_t b=byte();if(shift==63 && (b&0xfe)) {valid_=false;return 0;}v|=static_cast<uint64_t>(b&127)<<shift;if(!(b&128)) {return v;}}valid_=false;return 0;}
  int64_t integer() {uint64_t n=varint();return static_cast<int64_t>((n>>1)^-static_cast<int64_t>(n&1));}
  CompactValue structure(int depth=0) {
    CompactValue result;int id=0;
    if(depth>32) {valid_=false;return result;}
    while(valid_) {uint8_t head=byte();if(head==0) {break;}int delta=head>>4;int type=head&15;int64_t next_id=delta?static_cast<int64_t>(id)+delta:integer();if(next_id<=0 || next_id>32767) {valid_=false;break;}id=next_id;if(id<=0 || result.fields_.size()>4096) {valid_=false;break;}result.fields_[id]=value(type,depth+1);}
    return result;
  }
  CompactValue value(int type,int depth) {
    CompactValue result;
    if(depth>32 || ++nodes_>100000 || ((nodes_&1023)==0 && THIS_WORKER.check_status()!=OB_SUCCESS)) {valid_=false;return result;}
    switch(type) {
      case 1:result.number_=1;break;
      case 2:result.number_=0;break;
      case 3:result.number_=static_cast<int8_t>(byte());break;
      case 4:case 5:case 6:result.number_=integer();break;
      case 7:for(int i=0;i<8;++i) {byte();}break;
      case 8:{uint64_t n=varint();if(n>size_-std::min(pos_,size_)) {valid_=false;break;}result.text_.assign(reinterpret_cast<const char*>(data_+pos_),n);pos_+=n;break;}
      case 9:case 10:{uint8_t header=byte();uint64_t n=header>>4;int element=header&15;if(n==15) {n=varint();}if(n>100000 || n>size_-std::min(pos_,size_)) {valid_=false;break;}for(uint64_t i=0;valid_ && i<n;++i) {if(element==1 || element==2) {uint8_t boolean=byte();if(boolean!=1 && boolean!=2) {valid_=false;break;}CompactValue item;item.number_=boolean==1;result.list_.push_back(std::move(item));}else {result.list_.push_back(value(element,depth+1));}}break;}
      case 11:{uint64_t n=varint();if(n>100000 || n>(size_-std::min(pos_,size_))/2) {valid_=false;break;}uint8_t types=n?byte():0;for(uint64_t i=0;valid_ && i<n;++i) {value(types>>4,depth+1);value(types&15,depth+1);}break;}
      case 12:result=structure(depth+1);break;
      default:valid_=false;
    }
    return result;
  }
  bool valid_=true;
  size_t pos_=0;
private:
  const unsigned char *data_;size_t size_;
  size_t nodes_=0;
};
uint32_t little32(const unsigned char *p) {return p[0]|uint32_t(p[1])<<8|uint32_t(p[2])<<16|uint32_t(p[3])<<24;}
uint64_t little64(const unsigned char *p) {return little32(p)|uint64_t(little32(p+4))<<32;}
int invalid() {int status=THIS_WORKER.check_status();if(status!=OB_SUCCESS) {return status;}LOG_USER_ERROR(OB_INVALID_ARGUMENT,"file_sql: invalid or unsupported Parquet encoding");return OB_INVALID_ARGUMENT;}
bool read_at(FILE *file,int64_t offset,size_t size,std::vector<unsigned char> &data) {
  if(offset<0 || size>MAX_PAGE || fseeko(file,offset,SEEK_SET)!=0) {return false;}data.resize(size);return size==0 || fread(data.data(),1,size,file)==size;
}
bool snappy(const std::vector<unsigned char> &input,size_t expected,std::vector<unsigned char> &output) {
  if(expected>MAX_PAGE) {return false;}CompactReader reader(input.data(),input.size());uint64_t length=reader.varint();if(!reader.valid_ || length!=expected) {return false;}output.clear();output.reserve(length);size_t checked=0;
  while(reader.valid_ && reader.pos_<input.size() && output.size()<length) {
    if(output.size()>=checked+65536) {checked=output.size();if(THIS_WORKER.check_status()!=OB_SUCCESS) {return false;}}
    uint8_t tag=reader.byte();size_t count=0,offset=0;
    if((tag&3)==0) {count=tag>>2;if(count<60) {++count;}else {size_t bytes=count-59;count=0;if(bytes>4) {return false;}for(size_t i=0;i<bytes;++i) {count|=size_t(reader.byte())<<(8*i);}++count;}if(count>length-output.size() || count>input.size()-reader.pos_) {return false;}output.insert(output.end(),input.begin()+reader.pos_,input.begin()+reader.pos_+count);reader.pos_+=count;}
    else {
      if((tag&3)==1) {count=4+((tag>>2)&7);offset=((tag&0xe0)<<3)|reader.byte();}
      else if((tag&3)==2) {count=1+(tag>>2);offset=reader.byte();offset|=size_t(reader.byte())<<8;}
      else {count=1+(tag>>2);for(int i=0;i<4;++i) {offset|=size_t(reader.byte())<<(8*i);}}
      if(!reader.valid_ || offset==0 || offset>output.size() || count>length-output.size()) {return false;}
      for(size_t i=0;i<count;++i) {output.push_back(output[output.size()-offset]);}
    }
  }
  return reader.valid_ && reader.pos_==input.size() && output.size()==length;
}
bool hybrid(const unsigned char *data,size_t size,int width,size_t count,std::vector<uint32_t> &values,size_t &used) {
  values.clear();used=0;if(width<0 || width>32) {return false;}if(width==0) {values.assign(count,0);return true;}
  CompactReader reader(data,size);size_t checked=0;
  while(reader.valid_ && values.size()<count) {
    if(values.size()>=checked) {checked=values.size()+4096;if(THIS_WORKER.check_status()!=OB_SUCCESS) {return false;}}
    uint64_t run=reader.varint();if(run==0) {return false;}
    if(run&1) {
      uint64_t groups=run>>1;if(groups>(MAX_PAGE/width)) {return false;}size_t bytes=groups*width;
      if(bytes>size-reader.pos_) {return false;}
      for(size_t i=0;i<groups*8 && values.size()<count;++i) {if((i&4095)==0 && THIS_WORKER.check_status()!=OB_SUCCESS) {return false;}uint32_t value=0;for(int b=0;b<width;++b) {size_t bit=i*width+b;value|=uint32_t((data[reader.pos_+bit/8]>>(bit%8))&1)<<b;}values.push_back(value);}reader.pos_+=bytes;
    } else {
      uint64_t n=run>>1;uint32_t value=0;for(int i=0;i<(width+7)/8;++i) {value|=uint32_t(reader.byte())<<(8*i);}if(n>count-values.size()) {return false;}values.insert(values.end(),n,value);
    }
  }
  used=reader.pos_;return reader.valid_ && values.size()==count;
}
std::string number(double value) {std::ostringstream out;out<<std::setprecision(17)<<value;return out.str();}
std::string date_string(int64_t days,int64_t time=-1) {
  int64_t z=days+719468;int64_t era=(z>=0?z:z-146096)/146097;unsigned doe=z-era*146097;unsigned yoe=(doe-doe/1460+doe/36524-doe/146096)/365;int y=yoe+era*400;unsigned doy=doe-(365*yoe+yoe/4-yoe/100);unsigned mp=(5*doy+2)/153;unsigned d=doy-(153*mp+2)/5+1;unsigned m=mp+(mp<10?3:-9);y+=m<=2;
  char text[64];if(time<0) {snprintf(text,sizeof(text),"%04d-%02u-%02u",y,m,d);}else {snprintf(text,sizeof(text),"%04d-%02u-%02u %02ld:%02ld:%02ld.%06ld",y,m,d,time/3600000000,(time/60000000)%60,(time/1000000)%60,time%1000000);}return text;
}
struct ParquetColumn {
  int physical_=-1,converted_=-1,length_=0,unit_=0;
  bool optional_=false;
};
struct Chunk {
  int64_t offset_=0,end_=0,values_=0;
  int codec_=0;
};
struct Group {int64_t rows_=0;std::vector<Chunk> chunks_;};
struct PageCursor {
  Chunk chunk_;
  ParquetColumn column_;
  int64_t offset_=0,seen_=0;
  std::vector<FileSqlCell> page_,dictionary_;
  size_t index_=0;
};
bool plain(const unsigned char *data,size_t size,size_t count,const ParquetColumn &column,std::vector<FileSqlCell> &values) {
  values.clear();size_t pos=0;size_t memory=count*sizeof(FileSqlCell);if(memory>MAX_PAGE) {return false;}
  for(size_t i=0;i<count;++i) {
    if((i&255)==0 && THIS_WORKER.check_status()!=OB_SUCCESS) {return false;}
    FileSqlCell cell;cell.null_=false;
    size_t bytes=column.physical_==1 || column.physical_==4?4:column.physical_==2 || column.physical_==5?8:column.physical_==3?12:column.physical_==7?column.length_:0;
    if(column.physical_==0) {if(i/8>=size) {return false;}cell.text_=((data[i/8]>>(i%8))&1)?"true":"false";}
    else if(column.physical_==6) {if(size-pos<4) {return false;}bytes=little32(data+pos);pos+=4;if(bytes>size-pos) {return false;}cell.text_.assign(reinterpret_cast<const char*>(data+pos),bytes);pos+=bytes;}
    else {
      if(bytes==0 || bytes>size-pos) {return false;}
      if(column.physical_==1 || column.physical_==2) {
        int64_t value=column.physical_==1?static_cast<int32_t>(little32(data+pos)):static_cast<int64_t>(little64(data+pos));
        if(column.converted_==6) {cell.text_=date_string(value);}
        else if(column.converted_==9 || column.converted_==10 || column.unit_!=0) {int64_t micros=value;if(column.converted_==9 || column.unit_==1) {if(value>INT64_MAX/1000 || value<INT64_MIN/1000) {return false;}micros*=1000;}else if(column.unit_==3) {micros/=1000;}int64_t days=micros/86400000000;int64_t rest=micros%86400000000;if(rest<0) {--days;rest+=86400000000;}cell.text_=date_string(days,rest);}
        else if(column.converted_==14) {cell.text_=std::to_string(static_cast<uint64_t>(value));}
        else if(column.converted_>=11 && column.converted_<=13) {cell.text_=std::to_string(static_cast<uint32_t>(value));}
        else {cell.text_=std::to_string(value);}
      } else if(column.physical_==3) {int64_t days=static_cast<int64_t>(little32(data+pos+8))-2440588;cell.text_=date_string(days,little64(data+pos)/1000);}
      else if(column.physical_==4) {uint32_t bits=little32(data+pos);float value;memcpy(&value,&bits,4);if(!std::isfinite(value)) {return false;}cell.text_=number(value);}
      else if(column.physical_==5) {uint64_t bits=little64(data+pos);double value;memcpy(&value,&bits,8);if(!std::isfinite(value)) {return false;}cell.text_=number(value);}
      else {cell.text_.assign(reinterpret_cast<const char*>(data+pos),bytes);}
      pos+=bytes;
    }
    if(cell.text_.size()>MAX_PAGE-memory) {return false;}memory+=cell.text_.size();values.push_back(std::move(cell));
  }
  return true;
}
bool delta_numbers(const unsigned char *data,size_t size,std::vector<int64_t> &values,size_t &used) {
  CompactReader reader(data,size);uint64_t block=reader.varint(),miniblocks=reader.varint(),total=reader.varint();
  if(!reader.valid_ || block==0 || block%128 || miniblocks==0 || block%miniblocks || (block/miniblocks)%32 || block>65536 || total>MAX_PAGE/8) {return false;}
  values.clear();if(total==0) {used=reader.pos_;return true;}values.push_back(reader.integer());
  size_t mini=block/miniblocks;
  while(reader.valid_ && values.size()<total) {
    if(THIS_WORKER.check_status()!=OB_SUCCESS) {return false;}
    int64_t minimum=reader.integer();std::vector<uint8_t> widths;for(size_t i=0;i<miniblocks;++i) {widths.push_back(reader.byte());}
    for(size_t m=0;reader.valid_ && m<miniblocks && values.size()<total;++m) {
      int width=widths[m];size_t bytes=mini*width/8;if(width>64 || bytes>size-reader.pos_) {return false;}
      for(size_t i=0;i<mini && values.size()<total;++i) {uint64_t delta=0;for(int b=0;b<width;++b) {size_t bit=i*width+b;delta|=uint64_t((data[reader.pos_+bit/8]>>(bit%8))&1)<<b;}uint64_t last=static_cast<uint64_t>(values.back())+static_cast<uint64_t>(minimum)+delta;values.push_back(static_cast<int64_t>(last));}reader.pos_+=bytes;
    }
  }
  used=reader.pos_;return reader.valid_ && values.size()==total;
}
bool values_decode(const unsigned char *data,size_t size,size_t count,int encoding,const ParquetColumn &column,const std::vector<FileSqlCell> &dictionary,std::vector<FileSqlCell> &values) {
  if(encoding==0) {return plain(data,size,count,column,values);}
  if(encoding==2 || encoding==8) {if(size==0) {return count==0;}std::vector<uint32_t> indices;size_t used;if(!hybrid(data+1,size-1,data[0],count,indices,used)) {return false;}values.clear();size_t bytes=count*sizeof(FileSqlCell);for(uint32_t i:indices) {if(i>=dictionary.size() || dictionary[i].text_.size()>MAX_PAGE-bytes) {return false;}bytes+=dictionary[i].text_.size();values.push_back(dictionary[i]);}return true;}
  if(encoding==3 && column.physical_==0) {if(size<4 || little32(data)>size-4) {return false;}std::vector<uint32_t> decoded;size_t used;if(!hybrid(data+4,little32(data),1,count,decoded,used)) {return false;}values.clear();for(uint32_t value:decoded) {if(value>1) {return false;}FileSqlCell cell;cell.null_=false;cell.text_=value?"true":"false";values.push_back(std::move(cell));}return true;}
  if(encoding==5 && (column.physical_==1 || column.physical_==2)) {std::vector<int64_t> integers;size_t used;if(!delta_numbers(data,size,integers,used) || integers.size()!=count) {return false;}std::vector<unsigned char> encoded;for(int64_t i:integers) {int bytes=column.physical_==1?4:8;for(int b=0;b<bytes;++b) {encoded.push_back(static_cast<uint64_t>(i)>>(8*b));}}return plain(encoded.data(),encoded.size(),count,column,values);}
  if(encoding==6 && column.physical_==6) {std::vector<int64_t> lengths;size_t pos;if(!delta_numbers(data,size,lengths,pos) || lengths.size()!=count) {return false;}values.clear();for(int64_t length:lengths) {if(length<0 || static_cast<uint64_t>(length)>size-pos) {return false;}FileSqlCell cell;cell.null_=false;cell.text_.assign(reinterpret_cast<const char*>(data+pos),length);pos+=length;values.push_back(std::move(cell));}return true;}
  if(encoding==7 && (column.physical_==6 || column.physical_==7)) {
    std::vector<int64_t> prefixes,lengths;size_t start,used;
    if(!delta_numbers(data,size,prefixes,start) || prefixes.size()!=count
        || !delta_numbers(data+start,size-start,lengths,used) || lengths.size()!=count) {return false;}
    size_t pos=start+used;std::string previous;values.clear();size_t memory=count*sizeof(FileSqlCell);if(memory>MAX_PAGE) {return false;}
    for(size_t i=0;i<count;++i) {if(prefixes[i]<0 || static_cast<uint64_t>(prefixes[i])>previous.size() || lengths[i]<0 || static_cast<uint64_t>(lengths[i])>size-pos || prefixes[i]+lengths[i]>FileSqlUtils::MAX_RECORD_BYTES) {return false;}FileSqlCell cell;cell.null_=false;cell.text_=previous.substr(0,prefixes[i]);cell.text_.append(reinterpret_cast<const char*>(data+pos),lengths[i]);pos+=lengths[i];if(column.physical_==7 && cell.text_.size()!=column.length_) {return false;}if(cell.text_.size()>MAX_PAGE-memory) {return false;}memory+=cell.text_.size();previous=cell.text_;values.push_back(std::move(cell));}
    return true;
  }
  if(encoding==9 && (column.physical_==1 || column.physical_==2 || column.physical_==4 || column.physical_==5 || column.physical_==7)) {size_t width=column.physical_==1 || column.physical_==4?4:column.physical_==7?column.length_:8;if(width==0 || width>MAX_PAGE || (count!=0 && width>size/count)) {return false;}std::vector<unsigned char> encoded(count*width);for(size_t i=0;i<count;++i) {for(size_t b=0;b<width;++b) {encoded[i*width+b]=data[b*count+i];}}return plain(encoded.data(),encoded.size(),count,column,values);}
  return false;
}
size_t memory(const PageCursor &cursor) {size_t bytes=(cursor.page_.capacity()+cursor.dictionary_.capacity())*sizeof(FileSqlCell);for(const auto &cell:cursor.page_) {bytes+=cell.text_.capacity();}for(const auto &cell:cursor.dictionary_) {bytes+=cell.text_.capacity();}return bytes;}
int page(FILE *file,PageCursor &cursor,size_t budget) {
  while(cursor.offset_<cursor.chunk_.end_) {
    int status=THIS_WORKER.check_status();if(status!=OB_SUCCESS) {return status;}
    size_t head_size=std::min<int64_t>(65536,cursor.chunk_.end_-cursor.offset_);std::vector<unsigned char> header;
    if(!read_at(file,cursor.offset_,head_size,header)) {return invalid();}CompactReader reader(header.data(),header.size());CompactValue meta=reader.structure();
    if(!reader.valid_ || !meta.has(1) || !meta.has(2) || !meta.has(3)) {return invalid();}
    int type=meta.field(1).number_;int64_t uncompressed=meta.field(2).number_,compressed=meta.field(3).number_;
    int64_t payload_offset=cursor.offset_+reader.pos_;
    if(compressed<0 || uncompressed<0 || compressed>MAX_PAGE || uncompressed>MAX_PAGE || compressed>cursor.chunk_.end_-payload_offset) {return invalid();}
    std::vector<unsigned char> compressed_data,raw;
    if(!read_at(file,payload_offset,compressed,compressed_data)) {return invalid();}cursor.offset_=payload_offset+compressed;
    if(type!=0 && type!=2 && type!=3) {continue;}
    const CompactValue &data_header=meta.field(type==0?5:type==2?7:8);
    if(type==3) {
      int64_t repetition=data_header.field(6).number_,definition=data_header.field(5).number_;bool compressed_values=!data_header.has(7) || data_header.field(7).number_;
      if(repetition!=0 || definition<0 || definition>compressed || definition>uncompressed) {return invalid();}
      raw.insert(raw.end(),compressed_data.begin(),compressed_data.begin()+definition);
      if(compressed_values && cursor.chunk_.codec_==1) {std::vector<unsigned char> encoded(compressed_data.begin()+definition,compressed_data.end()),decoded;if(!snappy(encoded,uncompressed-definition,decoded)) {return invalid();}raw.insert(raw.end(),decoded.begin(),decoded.end());}
      else if((!compressed_values || cursor.chunk_.codec_==0) && compressed==uncompressed) {raw=std::move(compressed_data);}
      else {return invalid();}
    } else if(cursor.chunk_.codec_==0 && compressed==uncompressed) {raw=std::move(compressed_data);}
    else if(cursor.chunk_.codec_==1) {if(!snappy(compressed_data,uncompressed,raw)) {return invalid();}}
    else {return invalid();}
    int64_t count=data_header.field(1).number_;int encoding=data_header.field(type==3?4:2).number_;
    if(count<0 || count>MAX_PAGE/sizeof(FileSqlCell) || static_cast<uint64_t>(count)*sizeof(FileSqlCell)>budget/3 || static_cast<uint64_t>(uncompressed)>budget/3) {return invalid();}
    if(type==2) {if(!plain(raw.data(),raw.size(),count,cursor.column_,cursor.dictionary_)) {return invalid();}continue;}
    if(count>cursor.chunk_.values_-cursor.seen_) {return invalid();}cursor.seen_+=count;
    size_t pos=0,used=0;std::vector<uint32_t> definitions(count,1);
    if(cursor.column_.optional_) {
      size_t bytes;
      if(type==0) {if(raw.size()<4) {return invalid();}bytes=little32(raw.data());pos=4;}
      else {bytes=data_header.field(5).number_;}
      if(bytes>raw.size()-pos || !hybrid(raw.data()+pos,bytes,1,count,definitions,used)) {return invalid();}pos+=bytes;
    }
    size_t present=0;for(uint32_t definition:definitions) {if(definition>1) {return invalid();}present+=definition;}
    std::vector<FileSqlCell> values;if(!values_decode(raw.data()+pos,raw.size()-pos,present,encoding,cursor.column_,cursor.dictionary_,values)) {return invalid();}
    cursor.page_.clear();cursor.page_.reserve(count);cursor.index_=0;size_t index=0;
    for(uint32_t definition:definitions) {cursor.page_.push_back(definition?std::move(values[index++]):FileSqlCell());}
    if(count>0) {return OB_SUCCESS;}
  }
  return OB_ITER_END;
}
}
struct ParquetReader::State {
  FILE *file_=nullptr;std::vector<ParquetColumn> columns_;std::vector<Group> groups_;std::vector<int64_t> projection_;std::vector<PageCursor> cursors_;size_t group_=0;int64_t row_=0;bool initialized_=false;
};
ParquetReader::ParquetReader():state_(new(std::nothrow) State()) {}
ParquetReader::~ParquetReader()=default;
int ParquetReader::open(FILE *file,FileSqlConfig &config,bool infer,const std::vector<int64_t> &projection) {
  if(!state_) {return OB_ALLOCATE_MEMORY_FAILED;}state_.reset(new(std::nothrow) State());if(!state_) {return OB_ALLOCATE_MEMORY_FAILED;}
  state_->file_=file;state_->projection_=projection;
  if(fseeko(file,0,SEEK_END)!=0) {return invalid();}int64_t size=ftello(file);if(size<12) {return invalid();}
  std::vector<unsigned char> trailer,magic;if(!read_at(file,size-8,8,trailer) || !read_at(file,0,4,magic) || memcmp(magic.data(),"PAR1",4) || memcmp(trailer.data()+4,"PAR1",4)) {return invalid();}
  uint32_t footer_size=little32(trailer.data());if(footer_size>MAX_PAGE || footer_size>size-12) {return invalid();}
  std::vector<unsigned char> footer;if(!read_at(file,size-8-footer_size,footer_size,footer)) {return invalid();}CompactReader reader(footer.data(),footer.size());CompactValue metadata=reader.structure();
  if(!reader.valid_) {return invalid();}const auto &schema=metadata.field(2).list_;if(schema.size()<2 || schema.size()>FileSqlUtils::MAX_COLUMNS+1 || schema[0].field(5).number_!=schema.size()-1) {return invalid();}
  if(infer) {config.columns_.clear();}
  for(size_t i=1;i<schema.size();++i) {
    const auto &element=schema[i];ParquetColumn column;column.physical_=element.field(1).number_;column.length_=element.field(2).number_;column.optional_=element.field(3).number_==1;column.converted_=element.has(6)?element.field(6).number_:-1;
    if(!element.has(1) || element.field(3).number_>1 || element.has(5) || column.physical_<0 || column.physical_>7 || (column.physical_==7 && (column.length_<=0 || column.length_>FileSqlUtils::MAX_RECORD_BYTES))) {return invalid();}
    const auto &logical=element.field(10);
    if(logical.has(8)) {const auto &unit=logical.field(8).field(2);column.unit_=unit.has(1)?1:unit.has(2)?2:unit.has(3)?3:0;if(column.unit_==0) {return invalid();}}
    if(logical.has(6)) {column.converted_=6;}
    if(column.converted_==5 || logical.has(5) || logical.has(2) || logical.has(3)) {return invalid();}
    state_->columns_.push_back(column);
    if(infer) {FileSqlColumn col;col.name_=element.field(4).text_;col.nullable_=column.optional_;col.length_=column.physical_==6?16*1024*1024:std::max(1,column.length_);col.type_=column.physical_==0?FileSqlType::BOOLEAN:(column.physical_==4 || column.physical_==5)?FileSqlType::DOUBLE:(column.physical_==1 || column.physical_==2)?FileSqlType::INTEGER:FileSqlType::VARCHAR;if(column.converted_==6) {col.type_=FileSqlType::DATE;}else if(column.physical_==3 || column.converted_==9 || column.converted_==10 || column.unit_!=0) {col.type_=FileSqlType::DATETIME;}if(col.type_==FileSqlType::DATETIME) {col.length_=(column.converted_==9 || column.unit_==1)?23:26;}if(logical.has(11)) {col.type_=FileSqlType::UNKNOWN;}if(column.converted_==14) {col.type_=FileSqlType::VARCHAR;}if(col.name_.empty()) {return invalid();}config.columns_.push_back(std::move(col));}
  }
  if(config.columns_.size()!=state_->columns_.size()) {return invalid();}
  const auto &groups=metadata.field(4).list_;if(groups.size()>100000) {return invalid();}
  for(const auto &group:groups) {Group output;output.rows_=group.field(3).number_;if(output.rows_<0 || group.field(1).list_.size()!=state_->columns_.size()) {return invalid();}size_t column_index=0;for(const auto &column:group.field(1).list_) {if(column.has(1) && !column.field(1).text_.empty()) {return invalid();}const auto &meta=column.field(3);if(meta.field(1).number_!=state_->columns_[column_index].physical_ || meta.field(3).list_.size()!=1 || meta.field(3).list_[0].text_!=config.columns_[column_index].name_) {return invalid();}++column_index;Chunk chunk;chunk.offset_=meta.field(9).number_;if(meta.has(11)) {chunk.offset_=std::min(chunk.offset_,meta.field(11).number_);}int64_t total=meta.field(7).number_;int64_t footer_start=size-8-footer_size;if(total<0 || chunk.offset_<4 || chunk.offset_>footer_start || total>footer_start-chunk.offset_) {return invalid();}chunk.end_=chunk.offset_+total;chunk.values_=meta.field(5).number_;chunk.codec_=meta.field(4).number_;if(chunk.offset_<4 || chunk.end_<chunk.offset_ || chunk.end_>size-8-footer_size || chunk.values_!=output.rows_) {return invalid();}output.chunks_.push_back(chunk);}state_->groups_.push_back(std::move(output));}
  int64_t total_rows=0;for(const auto &group:state_->groups_) {if(group.rows_>INT64_MAX-total_rows) {return invalid();}total_rows+=group.rows_;}if(total_rows!=metadata.field(3).number_) {return invalid();}
  for(int64_t column:projection) {if(column<0 || column>=state_->columns_.size()) {return invalid();}}
  return OB_SUCCESS;
}
int ParquetReader::next(std::vector<FileSqlCell> &row) {
  if(!state_) {return OB_NOT_INIT;}
  while(state_->group_<state_->groups_.size()) {
    const auto &group=state_->groups_[state_->group_];
    if(!state_->initialized_) {state_->cursors_.clear();for(int64_t index:state_->projection_) {PageCursor cursor;cursor.chunk_=group.chunks_[index];cursor.column_=state_->columns_[index];cursor.offset_=cursor.chunk_.offset_;state_->cursors_.push_back(std::move(cursor));}state_->row_=0;state_->initialized_=true;}
    if(state_->row_>=group.rows_) {++state_->group_;state_->initialized_=false;continue;}
    row.assign(state_->columns_.size(),FileSqlCell());
    for(size_t i=0;i<state_->projection_.size();++i) {auto &cursor=state_->cursors_[i];if(cursor.index_>=cursor.page_.size()) {size_t occupied=0;for(size_t j=0;j<state_->cursors_.size();++j) {if(j!=i) {occupied+=memory(state_->cursors_[j]);}}if(occupied>4*MAX_PAGE) {return invalid();}int ret=page(state_->file_,cursor,4*MAX_PAGE-occupied);if(ret!=OB_SUCCESS) {return ret==OB_ITER_END?invalid():ret;}}row[state_->projection_[i]]=cursor.page_[cursor.index_++];}
    ++state_->row_;return OB_SUCCESS;
  }
  return OB_ITER_END;
}
} }

#else

namespace oceanbase { namespace sql {
struct ParquetReader::State {};
ParquetReader::ParquetReader()=default;
ParquetReader::~ParquetReader()=default;
int ParquetReader::open(FILE *,FileSqlConfig &,bool,const std::vector<int64_t> &) {return common::OB_NOT_SUPPORTED;}
int ParquetReader::next(std::vector<FileSqlCell> &) {return common::OB_NOT_SUPPORTED;}
} }
#endif
