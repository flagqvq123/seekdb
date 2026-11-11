# 05 存储、事务与恢复

## 1. 存储层要同时满足三件互相拉扯的事
1. **不丢**：COMMIT 响应发出前必须已经落盘（fsync），强杀后已提交数据还在。
2. **不多**：未提交事务不能部分生效；语句失败不能留下半条修改。
3. **不撑爆内存**：正式测试有"数据量超过内存限额"的场景，不能全量常驻内存。

## 2. 接口（trait 就是契约，engine 只认这个）
~~~text
Storage:
  # 表
  table(name) / table_by_id(id) / list_tables()
  create_table(name, columns, pk) / create_vector_index(table_id, column) / vector_indexes(table_id)
  # 事务
  in_txn() / begin() / commit() / rollback()
  statement_mark() / rollback_statement()      # 语句级 savepoint
  # 行
  get(table_id, pk) -> Option<Row>
  put(table_id, pk, row) / delete(table_id, pk) -> existed
  scan(table_id, callback)                     # 按主键升序流式回调
  flush()
~~~
两种实现：
- `MemStorage`：纯内存 + overlay + undo，用于开发/单测（逻辑参考实现）。
- `DiskStorage`：正式版，磁盘数据 + WAL + 恢复（进行中）。

## 3. DiskStorage 的设计（约定）
### 3.1 文件布局（全部在 /data）
~~~
/data/
  catalog.log       # 表定义、向量索引元信息（append-only，可压缩）
  wal.log           # 行级 redo 日志（append-only，带校验和）
  wal.ckpt          # checkpoint 元信息（原子替换写入）
  tables/<id>.seg   # 行数据段（按 pk 排序写入，checkpoint 时生成）
  LOCK              # 单进程锁（可选，防止同一目录双开）
~~~

### 3.2 WAL 记录格式（自描述，可跳过坏尾）
~~~
[magic u32][len u32][crc32 u32][records...] 
record: [type u8][table_id u32][pk i64][payload_len u32][payload bytes]
type: 1=PUT 行镜像  2=DELETE  3=COMMIT  4=SCHEMA
~~~
- 每条写记录先落 WAL；最后一个 COMMIT 记录代表"这批写已提交"。
- 读取时严格按 len 跳；尾部不完整（长度不够 / crc 不符）就**截断**，不报错。
- 恢复幂等：重放中可以再次被杀，下次重放从同一个 ckpt 位置重来仍然正确。

### 3.3 提交流程（关键顺序）
~~~text
commit():
    for 每个本事务的写: append 到 WAL 缓冲
    append COMMIT 记录
    write(wal) + fsync(wal)        # 到这里才算"已提交"
    更新内存主键索引（pk -> 位置）
    清空事务 overlay
    返回成功                      # 之后才允许回 COMMIT 响应
~~~

### 3.4 恢复流程（必须在 ready 之前完成）
~~~text
open(dir):
    读 wal.ckpt，拿到 checkpoint 位置和段文件清单
    从 checkpoint 位置开始顺序重放 wal.log：
        每条完整记录应用到内存索引；遇到 COMMIT 才认为这批生效
        （未 COMMIT 的尾部记录直接丢弃，不生效）
    重建 表名->表定义、pk->位置 的内存索引
    若 WAL 过长，触发 checkpoint（见下）
~~~

### 3.5 checkpoint（防止 WAL 无限增长）
~~~text
checkpoint():
    把当前可见行按 pk 顺序写到 tables/<id>.seg.tmp
    fsync(tables/<id>.seg.tmp)
    rename 成 tables/<id>.seg            # 原子替换
    写新 ckpt 文件（含新段文件清单 + wal 偏移）到 wal.ckpt.tmp，fsync，rename
    截断 wal.log（保留偏移之后的）
~~~
崩溃点分析：任何一步崩溃，下次都能用"旧 ckpt + 旧 WAL"或"新 ckpt + 新 WAL"恢复，
不会出现"一半新一半旧"。

### 3.6 内存占用控制
- 内存里只放：表定义、`pk -> 文件位置` 索引（每行约十几字节）。
- 行数据扫描时**流式**读文件，不整表载入 Vec。
- 向量查询有独立的"热点缓存"：只缓存向量列的扁平 f32 数组，且有字节上限
  （当前 256 MiB）；超预算就放弃缓存、回退磁盘扫描。这样 10k×128 只有 5 MB，
  既快又不会在大数据场景撑爆。

## 4. 语句级回滚（undo）语义
~~~text
事务内写操作 = 记进 overlay + 同时往 undo 栈压一条"改之前是什么"
statement_mark()        -> 记下 undo 栈当前长度
rollback_statement()    -> 弹栈直到长度回到 mark（只撤销本条语句的写）
commit()                -> overlay 全部落 WAL（undo 栈丢弃）
rollback()              -> overlay 整体丢弃
~~~

## 5. 现状与风险
- 当前仓库里的 DiskStorage 还是"委托给内存实现"的占位版 → **重启丢数据**，
  所以 7 个 `--restart` 公开用例失败。
- 真正的 WAL 版正在实现；恢复的正确性（torn tail、恢复中再被杀、checkpoint 边界）
  是整个项目最大风险点，需要专用的故障注入用例（tools/verify/edge_cases.py 的
  durability 组）验证。
