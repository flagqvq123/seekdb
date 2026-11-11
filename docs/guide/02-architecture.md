# 02 整体架构

## 1. 分层图
~~~
                    stdin (JSONL 请求)              stdout (JSONL 响应)
                            |                              ^
                    +-------+------------------------------+-------+
                    |            main.rs  协议主循环                  |
                    +-------------------------+---------------------+
                                              |  sql 字符串
                    +-------------------------v---------------------+
                    |            engine.rs  语句调度与执行            |
                    |  DDL / INSERT / UPDATE / DELETE / 事务 / SELECT |
                    +--------+------------------+-------------------+
                             |                  |
              +--------------v----+   +---------v------------------+
              |  sql/  解析成 AST  |   |  exec/ 表达式与类型系统     |
              +-------------------+   +----------------------------+
                             |                  |
                    +--------v------------------v-------------------+
                    |      storage/  Storage trait（两种实现）        |
                    |  MemStorage（开发用）/ DiskStorage（正式，WAL）  |
                    +-----------------------------------------------+
                                              |
                                         /data 磁盘文件
~~~

## 2. 每个模块只干一件事
| 模块 | 职责 | 不该做的事 |
|---|---|---|
| `json.rs` | JSON 文本 ⇄ 内存结构；i64 精确；浮点安全输出 | 不懂 SQL |
| `sql/lexer.rs` | 把 SQL 切成 token | 不懂语义 |
| `sql/parser.rs` | token → AST，拒绝不支持的语法 | 不查表、不算数 |
| `sql/ast.rs` | AST 数据结构定义（双方约定） | 无逻辑 |
| `exec/` | 表达式求值、类型强制、比较/排序、列解析、投影命名 | 不碰磁盘 |
| `storage/` | 表/行/事务/持久化的唯一入口（trait 定义边界） | 不懂 SQL 语义 |
| `vector/` | 向量解析校验、L2 距离、AnnIndex trait | 不管存储 |
| `fastscan.rs` | 扁平 f32 数组上的精确 TopK 扫描 | 不管事务 |
| `engine.rs` | 把前三层粘起来：语句调度、原子性、快路径选择 | 不直接读写文件 |

**关键设计**：engine 只依赖 `storage::Storage` trait，不依赖具体实现。所以
"内存版"和"磁盘 WAL 版"可以互换，测试时用内存版跑逻辑，正式跑磁盘版。

## 3. 一次请求的完整数据流
~~~
1. main 读到一行 {"id":7,"op":"execute","sql":"...","search_mode":"exact"}
2. json::parse 解出 id / sql / mode（id 原样保存，响应必须回传同类型同值）
3. engine::execute(sql, mode):
   a. sql::parse -> Stmt（DDL / DML / 事务控制 / SELECT / Unsupported）
   b. 按语句类型分派：
      - 写语句：走 mutate() 包一层"自动提交 + 语句级原子性"
      - SELECT：走 select()（可能命中向量快路径）
      - 事务控制：直接调用 storage
4. 结果封装成 ExecResult（Ack / Affected(n) / Query{columns,rows}）
5. main 组装 JSON 并 flush 一行；出错则 {"ok":false,"error":{"code","message"}}
~~~

## 4. 事务边界（谁负责什么）
- **storage 负责**：overlay（事务内未提交写）、undo（语句级回滚）、commit 落盘、rollback 丢弃。
- **engine 负责**：判断"现在是不是在显式事务里"；不在事务里时自动 BEGIN/COMMIT；
  在事务里时在每条写语句前打 savepoint、失败时回滚到 savepoint。
- **exec 负责**：值类型转换与语义错误（例如维度不符、主键重复由 engine 判）。

## 5. 为什么这样分
1. 可测试：每层都能单独测（JSON 单测、解析单测、存储恢复单测）。
2. 可替换：磁盘存储实现改动不影响 SQL 语义；快路径可回退到通用路径。
3. 可解释：出问题时能快速定位"是解析错了、语义错了、还是持久化错了"。
