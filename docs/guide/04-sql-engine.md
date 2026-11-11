# 04 SQL 解析与执行

## 1. 一条 SQL 的旅程
~~~text
文本 "SELECT id, L2_DISTANCE(embedding,'[0,0]') AS d FROM docs WHERE category='x' ORDER BY d LIMIT 5;"
  -> lexer：Ident/Number/Str/括号/运算符 ...（去掉注释、处理单引号转义）
  -> parser：递归下降，按优先级 OR < AND < 比较 < 加减 < 乘除 < 一元 < 原子
            产出 Stmt::Select { items, table, filter, order_by, limit }
  -> engine::select：决定走"通用逐行路径"还是"向量快路径"
  -> 产出 ExecResult::Query { columns, rows }
~~~

## 2. 支持的语句（AST 对应）
| SQL | AST | 说明 |
|---|---|---|
| CREATE TABLE t (id BIGINT PRIMARY KEY, c VARCHAR(8), e VECTOR(2)) | CreateTable | 必须有且仅有一个 BIGINT 主键 |
| CREATE VECTOR INDEX i ON t(e) | CreateVectorIndex | 只记录元信息（我们不依赖索引算结果） |
| INSERT INTO t VALUES (...),(...) | Insert / InsertOrdered | 多行；可带列名列表 |
| UPDATE t SET c='x', e='[1,2]' WHERE id=1 | Update | 不允许改主键 |
| DELETE FROM t WHERE id>=1 | Delete | 无 WHERE 则删全表 |
| SELECT ... FROM t [WHERE ...] [ORDER BY ...] [LIMIT n] | Select | 支持 * 展开 |
| BEGIN / COMMIT / ROLLBACK | 事务控制 | 无显式事务时单语句自动提交 |
| ALTER TABLE / DROP / JOIN / 聚合 ... | Unsupported | 必须返回错误，不能静默执行 |

## 3. 表达式求值（exec/）
~~~text
eval(expr, table, row):
  Lit(v)            -> v
  Column(name)      -> row[列下标]
  L2Distance(col,q) -> 校验 col 是向量列；q 解析成 f32 数组；校验维度；
                       返回 Float(sqrt(Σ(f32差)^2，f64 累加))
  Binary(op,l,r)    -> 比较类：compare(l,r) 返回 Ordering（NULL 参与则视为不满足）
                       逻辑类：AND/OR 用真值判断（0/空串为假）
                       算术类：整数优先整数运算，否则 f64
  Unsupported(fn)   -> 返回 unsupported_sql 错误
~~~

## 4. 取值类型转换（INSERT/UPDATE 时）
| 目标列 | 接受 | 拒绝/报错 |
|---|---|---|
| BIGINT | 整数；可无损转整的浮点；数字字符串 | 其它类型 → type_mismatch |
| VARCHAR(n) | 字符串/数字转文本；**按字符数**校验超长 | 超过 n 个 Unicode 字符 → string_too_long |
| VECTOR(d) | 形如 `[1,2.5]` 的字符串；逐个转 f32 | 非有限/溢出 → invalid_vector；维度不符 → dimension_mismatch |

## 5. SELECT 的两条路径
**A. 通用逐行路径**（任何查询都正确）
~~~text
scan(表) 按主键升序逐行：
    若 WHERE 不满足 -> 跳过
    计算投影 -> projected
    计算 ORDER BY 键 -> keys
收集全部 (pk, projected, keys)
若 ORDER BY：按 keys 升序；完全相等按 pk 升序（并列规则）
若 LIMIT：截断
~~~
（内存换时间：结果集有上限时够用，大结果集后续会改成 TopK 堆/外部排序。）

**B. 向量快路径**（性能 benchmark 的主力，命中条件很严）
命中条件：不在事务里 + 只有一个 ORDER BY 键 + 该键是 L2_DISTANCE（或它的别名）
+ 有 LIMIT + 查询向量是字面量 + 向量列能放进内存预算。
~~~text
确保该 (表,向量列) 的扁平缓存存在：ids[n], flat[n*dim]（f32，行主序）
若 WHERE 存在：流式扫描，对每行求 WHERE，把命中的行下标放进 candidates
调用 fastscan：在 candidates（或全表）上算精确 L2，取 top-k（距离升序、并列 pk 升序）
对 top-k 的每个 pk 回表取整行，按投影表达式生成输出列
~~~
不命中就回退路径 A，所以正确性不依赖快路径。

## 6. 事务与"语句原子性"（engine::mutate）
~~~text
mutate(语句执行函数 f):
    if 不在显式事务:
        storage.begin()                 # 开一个隐式事务
        r = f()
        if r 成功: storage.commit()     # 失败: storage.rollback()，什么都不留下
    else:
        storage.statement_mark()        # 打 savepoint（记录 undo 长度）
        r = f()
        if r 失败: storage.rollback_statement()  # 只回滚本条语句的写
    返回 r
~~~
这一层保证了两条赛题语义：
- 多行 INSERT 中途失败 → 一行都不能留下（自动提交场景）；
- 显式事务里某条语句失败 → 之前成功语句的修改仍然保留（可以继续 COMMIT/ROLLBACK）。

> 注：这条逻辑曾经有 bug（savepoint 没记录 undo 起点，回滚把整个事务的写都弹掉了），
> 由独立加固用例 `atomicity.transaction_failed_insert` 抓出，已修复并加了回归测试。
