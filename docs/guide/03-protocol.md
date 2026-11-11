# 03 协议与主循环

## 1. 协议硬规则（错一条就可能 0 分）
1. UTF-8；每个报文一个完整 JSON 对象；**一个报文只占一行**（字符串内换行必须转义）。
2. 启动先输出一行 `{"type":"ready"}` 并 flush，**在等待第一条请求之前**。
3. stdout 只允许协议报文；日志/调试/堆栈全部写 stderr。
4. 每个请求恰好一个响应；`id` 原样回传（数字不能变字符串）。
5. 每行输出后立即 flush，不能等缓冲区满。
6. 不输出空行、提示符、ANSI 颜色、Markdown 围栏、NaN/Infinity。
7. shutdown 的响应之后不能再输出任何内容，然后**退出码 0**。

## 2. 请求 / 响应形状
~~~json
请求：{"id":1,"op":"execute","sql":"SELECT id FROM docs;","search_mode":"exact"}
查询成功：{"id":1,"ok":true,"columns":["id"],"rows":[[1]]}
修改成功：{"id":2,"ok":true,"affected_rows":1}
执行失败：{"id":3,"ok":false,"error":{"code":"duplicate_key","message":"..."}}
只要求成功：{"id":4,"ok":true}                 // 例如 CREATE TABLE / BEGIN / COMMIT
关闭：{"id":5,"op":"shutdown"} -> {"id":5,"ok":true} 然后退出
~~~
注意：期望文件里的 `{}` 不是让程序输出 `{}`，而是"该语句要成功、不校验额外字段"。

## 3. 主循环伪代码（main.rs）
~~~text
func main():
    dir = 参数 --data-dir，否则环境变量，否则 "/data"
    engine = Engine.open(dir)          // 先完成恢复，恢复失败直接退出非 0
    print_line({"type":"ready"})        // 然后 flush

    loop:
        line = read_line(stdin)
        if 读到 EOF: break
        if line 为空: continue
        req = json_parse(line)          // 失败：回一条 invalid_request 错误并继续
        id  = req["id"]                 // 原样保留（可能是数字也可能是字符串）
        op  = req["op"]

        if op == "shutdown":
            engine.shutdown()           // 回滚未提交事务 + 落盘 + 关文件
            print_line({"id":id,"ok":true})
            exit(0)                     // 之后不能再写 stdout

        if op != "execute":
            print_line(error(id, "unsupported_op"))
            continue

        sql  = req["sql"]
        mode = req["search_mode"] 或 "exact"
        result = engine.execute(sql, mode)
        if ok:   print_line({id, ok:true, ...result字段})
        else:    print_line({id, ok:false, error:{code,message}})
~~~

## 4. 恢复必须发生在 ready 之前
赛题要求"重建完成前不得报告就绪"。所以 `Engine.open` 里要完成：读 catalog、重放 WAL、
校验/截断尾部残缺记录。宁可启动慢一点，也不能先 ready 再慢慢恢复。

## 5. 常见失败对照
| 现象 | 多半原因 |
|---|---|
| invalid JSON response | 第一行输出了 READY/提示符/空行，或日志混进 stdout |
| expected ready, got ... | 启动对象不是精确的 `{"type":"ready"}` |
| JSON response timeout | 忘了 flush，或恢复卡住 |
| response id differs | id 漏回传 / 把数字转成字符串 |
| missing boolean ok | ok 用了 0/1 或字符串 |
| extra stdout after shutdown | shutdown 后又打印了日志 |
