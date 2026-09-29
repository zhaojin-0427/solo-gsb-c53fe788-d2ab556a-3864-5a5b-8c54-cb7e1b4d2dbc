# 条件请求对象源站 API

基于 **Python + FastAPI + PostgreSQL** 的对象源站，完整实现 RFC 9110 条件请求与字节范围服务：

- 客户端按路径 `PUT /objects/{path}` 发布内容修订；每次发布生成新修订。
- 以内容的 **SHA-256** 生成**强 ETag**（`"<64 hex>"`），`Last-Modified` 为修订创建时间（秒级精度）。
- 所有方法按 **RFC 9110 §13.2.2** 的顺序求值前置条件：
  `If-Match` → `If-Unmodified-Since`（仅当无 `If-Match`）→ `If-None-Match` → `If-Modified-Since`（仅 GET/HEAD 且无 `If-None-Match`）。
  条件失败返回 **412**，GET/HEAD 命中 `If-None-Match` / `If-Modified-Since` 返回 **304**。
- 条件求值与修订选择发生在**同一事务**内：读请求对对象行加 `FOR SHARE` 锁、写请求加 `FOR UPDATE` 锁，并发发布串行化，杜绝并发覆盖（`If-None-Match: *` 的"仅创建"语义在并发下也只有一个成功）。
- GET/HEAD **先处理前置条件，再处理 Range**：支持单段（`bytes=0-99`、`bytes=0-`）、后缀（`bytes=-500`）与多段范围；重叠或相邻区间按升序合并；分别返回 200 / 206 / 304 / 416。
- 多段范围返回 `multipart/byteranges`，**边界由修订 ETag + 区间集合派生**，重复请求产生完全相同的边界与 `Content-Range`。
- `If-Range`（ETag 强比较或 HTTP 日期）失配时忽略 Range，返回完整实体（200）。
- 每次响应绑定同一修订：流式传输期间事务与行锁保持打开，此时发布新版本**不会混入**内容或元数据。
- **可恢复复制会话**：`POST /copy-sessions` 在同一数据库快照中锁定源路径集合与全部目标路径，生成按稳定路径与修订序排列、带 SHA-256 内容摘要的清单，并为每个目标路径（含目标尚不存在的状态）保存**修订身份基线**；客户端按游标逐段拉取内容（重复拉取字节、摘要、游标完全一致，即使源对象随后发布新修订），逐段确认时服务锁定目标并与基线比较修订身份——**建档后目标路径的任何发布（哪怕字节与待复制内容完全相同）都判为冲突**，返回 `409`、冲突对象与恢复位置，整批不应用、不推进游标；基线未变时维持原子应用与会话前同内容目标的幂等跳过，重复确认或重放返回同一批次结果。

## 快速启动（Docker Compose）

```bash
docker compose up --build
```

服务监听 <http://localhost:8000>。PostgreSQL 数据保存在 `pgdata` 卷中；表结构由应用启动时自动创建。

健康检查：

```bash
curl http://localhost:8000/healthz
```

## API 一览

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| `PUT` | `/objects/{path}` | 发布新修订；请求体为内容，`Content-Type` 被保存；响应 `204` + `ETag` / `Last-Modified` / `X-Revision-No` |
| `GET` | `/objects/{path}` | 读取当前修订；支持全部条件头与 `Range` / `If-Range` |
| `HEAD` | `/objects/{path}` | 同 GET，仅返回头部 |
| `POST` | `/copy-sessions` | 创建复制会话：提交 `source_paths`、`target_prefix`、可选 `cursor`（上次确认游标）与 `segment_size`；响应 `201` + 带 SHA-256 摘要的清单 |
| `GET` | `/copy-sessions/{id}` | 会话状态：清单摘要、分段进度、恢复位置 |
| `GET` | `/copy-sessions/{id}/segments?cursor=` | 拉取游标之后的内容分段（无 `cursor` 参数时返回首段）；重复拉取字节、摘要、游标完全一致 |
| `POST` | `/copy-sessions/{id}/acks` | 按游标确认分段：锁定目标并与建档修订基线比较身份，原子批次应用；建档后任何发布（含同字节重发）返回 `409`，整批不应用、不推进游标；重复确认 / 重放返回同一批次结果 |
| `GET` | `/healthz` | 健康检查 |

支持的条件头：`If-Match`、`If-None-Match`、`If-Modified-Since`、`If-Unmodified-Since`（PUT/GET/HEAD 均按 §13.2.2 顺序求值）。

## curl 验证

```bash
# 1. 发布内容（记下返回的 ETag 与 Last-Modified）
curl -i -X PUT --data-binary 'hello world' -H 'Content-Type: text/plain' \
  http://localhost:8000/objects/demo.txt
# HTTP/1.1 204 ...  ETag: "b94d27b9..."  Last-Modified: Tue, 22 Sep 2026 ...

ETAG=$(curl -sI http://localhost:8000/objects/demo.txt | awk '/^[Ee][Tt]ag:/ {print $2}' | tr -d '\r')

# 2. 条件 GET：ETag 未变 -> 304（If-None-Match 用弱比较，W/"..." 形式同样命中）
curl -i -H "If-None-Match: $ETAG" http://localhost:8000/objects/demo.txt
curl -i -H "If-None-Match: W/${ETAG//\"/\\\"}" http://localhost:8000/objects/demo.txt

# 3. If-Modified-Since -> 304；If-Match 不匹配 -> 412（强比较，W/ 形式必失败）
curl -i -H "If-Modified-Since: $(curl -sI http://localhost:8000/objects/demo.txt | awk '/^[Ll]ast-[Mm]odified:/ {sub(/^[^:]*: /,""); print}' | tr -d '\r')" \
  http://localhost:8000/objects/demo.txt
curl -i -H 'If-Match: "stale"' http://localhost:8000/objects/demo.txt

# 4. 乐观并发写：仅当未修改才覆盖
curl -i -X PUT --data-binary 'v2' -H "If-Match: $ETAG" http://localhost:8000/objects/demo.txt   # 204
curl -i -X PUT --data-binary 'v3' -H "If-Match: $ETAG" http://localhost:8000/objects/demo.txt   # 412（ETag 已变）

# 5. 仅创建语义：已存在则 412
curl -i -X PUT --data-binary 'x' -H 'If-None-Match: *' http://localhost:8000/objects/demo.txt   # 412

# 6. 字节范围
curl -i -H 'Range: bytes=0-4'   http://localhost:8000/objects/demo.txt   # 206 + Content-Range
curl -i -H 'Range: bytes=-5'    http://localhost:8000/objects/demo.txt   # 后缀范围
curl -i -H 'Range: bytes=0-99999' http://localhost:8000/objects/demo.txt # 截断到末尾
curl -i -H 'Range: bytes=99999-' http://localhost:8000/objects/demo.txt  # 416 + Content-Range: bytes */N

# 7. 多段范围：重叠/相邻自动合并；重复请求边界与 Content-Range 完全一致
curl -i -H 'Range: bytes=0-1,2-3' http://localhost:8000/objects/demo.txt # 合并为单段 206
curl -i -H 'Range: bytes=0-1,5-9' http://localhost:8000/objects/demo.txt # multipart/byteranges
curl -s -H 'Range: bytes=0-1,5-9' http://localhost:8000/objects/demo.txt | sha256sum
curl -s -H 'Range: bytes=0-1,5-9' http://localhost:8000/objects/demo.txt | sha256sum   # 两次相同

# 8. If-Range：失配返回完整实体（200），匹配才返回 206
curl -i -H 'Range: bytes=0-4' -H 'If-Range: "stale"' http://localhost:8000/objects/demo.txt  # 200
curl -i -H 'Range: bytes=0-4' -H "If-Range: $ETAG"   http://localhost:8000/objects/demo.txt  # 206
```

## 复制会话（可恢复的快照复制）

复制会话把一组源对象的**某一时刻快照**复制到目标路径前缀下，全过程可重试、可断点续传：

1. **创建会话**：提交源路径集合、目标前缀与可选的上次确认游标。服务在**同一事务**里先对源路径与全部目标路径（目标尚不存在时先插入占位行）按全局路径升序加 `FOR SHARE` 锁，再在锁内快照源修订并为**每个目标路径保存修订身份基线**（`revision_id` 为 NULL 即"目标不存在"）；清单按稳定路径与修订序生成，每条清单项带 SHA-256 内容摘要。清单与基线持久化后源对象发布新修订**不影响**本会话。
2. **拉取分段**：`GET /copy-sessions/{id}/segments` 返回首段；之后用上一段响应里的 `cursor`
   作为查询参数取下一段。每段包含条目内容（base64）、每条目的 SHA-256、段摘要与标识本段的游标。
   重复拉取同一段返回**完全相同的字节、摘要和游标**。
3. **逐段确认**：`POST /copy-sessions/{id}/acks`，请求体 `{"cursor": "<该段游标>"}`。服务将该分段
   对应的修订**作为一次原子批次**应用到目标前缀（目标路径 = `target_prefix + source_path`），并按
   目标路径升序 `FOR UPDATE` 锁定后**与建档基线比较修订身份**：
   基线不存在且目标仍不存在则创建修订；基线身份未变且摘要相同则幂等跳过；
   **基线之后目标路径只要发生过发布就是冲突——即使发布的字节与待复制内容完全相同**
   （仅凭摘要无法区分"未改动"与"同字节重新发布"，这是漏判的来源）。
   任一冲突则整批回滚，返回 `409` 及结构化结果（冲突对象、`reason`、期望/实际/基线摘要与修订号、
   恢复位置），不留部分应用、不推进游标。批次结果持久化，**重复确认或重放返回同一批次结果**。
4. **恢复**：确认严格按段序进行；游标过期 / 乱序确认返回 `409 cursor_expired` 与恢复位置。
   冲突解决后（例如用条件 `PUT` 修正目标），用结构化结果里的 `resume.last_confirmed_cursor`
   创建新会话即可从上次确认处继续，新会话基于**新的快照与基线**。
   升级前创建、缺少基线的未完成会话，确认未完成段时返回 `409 baseline_missing`（不会按摘要放行，
   也不会推进游标）；用新会话重新建档即可继续。已持久化的历史确认结果仍原样重放。

```bash
# 1. 准备源对象并创建会话（每段 2 个条目）
curl -s -X PUT --data-binary 'alpha' http://localhost:8000/objects/src/a.txt
curl -s -X PUT --data-binary 'beta'  http://localhost:8000/objects/src/b.txt
curl -s -X PUT --data-binary 'gamma' http://localhost:8000/objects/src/c.txt

SID=$(curl -s -X POST http://localhost:8000/copy-sessions \
  -H 'Content-Type: application/json' \
  -d '{"source_paths": ["src/a.txt", "src/b.txt", "src/c.txt"],
       "target_prefix": "backup/", "segment_size": 2}' | python3 -c 'import sys,json;print(json.load(sys.stdin)["session"])')

# 2. 拉取首段（响应含内容、每条目 sha256、segment_sha256 与本段 cursor）
curl -s "http://localhost:8000/copy-sessions/$SID/segments" | python3 -m json.tool
CURSOR0=$(curl -s "http://localhost:8000/copy-sessions/$SID/segments" | python3 -c 'import sys,json;print(json.load(sys.stdin)["cursor"])')

# 源对象随后发布新修订，重拉该段字节/摘要/游标仍完全一致
curl -s -X PUT --data-binary 'alpha-v2' http://localhost:8000/objects/src/a.txt
curl -s "http://localhost:8000/copy-sessions/$SID/segments" | sha256sum
curl -s "http://localhost:8000/copy-sessions/$SID/segments" | sha256sum   # 两次相同

# 3. 确认首段：原子应用到 backup/ 前缀；重复确认返回同一结果
curl -s -X POST "http://localhost:8000/copy-sessions/$SID/acks" \
  -H 'Content-Type: application/json' -d "{\"cursor\": \"$CURSOR0\"}" | python3 -m json.tool
curl -s http://localhost:8000/objects/backup/src/a.txt   # alpha（源快照内容，非 alpha-v2）

# 4. 拉取并确认下一段，直至 has_more == false
CURSOR1=$(curl -s "http://localhost:8000/copy-sessions/$SID/segments?cursor=$CURSOR0" | python3 -c 'import sys,json;print(json.load(sys.stdin)["cursor"])')
curl -s -X POST "http://localhost:8000/copy-sessions/$SID/acks" \
  -H 'Content-Type: application/json' -d "{\"cursor\": \"$CURSOR1\"}" | python3 -m json.tool

# 5. 查看会话状态（complete）与恢复位置
curl -s "http://localhost:8000/copy-sessions/$SID" | python3 -m json.tool

# 6. 冲突示例：目标被独立修改后确认 -> 409 + 冲突对象 + 恢复位置，且整批未应用
curl -s -X PUT --data-binary 'divergent' http://localhost:8000/objects/backup/src/c.txt
SID2=$(curl -s -X POST http://localhost:8000/copy-sessions -H 'Content-Type: application/json' \
  -d '{"source_paths": ["src/c.txt"], "target_prefix": "backup/"}' | python3 -c 'import sys,json;print(json.load(sys.stdin)["session"])')
C=$(curl -s "http://localhost:8000/copy-sessions/$SID2/segments" | python3 -c 'import sys,json;print(json.load(sys.stdin)["cursor"])')
curl -s -X POST "http://localhost:8000/copy-sessions/$SID2/acks" \
  -H 'Content-Type: application/json' -d "{\"cursor\": \"$C\"}" | python3 -m json.tool
# -> {"status": "conflict", "conflicts": [{"target_path": "backup/src/c.txt", ...}],
#     "resume": {"next_segment": 0, "last_confirmed_cursor": null}}

# 6b. 同内容新修订冲突：建档后目标又发布了一份"字节完全相同"的修订
#     仅比较摘要会误判 already_present 并推进游标；修订身份基线使其返回 409
curl -s -X PUT --data-binary 'delta' http://localhost:8000/objects/src/d.txt
SID3=$(curl -s -X POST http://localhost:8000/copy-sessions -H 'Content-Type: application/json' \
  -d '{"source_paths": ["src/d.txt"], "target_prefix": "backup/"}' \
  | python3 -c 'import sys,json;print(json.load(sys.stdin)["session"])')
# 会话已创建（基线记录目标 backup/src/d.txt 不存在）；随后独立发布完全相同的字节
curl -s -X PUT --data-binary 'delta' http://localhost:8000/objects/backup/src/d.txt
C3=$(curl -s "http://localhost:8000/copy-sessions/$SID3/segments" | python3 -c 'import sys,json;print(json.load(sys.stdin)["cursor"])')
curl -s -X POST "http://localhost:8000/copy-sessions/$SID3/acks" \
  -H 'Content-Type: application/json' -d "{\"cursor\": \"$C3\"}" | python3 -m json.tool
# -> 409
# {"status": "conflict", "applied": [],
#  "conflicts": [{"target_path": "backup/src/d.txt",
#                 "reason": "target_republished_same_content",
#                 "expected_sha256": "<sha256(delta)>",
#                 "actual_sha256": "<sha256(delta)>",   // 与期望相同，仍是冲突
#                 "actual_revision_no": 1,
#                 "baseline_revision_no": null, ...}],
#  "resume": {"next_segment": 0, "last_confirmed_cursor": null}}   // 游标不推进
# 重新确认重放同一结果；以新会话重新建档（新基线指向当前修订）后则幂等跳过

# 7. 断点续传：解决冲突后，用上次确认游标创建新会话，从确认处继续
#    （last_confirmed_cursor 取自上一次成功确认的响应 resume.last_confirmed_cursor）
curl -s -X POST http://localhost:8000/copy-sessions -H 'Content-Type: application/json' \
  -d '{"source_paths": ["src/a.txt", "src/b.txt", "src/c.txt"], "target_prefix": "backup/",
       "cursor": "<last_confirmed_cursor>"}' | python3 -m json.tool
```

## 本地开发

```bash
pip install -r requirements.txt
export DATABASE_URL=postgresql://origin:origin@localhost:5432/origin
uvicorn app.main:app --reload
```

## 测试

```bash
pip install pytest httpx pgserver   # pgserver 提供免安装的本地 PostgreSQL
python -m pytest
```

- `tests/test_etags.py` / `test_preconditions.py` / `test_ranges.py`：纯逻辑单元测试（ETag 强弱比较、§13.2.2 求值顺序、范围解析与合并、边界确定性）。
- `tests/test_copy_cursor.py`：复制游标与清单摘要的纯逻辑单元测试（编解码往返、防篡改、确定性）。
- `tests/test_integration.py`：真实 PostgreSQL + 完整 HTTP 服务的端到端测试，覆盖 200/204/206/304/404/412/416、多段 multipart 确定性、`If-Range`、并发"仅创建"只有一个成功、以及流式传输期间发布新版本不混入内容。
- `tests/test_copy_sessions.py`：复制会话端到端测试，覆盖清单顺序与摘要、分段重复拉取字节一致、源对象发布新修订后仍返回快照内容、原子应用、冲突整批回滚与结构化结果、**建档后同字节重新发布仍判冲突（含已存在目标的同内容新修订、整批回滚、新会话恢复）**、升级前无基线会话返回 `baseline_missing` 不按摘要放行、重放返回同一批次结果、乱序/伪造游标拒绝、跨会话断点续传与冲突恢复、并发确认收敛。

## 设计要点

- **数据模型**：`objects(path, current_id)` 指向 `revisions(id, object_path, revision_no, content bytea, sha256, media_type, created_at)`；`created_at` 默认 `date_trunc('second', now())`，保证秒级 `Last-Modified`。
- **事务与锁**：GET/HEAD 在事务内 `SELECT ... FOR SHARE` 锁定对象行并读取当前修订，流式响应在事务提交前持续持有锁；PUT 先 `INSERT ... ON CONFLICT DO NOTHING` 再 `FOR UPDATE`，并发发布串行执行，前置条件在锁内求值，因此"检查-然后-写"不会交错。
- **流式隔离**：响应体由 `StreamingResponse` 从被锁定的修订分块读出（`substring(content ...)`），事务在最后一个字节发送后才提交，新修订的发布会等待锁释放，故响应内容、ETag、`Content-Range` 始终属于同一修订。
- **确定性 multipart**：边界 = `SHA-256(ETag | media-type | 升序合并后的区间)`，不含任何随机因素。
- **复制会话快照**：`copy_sessions` + `copy_session_entries` 在创建事务内先对源集与全部目标路径（先 `INSERT ... ON CONFLICT DO NOTHING` 补齐占位行）按全局路径升序 `FOR SHARE` 锁定，再在锁内读取源修订；清单钉住 `revision_id`（修订行永不更新/删除），因此分段内容在会话生命周期内不可变。
- **目标修订基线**：建档时在同一锁快照中把每个目标路径的当前修订身份写入 `copy_session_target_baselines`，目标不存在记 NULL。确认时以修订 `id`（而非摘要）判定是否变更：摘要相同只能证明字节相同，不能区分"自基线未改动"与"基线后又发布了同字节修订"，后者必须报冲突。占位行保证"目标不存在"这一基线也被行锁覆盖，并发首发布会阻塞到建档事务提交。
- **确定性游标**：游标 = HMAC-SHA-256 签名的 `(会话, 段号, 清单摘要, 段末路径, 目标前缀, 源集摘要)`，是同一会话状态的纯函数——重发必相同、跨会话/跨清单必失效（`cursor_expired`）；签名密钥存于 `service_meta` 表，重启后游标仍有效。
- **原子批次确认**：确认在单事务内按目标路径升序取 `FOR UPDATE` 锁（与建档的 `FOR SHARE`、并发发布及其它会话使用同一全局锁序，无死锁地串行化），逐目标与基线比较修订身份：基线不存在且仍不存在则创建、身份未变且摘要相同则幂等跳过、身份变化（含同字节重发，`reason=target_republished_same_content`）或摘要分歧（`content_diverged`）记为冲突；整批应用 + ack 记录 + 进度推进同事务提交，冲突则整批回滚后单独落库冲突结果。批次结果以**文本**形式存储（保持原始键序），重放与首次响应字节一致；`INSERT ... ON CONFLICT DO NOTHING` 使并发确认收敛到同一条结果。
- **升级兼容**：表结构由启动时的 `CREATE TABLE IF NOT EXISTS` 自动补齐，旧库重启即有基线表；升级前创建的未完成会话没有基线行，确认未完成段时返回 `409 baseline_missing` 且不推进游标（绝不按摘要放行），重新创建会话取得新基线后继续；已经持久化的历史确认结果仍优先重放，不受影响。
