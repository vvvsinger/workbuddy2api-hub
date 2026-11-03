#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""wb_taskqueue.py —— 成长任务的**异步队列**：把「一键跑全部账号」从请求线程里搬出来。

## 为什么需要这个模块

原来的 `/tasks/run`（`wb_proxy._route_tasks_run`）是在 **HTTP 请求线程里串行循环**
所有账号的：跑一轮几分钟，请求就一直挂着；中途看不到任何进度；客户端超时后重发
就会**并发两个跑同一批账号**——成长任务的上报和领奖都不幂等，重复跑会互相踩。

本模块把它拆成三层，和 `wb_media_tasks` 是同一个思路：

    请求线程：校验 → 建序号 → 起 worker → 立刻返回 202
    worker  ：按账号并发（账号内串行）→ 逐个账号跑 run_growth_tasks
    状态    ：seq / 每账号状态 / 最近进度行，随时可查

## 三条设计约束

1. **账号内必须串行**：`run_growth_tasks` 里有 gap 防风控，两个线程跑同一账号
   会互相把进度顶乱（上游按 (eventCode, id) 去重）。所以并发只发生在**账号之间**。
2. **不碰上游实现**：本模块只调 `wb_tasks` 的公开函数，任务码→动作的映射仍然只有
   `wb_tasks.TASK_SPECS` 一处（panel 那边是 `autotask.go` 再写一份，等于两个真相源）。
3. **可注入**：任务执行函数从 `wb_tasks` 取，测试里直接替换，不需要网络。

## 与单账号手动执行的关系

`busy_uids()` 给 `_route_tasks_run` 用来拒绝「队列正在跑这个账号时又手动点一次」。
这是**最小接缝**：与其在 2.4 万行的 `wb_proxy.py` 里给整个函数加锁（冲突面巨大），
不如让两个入口自己在边界上互相让一步。
"""
import collections
import queue
import threading
import time

import wb_tasks

#: 跨账号并发默认值。上游对「同一 IP 短时间大量上报」敏感，2 是保守值；
#: 面板那边默认给 3，我们用 2 起步，可用 payload 覆盖但上限锁死。
DEFAULT_CONCURRENCY = 2
MAX_CONCURRENCY = 4
#: 账号之间的间隔，与 `run_growth_tasks` 内部 gap 分开：这是「换号」的停顿。
DEFAULT_ACCOUNT_GAP = 1.5
#: 最近进度行保留条数（给前端轮询用，不是审计日志）。
RECENT_LIMIT = 200

STATUS_PENDING = "pending"
STATUS_RUNNING = "running"
STATUS_DONE = "done"
STATUS_FAILED = "failed"
STATUS_CANCELLED = "cancelled"


class Runner(object):
    """一次只跑一个队列；重复 start 直接拒绝，避免两轮互相踩。"""

    def __init__(self):
        self._lock = threading.Lock()
        self._cancel = threading.Event()
        self._state = self._blank()

    # ---------------------------------------------------------------- 状态

    @staticmethod
    def _blank():
        return {
            "seq": 0,
            "running": False,
            "started_at": None,
            "finished_at": None,
            "total": 0,
            "done": 0,
            "failed": 0,
            "cancelled": 0,
            "earned": 0,
            "concurrency": 0,
            "claim": True,
            "accounts": {},
            "recent": collections.deque(maxlen=RECENT_LIMIT),
        }

    def reset(self):
        """仅测试用：把队列复位成空闲。"""
        with self._lock:
            self._cancel.set()
            self._state = self._blank()

    def status(self):
        with self._lock:
            state = self._state
            return {
                "ok": True,
                "seq": state["seq"],
                "running": state["running"],
                "started_at": state["started_at"],
                "finished_at": state["finished_at"],
                "total": state["total"],
                "done": state["done"],
                "failed": state["failed"],
                "cancelled": state["cancelled"],
                "earned": round(state["earned"], 2),
                "concurrency": state["concurrency"],
                "accounts": [dict(v) for v in state["accounts"].values()],
                "recent": list(state["recent"]),
            }

    def busy_uids(self):
        """正在队列里（或排队中）的账号 uid 集合。"""
        with self._lock:
            return {uid for uid, item in self._state["accounts"].items()
                    if item["status"] in (STATUS_PENDING, STATUS_RUNNING)}

    # ---------------------------------------------------------------- 控制

    def cancel(self):
        with self._lock:
            if not self._state["running"]:
                return {"ok": False, "msg": "当前没有在跑的任务队列"}
            seq = self._state["seq"]
            self._cancel.set()
            for item in self._state["accounts"].values():
                if item["status"] == STATUS_PENDING:
                    item["status"] = STATUS_CANCELLED
                    self._state["cancelled"] += 1
            self._note_locked("队列已请求取消：排队中的账号不再启动，正在跑的账号会跑完当前账号", "warn")
            return {"ok": True, "seq": seq, "msg": "已请求取消（正在跑的账号会跑完当前这一轮）"}

    def start(self, accounts, concurrency=DEFAULT_CONCURRENCY, claim=True,
              gap=1.0, account_gap=DEFAULT_ACCOUNT_GAP):
        """启动一轮队列，立刻返回 seq 与总账号数。"""
        targets = [a for a in (accounts or []) if getattr(a, "realm", "") == "cn"]
        if not targets:
            return {"ok": False, "msg": "没有可执行的国内版账号（成长任务只适用于 cn 区）"}
        try:
            concurrency = int(concurrency or DEFAULT_CONCURRENCY)
        except (TypeError, ValueError):
            concurrency = DEFAULT_CONCURRENCY
        concurrency = max(1, min(MAX_CONCURRENCY, concurrency))

        with self._lock:
            if self._state["running"]:
                return {"ok": False,
                        "msg": "已有任务队列在跑（seq=%d），等它跑完或先取消"
                               % self._state["seq"]}
            self._cancel.clear()
            state = self._blank()
            state["seq"] = self._state["seq"] + 1
            state["running"] = True
            state["started_at"] = int(time.time())
            state["total"] = len(targets)
            state["concurrency"] = concurrency
            state["claim"] = bool(claim)
            for acc in targets:
                uid = acc.uid
                state["accounts"][uid] = {
                    "uid": uid,
                    "name": getattr(acc, "nickname", "") or uid[:8],
                    "status": STATUS_PENDING,
                    "earned": 0,
                    "current_task": "",
                    "error": "",
                    "started_at": None,
                    "finished_at": None,
                }
            self._state = state
            seq = state["seq"]
            self._note_locked("队列启动：%d 个账号，并发 %d" % (len(targets), concurrency), "info")

        work = queue.Queue()
        for acc in targets:
            work.put(acc)
        for index in range(min(concurrency, len(targets))):
            thread = threading.Thread(target=self._worker, name="wb-taskqueue-%d" % index,
                                      args=(work, claim, gap, account_gap), daemon=True)
            thread.start()
        return {"ok": True, "seq": seq, "total": len(targets), "concurrency": concurrency,
                "hint": "轮询 GET /tasks/queue/status 查看进度"}

    # ---------------------------------------------------------------- 执行

    def _worker(self, work, claim, gap, account_gap):
        while True:
            try:
                acc = work.get_nowait()
            except queue.Empty:
                return
            uid = getattr(acc, "uid", "")
            if self._cancel.is_set():
                with self._lock:
                    item = self._state["accounts"].get(uid)
                    if item is not None and item["status"] == STATUS_PENDING:
                        item["status"] = STATUS_CANCELLED
                        self._state["cancelled"] += 1
                continue
            self._mark(uid, status=STATUS_RUNNING, started_at=int(time.time()))
            started = time.time()
            try:
                result = wb_tasks.run_growth_tasks(
                    acc, gap=gap, claim=claim,
                    progress=self._progress_for(uid))
                earned = (result or {}).get("earned_credit") or 0
                ok = bool((result or {}).get("ok", True))
                msg = (result or {}).get("msg") or ""
                self._mark(uid, status=STATUS_DONE if ok else STATUS_FAILED,
                           earned=earned, finished_at=int(time.time()),
                           error="" if ok else (msg or "执行失败"))
                with self._lock:
                    self._state["earned"] += float(earned or 0)
                    if ok:
                        self._state["done"] += 1
                        self._note_locked("[%s] 完成，+%s 积分，用时 %.0fs"
                                          % (self._name(uid), earned, time.time() - started), "ok")
                    else:
                        self._state["failed"] += 1
                        self._note_locked("[%s] 失败：%s" % (self._name(uid), msg or "未知原因"), "error")
            except Exception as exc:                          # noqa: BLE001
                # 一个账号炸掉不能带走整轮：其余账号继续。
                self._mark(uid, status=STATUS_FAILED, finished_at=int(time.time()),
                           error=str(exc))
                with self._lock:
                    self._state["failed"] += 1
                    self._note_locked("[%s] 异常：%s" % (self._name(uid), exc), "error")
            finally:
                self._maybe_finish()
                if account_gap:
                    time.sleep(account_gap)

    def _maybe_finish(self):
        with self._lock:
            state = self._state
            pending = [i for i in state["accounts"].values()
                       if i["status"] in (STATUS_PENDING, STATUS_RUNNING)]
            if pending:
                return
            state["running"] = False
            state["finished_at"] = int(time.time())
            self._note_locked("队列结束：成功 %d / 失败 %d / 取消 %d，累计 +%s 积分"
                              % (state["done"], state["failed"], state["cancelled"],
                                 round(state["earned"], 2)), "info")

    def _progress_for(self, uid):
        """给一个账号造 progress 回调（闭包持有 uid，线程间不共享）。"""
        def cb(code, name, phase):
            label = "%s（%s）" % (name or code, phase)
            self._mark(uid, current_task=label)
            if phase in ("done", "claimed"):
                self._note_locked("[%s] %s" % (self._name(uid), label), "task")
        return cb

    # ---------------------------------------------------------------- 内部

    def _name(self, uid):
        item = self._state["accounts"].get(uid) or {}
        return item.get("name") or (uid[:8] if uid else "?")

    def _mark(self, uid, **fields):
        with self._lock:
            item = self._state["accounts"].get(uid)
            if item is not None:
                item.update(fields)

    def _note_locked(self, message, level="info"):
        """追加一行进度（调用方已持锁）。"""
        self._state["recent"].append({
            "ts": int(time.time()),
            "level": level,
            "msg": message,
        })


_RUNNER = Runner()


# -------------------------------------------------------------------- 模块级 API

def start(accounts, payload=None, **overrides):
    """启动一轮队列。`payload` 是 HTTP 入参，`overrides` 供脚本/测试直接传参。"""
    opts = dict(payload or {})
    opts.update(overrides)
    return _RUNNER.start(
        accounts,
        concurrency=opts.get("concurrency") or DEFAULT_CONCURRENCY,
        claim=opts.get("claim", True) is not False,
        gap=opts.get("gap", 1.0),
        account_gap=opts.get("account_gap", DEFAULT_ACCOUNT_GAP),
    )


def status():
    return _RUNNER.status()


def cancel():
    return _RUNNER.cancel()


def busy_uids():
    return _RUNNER.busy_uids()


def reset():
    _RUNNER.reset()


# -------------------------------------------------------------------- 补领

def sweep(accounts, gap=1.0):
    """周期补领：只发 accept / claim 两条**幂等**请求，不跑任何对话或上报。

    对应面板的 `tasksweep.go`。真实对话点亮的任务不在自动动作白名单里，队列与
    「一键完成」都不会碰它，积分于是卡在「进度已达标、没领奖」——这一趟专门捞这种。
    """
    stats = {"ok": True, "accounts": 0, "accepted": 0, "claimed": 0,
             "credit": 0.0, "errors": []}
    for acc in accounts or []:
        if getattr(acc, "realm", "") != "cn":
            continue
        stats["accounts"] += 1
        uid = getattr(acc, "uid", "")
        try:
            tasks = wb_tasks.fetch_growth_tasks(acc)
        except Exception as exc:                              # noqa: BLE001
            stats["errors"].append("%s: 拉取任务失败 %s" % (uid[:8], exc))
            continue
        to_accept = [t["task_code"] for t in tasks
                     if t.get("status") == "not_accepted"
                     and not t.get("unforgeable")
                     and t.get("task_type") != "auto"]
        if to_accept:
            try:
                res = wb_tasks.accept_tasks(acc, to_accept)
                stats["accepted"] += len(res.get("accepted") or [])
            except Exception as exc:                          # noqa: BLE001
                stats["errors"].append("%s: 接取失败 %s" % (uid[:8], exc))
        for task in tasks:
            if task.get("status") == "claimed":
                continue
            if (task.get("current") or 0) < (task.get("target") or 1):
                continue
            try:
                claimed = wb_tasks.claim_task(acc, task["task_code"])
            except Exception as exc:                          # noqa: BLE001
                stats["errors"].append("%s/%s: 领奖失败 %s"
                                       % (uid[:8], task.get("task_code"), exc))
                continue
            if claimed.get("ok"):
                stats["claimed"] += 1
                stats["credit"] += float(claimed.get("credit") or 0)
        if gap:
            time.sleep(gap)
    stats["credit"] = round(stats["credit"], 2)
    return stats
