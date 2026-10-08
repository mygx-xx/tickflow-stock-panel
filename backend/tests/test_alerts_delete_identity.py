"""触发记录删除定位 — ts 是"落盘时刻", 批量写入时整批共享同一个 ts。

实测近 7 天 408 条记录只有 2 个 ts (单批 213 条同 ts), 所以只按 ts 删除会删错
记录; 前端点击删除时必须能用身份字段精确定位到被点的那一条。
"""
from __future__ import annotations

import json
import time
from types import SimpleNamespace

from fastapi import FastAPI
from fastapi.testclient import TestClient

from app.api.alerts import router
from app.services import alert_store


def _client(tmp_path) -> TestClient:
    app = FastAPI()
    app.include_router(router)
    app.state.repo = SimpleNamespace(store=SimpleNamespace(data_dir=tmp_path))
    return TestClient(app)


def _read(tmp_path) -> list[dict]:
    p = tmp_path / "user_data" / "alerts.jsonl"
    if not p.exists():
        return []
    return [json.loads(line) for line in p.read_text(encoding="utf-8").splitlines() if line.strip()]


def _seed_same_ts(tmp_path) -> int:
    """写入一批共享同一个 ts 的记录 (模拟 append_many 批量落盘)。"""
    ts = int(time.time() * 1000)
    alert_store.append_many(
        tmp_path,
        [
            {
                "ts": ts, "rule_id": "r1", "rule_name": "策略监控 · A", "source": "strategy",
                "type": "pool_enter", "symbol": "600001.SH", "name": "甲",
                "message": "策略「A」进入 甲", "severity": "info",
            },
            {
                "ts": ts, "rule_id": "r1", "rule_name": "策略监控 · A", "source": "strategy",
                "type": "pool_exit", "symbol": "600002.SH", "name": "乙",
                "message": "策略「A」移出 乙", "severity": "info",
            },
            {
                # 与第 2 条完全同 ts / symbol / rule_name, 仅 type+message 不同
                "ts": ts, "rule_id": "r2", "rule_name": "策略监控 · A", "source": "strategy",
                "type": "pool_enter", "symbol": "600002.SH", "name": "乙",
                "message": "策略「A」进入 乙", "severity": "info",
            },
        ],
    )
    return ts


def test_same_ts_records_all_persisted(tmp_path):
    """锁住前提: 同 ts 的多条记录确实共存于文件中。"""
    ts = _seed_same_ts(tmp_path)
    rows = _read(tmp_path)
    assert len(rows) == 3
    assert {r["ts"] for r in rows} == {ts}


def test_delete_uses_identity_fields_not_ts_only(tmp_path):
    ts = _seed_same_ts(tmp_path)
    client = _client(tmp_path)

    resp = client.delete(f"/api/alerts/{ts}", params={
        "symbol": "600002.SH", "type": "pool_enter", "rule_name": "策略监控 · A",
        "message": "策略「A」进入 乙",
    })
    assert resp.status_code == 200, resp.text

    rows = _read(tmp_path)
    assert len(rows) == 2
    # 被删的是精确命中的那条, 同 symbol 的 pool_exit 必须留下
    assert not any(r["type"] == "pool_enter" and r["symbol"] == "600002.SH" for r in rows)
    assert any(r["type"] == "pool_exit" and r["symbol"] == "600002.SH" for r in rows)
    assert any(r["symbol"] == "600001.SH" for r in rows)


def test_delete_identity_mismatch_keeps_everything(tmp_path):
    ts = _seed_same_ts(tmp_path)
    resp = _client(tmp_path).delete(f"/api/alerts/{ts}", params={"symbol": "999999.SH"})
    assert resp.status_code == 404
    assert len(_read(tmp_path)) == 3


def test_delete_without_identity_still_deletes_first_match(tmp_path):
    """兼容既有无身份字段的调用: 退化为"删第一条同 ts 记录"。"""
    ts = _seed_same_ts(tmp_path)
    resp = _client(tmp_path).delete(f"/api/alerts/{ts}")
    assert resp.status_code == 200, resp.text
    assert len(_read(tmp_path)) == 2
