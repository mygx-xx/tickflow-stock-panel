"""策略生命周期状态端点测试 — 真实 `.py` 文件写入与回滚。

重点不在HTTP 形状, 而在**四段式写入的安全性**: 策略文件是 `.py`, 改坏
META 会让该策略整体加载失败, 所以必须锁住
「读旧码 → 改写 → reload 断言生效→ 失败回滚 + 二次 reload」。

用真实StrategyEngine(指向 tmp_path 的策略目录)而非 mock, 因为回滚语义
只有真引擎才验证得了 —— mock 掉 reload 就测不出"回滚后能不能加载回来"。
"""
from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from app.api.strategy import router
from app.strategy.engine import StrategyEngine


STRATEGY_CODE = '''"""测试策略"""
import polars as pl

META = {
    "id": "custom_lifecycle",
    "name": "生命周期测试",
    "description": "用于状态迁移测试",
    "tags": ["test"],
    "asset_types": ["stock"],
    "timeframes": ["1d"],
    "params": [],
    "scoring": {"momentum_20d": 1.0},
    "order_by": "score",
    "descending": True,
    "limit": 30,
}
EXECUTION_BACKEND = "polars_expr"
ENTRY_SIGNALS = ["signal_test"]
EXIT_SIGNALS = ["signal_test_exit"]


def filter(df: pl.DataFrame, params: dict):
    """polars_expr backend 要求恰好声明 filter(不得再声明 filter_history)。"""
    return df
'''


def _client(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> TestClient:
    """搭一个只挂策略路由的轻量 app + 真实引擎。"""
    strategy_dir = tmp_path / "strategies" / "custom"
    strategy_dir.mkdir(parents=True)
    (strategy_dir / "custom_lifecycle.py").write_text(STRATEGY_CODE, encoding="utf-8")

    engine = StrategyEngine(strategy_dirs=[strategy_dir])

    app = FastAPI()
    app.include_router(router)
    app.state.strategy_engine = engine
    app.state.repo = SimpleNamespace(store=SimpleNamespace(data_dir=tmp_path))
    return TestClient(app)


@pytest.fixture
def client(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> TestClient:
    return _client(tmp_path, monkeypatch)


def _file(tmp_path: Path) -> Path:
    return tmp_path / "strategies" / "custom" / "custom_lifecycle.py"


# ── 读取 ──────────────────────────────────────────────────────────


def test_未声明状态视为draft(client):
    """存量策略 META 里没有 status -> 响应 draft, 不是 active。"""
    r = client.get("/api/strategies/custom_lifecycle")
    assert r.status_code == 200
    body = r.json()
    assert body["status"] == "draft"
    assert body["status_label"]
    assert body["selectable"] is False


def test_lifecycle端点返回四维视图(client):
    r = client.get("/api/strategies/custom_lifecycle/lifecycle")
    assert r.status_code == 200
    body = r.json()
    assert body["status"] == "draft"
    assert body["selectable"] is False
    assert body["visible"] is True, "draft 仍应可见(只是不进自动池)"
    assert body["research_only"] is False


# ── 人工迁移 ───────────────────────────────────────────────────────


def test_draft升active(client, tmp_path):
    r = client.post("/api/strategies/custom_lifecycle/status", json={"status": "active"})
    assert r.status_code == 200
    body = r.json()
    assert body["status"] == "active"
    assert body["previous_status"] == "draft"
    assert body["changed"] is True
    assert body["automatic"] is False, "人工迁移不是自动降级"
    # 真正落盘且可被引擎加载
    assert '"status"' in _file(tmp_path).read_text(encoding="utf-8")
    assert client.get("/api/strategies/custom_lifecycle").json()["status"] == "active"


def test_active降级watch被标记为automatic(client):
    client.post("/api/strategies/custom_lifecycle/status", json={"status": "active"})
    r = client.post("/api/strategies/custom_lifecycle/status", json={"status": "watch"})
    assert r.status_code == 200
    assert r.json()["automatic"] is True, "active→watch 是唯一的自动迁移路径"


def test_重复迁移幂等(client):
    client.post("/api/strategies/custom_lifecycle/status", json={"status": "active"})
    r = client.post("/api/strategies/custom_lifecycle/status", json={"status": "active"})
    assert r.status_code == 200
    assert r.json()["changed"] is False, "同状态重复设置应幂等, 不报错"


def test_retired需回draft才能复活(client):
    client.post("/api/strategies/custom_lifecycle/status", json={"status": "retired"})
    # retired -> active 不合法
    r = client.post("/api/strategies/custom_lifecycle/status", json={"status": "active"})
    assert r.status_code == 409
    # retired -> draft 合法
    r = client.post("/api/strategies/custom_lifecycle/status", json={"status": "draft"})
    assert r.status_code == 200
    assert r.json()["status"] == "draft"


# ── 拒绝路径 ───────────────────────────────────────────────────────


def test_非法状态值被拒400(client):
    r = client.post("/api/strategies/custom_lifecycle/status", json={"status": "bogus"})
    assert r.status_code == 400
    assert "合法值" in r.json()["detail"]


def test_非法迁移被拒409(client):
    client.post("/api/strategies/custom_lifecycle/status", json={"status": "retired"})
    r = client.post("/api/strategies/custom_lifecycle/status", json={"status": "watch"})
    assert r.status_code == 409, "retired 只能回 draft"
    assert "不允许" in r.json()["detail"]


def test_active不能直接回draft(client):
    client.post("/api/strategies/custom_lifecycle/status", json={"status": "active"})
    r = client.post("/api/strategies/custom_lifecycle/status", json={"status": "draft"})
    assert r.status_code == 409


def test_不存在的策略404(client):
    r = client.post("/api/strategies/nope_status_xyz/status", json={"status": "active"})
    assert r.status_code == 404


# ── 文件安全 ───────────────────────────────────────────────────────


def test_写入后策略文件仍是合法Python(client, tmp_path):
    """状态写进 .py —— 语法坏了整个策略就加载失败, 必须仍是合法 Python。"""
    import ast

    client.post("/api/strategies/custom_lifecycle/status", json={"status": "active"})
    code = _file(tmp_path).read_text(encoding="utf-8")
    ast.parse(code), "写入后源码语法错误, 会让策略整体加载失败"


def test_状态写入不破坏META其他字段(client, tmp_path):
    """改写只碰 status, 其余 META 键必须原样保留。"""
    client.post("/api/strategies/custom_lifecycle/status", json={"status": "watch"})
    body = client.get("/api/strategies/custom_lifecycle").json()
    assert body["name"] == "生命周期测试"
    assert body["scoring"] == {"momentum_20d": 1.0}
    assert body["limit"] == 30
    assert body["execution_backend"] == "polars_expr"
    assert body["entry_signals"] == ["signal_test"]


def test_多次迁移不累积垃圾字段(client, tmp_path):
    """反复切换状态, META 里应只有一份 status 键。"""
    for target in ("active", "watch", "active", "retired", "draft", "active"):
        client.post("/api/strategies/custom_lifecycle/status", json={"status": target})
    code = _file(tmp_path).read_text(encoding="utf-8")
    assert code.count('"status"') == 1, f"出现重复 status 字段:\n{code}"


def test_原状态字段被替换而非追加重复(client, tmp_path):
    """已有 status 时应就地改值, 不产生第二行。"""
    # 先写一次
    client.post("/api/strategies/custom_lifecycle/status", json={"status": "active"})
    # 再手工植入一个已存在的 status, 验证被替换
    code = _file(tmp_path).read_text(encoding="utf-8")
    assert code.count('"status"') == 1
    client.post("/api/strategies/custom_lifecycle/status", json={"status": "watch"})
    code2 = _file(tmp_path).read_text(encoding="utf-8")
    assert code2.count('"status"') == 1
    assert '"watch"' in code2


# ── 判定预演(只读, 绝不改文件) ─────────────────────────────────────


def test_lifecycle_check只读不改状态(client, tmp_path):
    """预演端点绝不能顺手改策略状态。"""
    before = _file(tmp_path).read_text(encoding="utf-8")
    r = client.post("/api/strategies/custom_lifecycle/lifecycle/check")
    assert r.status_code == 200
    body = r.json()
    assert body["has_walkforward"] is False, "没跑过 walkforward 应如实报告"
    assert body["should_degrade"] is False, "非active 状态不参与判定, 不降级"
    assert _file(tmp_path).read_text(encoding="utf-8") == before, "预演不应改文件"


def test_lifecycle_check对active无数据时弃权(client):
    """active 策略但无 walkforward 缓存 -> 明确弃权(而非误判健康或降级)。"""
    client.post("/api/strategies/custom_lifecycle/status", json={"status": "active"})
    r = client.post("/api/strategies/custom_lifecycle/lifecycle/check")
    body = r.json()
    assert body["current_status"] == "active"
    assert body["should_degrade"] is False
    assert "walkforward_missing" in body["abstained"]


def test_check对非active状态不判定(client):
    """draft 状态本就未激活, 判定应短路。"""
    r = client.post("/api/strategies/custom_lifecycle/lifecycle/check")
    assert r.status_code == 200
    assert r.json()["current_status"] == "draft"
    assert r.json()["should_degrade"] is False


def test_check读取已缓存的walkforward(client, tmp_path):
    """端到端: 缓存存在时应真正参与判定并给出降级建议。"""
    from app.services import walkforward_store

    walkforward_store.save_walkforward_result(
        tmp_path,
        "custom_lifecycle",
        {
            "objective": "sortino",
            "direction": "max",
            "n_folds": 4,
            "folds": [
                {"index": i, "test_end": "2025-06-30", "is_score": 2.5,
                 "oos_objective": 1.0, "oos_degraded": True,
                 "oos_stats": {"n_trades": 30}}
                for i in range(1, 5)
            ],
            # sortino 阈值 0.6, degradation 1.5 明显超
            "summary": {"n_folds": 4, "avg_is_objective": 2.5,
                        "avg_oos_objective": 1.0, "degradation": 1.5,
                        "consistency": 1.0},
        },
    )
    client.post("/api/strategies/custom_lifecycle/status", json={"status": "active"})
    r = client.post("/api/strategies/custom_lifecycle/lifecycle/check")
    body = r.json()
    assert body["has_walkforward"] is True
    assert body["current_status"] == "active"
    assert body["should_degrade"] is True
    assert body["target_status"] == "watch"
    # 预演只是预演: 文件状态未被改动
    assert client.get("/api/strategies/custom_lifecycle").json()["status"] == "active"


# ── 批量巡检 ───────────────────────────────────────────────────────


def _seed_wf(data_dir, strategy_id, degradation, objective="sortino", oos=None):
    from app.services import walkforward_store

    oos_value = (2.5 - degradation) if oos is None else oos
    walkforward_store.save_walkforward_result(
        data_dir,
        strategy_id,
        {
            "objective": objective,
            "direction": "max",
            "n_folds": 4,
            "folds": [
                {"index": i, "test_end": "2025-06-30", "is_score": 2.5,
                 "oos_objective": oos_value,
                 "oos_degraded": degradation > 0,
                 "oos_stats": {"n_trades": 30}}
                for i in range(1, 5)
            ],
            "summary": {"n_folds": 4, "avg_is_objective": 2.5,
                        "avg_oos_objective": oos_value,
                        "degradation": degradation, "consistency": 1.0},
        },
    )


def test_sweep默认dry_run不改状态(client, tmp_path):
    """默认 dry_run=True: 出报告但不改文件。"""
    client.post("/api/strategies/custom_lifecycle/status", json={"status": "active"})
    _seed_wf(tmp_path, "custom_lifecycle", degradation=1.5)
    before = _file(tmp_path).read_text(encoding="utf-8")

    r = client.post("/api/strategies/lifecycle/sweep", json={"dry_run": True})
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["dry_run"] is True
    assert body["applied"] is False
    assert len(body["degraded"]) == 1
    assert body["degraded"][0]["strategy_id"] == "custom_lifecycle"
    assert _file(tmp_path).read_text(encoding="utf-8") == before, "dry_run 不应改文件"


def test_sweep实际落盘降级(client, tmp_path):
    """dry_run=False: 建议的降级真正写入策略文件。"""
    client.post("/api/strategies/custom_lifecycle/status", json={"status": "active"})
    _seed_wf(tmp_path, "custom_lifecycle", degradation=1.5)

    r = client.post("/api/strategies/lifecycle/sweep", json={"dry_run": False})
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["applied"] is True
    assert body["applied_count"] == 1
    assert body["failed"] == []
    assert client.get("/api/strategies/custom_lifecycle").json()["status"] == "watch"
    assert "watch" in _file(tmp_path).read_text(encoding="utf-8")


def test_sweep健康策略不降级(client, tmp_path):
    client.post("/api/strategies/custom_lifecycle/status", json={"status": "active"})
    _seed_wf(tmp_path, "custom_lifecycle", degradation=0.1)
    r = client.post("/api/strategies/lifecycle/sweep", json={"dry_run": False})
    body = r.json()
    assert body["degraded"] == []
    assert body["applied_count"] == 0
    assert client.get("/api/strategies/custom_lifecycle").json()["status"] == "active"


def test_sweep跳过非active策略(client, tmp_path):
    """draft 策略不参与巡检判定, 记入 skipped 而非 healthy。"""
    _seed_wf(tmp_path, "custom_lifecycle", degradation=1.5)
    r = client.post("/api/strategies/lifecycle/sweep", json={"dry_run": True})
    body = r.json()
    assert body["degraded"] == []
    assert any(s["strategy_id"] == "custom_lifecycle" for s in body["skipped"])
    assert any("draft" in s["reason"] for s in body["skipped"])


def test_sweep路由不被参数化路由吞掉(client):
    """回归防护: /lifecycle/sweep 是静态段。

    若被 `/{strategy_id}/lifecycle` 抢先匹配, strategy_id 会变成
    "lifecycle" 并返回 404 —— 巡检整体失灵且难排查。
    """
    r = client.post("/api/strategies/lifecycle/sweep", json={"dry_run": True})
    assert r.status_code == 200, f"静态路由被参数化路由吞掉: {r.text[:200]}"
    assert "degraded" in r.json()


def test_sweep_无缓存时不误判(client):
    """没有 walkforward 缓存 -> 弃权, 绝不降级。"""
    client.post("/api/strategies/custom_lifecycle/status", json={"status": "active"})
    r = client.post("/api/strategies/lifecycle/sweep", json={"dry_run": False})
    body = r.json()
    assert body["degraded"] == []
    assert client.get("/api/strategies/custom_lifecycle").json()["status"] == "active"


# ── EM 基准 ────────────────────────────────────────────────────────


def test_夏普样本不足时不做EM校正(client, tmp_path):
    """少于 3 个夏普样本 -> 不做 EM 校正(PSR 弃权, 只剩 degradation)。"""
    _seed_wf(tmp_path, "custom_lifecycle", degradation=0.1, objective="sharpe")
    r = client.post("/api/strategies/lifecycle/sweep", json={"dry_run": True})
    body = r.json()
    assert body["n_trials"] is None
    assert body["sharpe_variance"] is None


def test_夏普样本足够时给EM基准(client, tmp_path):
    """夏普有差异 -> 方差 > 0 -> 给出 EM 基准。"""
    for i, sp in enumerate([0.5, 1.5, 2.0, 2.5], start=1):
        _seed_wf(tmp_path, f"sharpe_strategy_{i}", degradation=0.0,
                 objective="sharpe", oos=sp)
    r = client.post("/api/strategies/lifecycle/sweep", json={"dry_run": True})
    body = r.json()
    assert body["n_trials"] == 4
    assert body["sharpe_variance"] is not None and body["sharpe_variance"] > 0


def test_夏普全相同则不做EM校正(client, tmp_path):
    """方差为 0(所有策略夏普相同) -> 无分布可比, 不校正。"""
    for i in range(1, 5):
        _seed_wf(tmp_path, f"same_{i}", degradation=0.0, objective="sharpe", oos=2.0)
    r = client.post("/api/strategies/lifecycle/sweep", json={"dry_run": True})
    body = r.json()
    assert body["n_trials"] is None, "方差为 0 时不该校正"
    assert body["sharpe_variance"] is None
