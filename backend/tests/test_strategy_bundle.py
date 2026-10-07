"""策略导出(备份包)端点测试。

为什么测这个: data/strategies 被 .gitignore 排除(data/**), 这些策略只存在
本机磁盘 —— 导出是唯一的备份手段, 一旦它静默少导/导空, 用户会在需要时才发现。

锚定的三件事:
  1. 源码必须真的进包(否则备份只有元信息, 还原不回去)
  2. 路由不能被 `/{strategy_id}` 抢走(/export-all 必须命中自己的 handler)
  3. 源码文件缺失时降级为 code_missing, 不让整包失败(一个坏文件不该毁掉备份)
"""
from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from app.api.strategy import bundle_router
from app.strategy.engine import StrategyEngine

STRATEGY_CODE = '''"""导出测试策略"""
import polars as pl

META = {
    "id": "bundle_demo",
    "name": "导出演示",
    "description": "用于验证导出包",
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
ENTRY_SIGNALS = []
EXIT_SIGNALS = []


def filter(df: pl.DataFrame, params: dict):
    return df
'''


def _client(tmp_path: Path, code: str = STRATEGY_CODE, write_file: bool = True) -> TestClient:
    strategy_dir = tmp_path / "strategies" / "custom"
    strategy_dir.mkdir(parents=True)
    if write_file:
        (strategy_dir / "bundle_demo.py").write_text(code, encoding="utf-8")

    engine = StrategyEngine(strategy_dirs=[strategy_dir])
    app = FastAPI()
    app.include_router(bundle_router)
    app.state.strategy_engine = engine
    app.state.repo = SimpleNamespace(store=SimpleNamespace(data_dir=tmp_path))
    return TestClient(app)


@pytest.fixture
def client(tmp_path: Path) -> TestClient:
    return _client(tmp_path)


def test_单个导出含完整源码(client):
    r = client.get("/api/strategy-bundle/bundle_demo")
    assert r.status_code == 200
    body = r.json()
    assert body["format"] == "tickflow-strategy-bundle"
    assert body["format_version"] == 1
    assert body["count"] == 1
    entry = body["strategies"][0]
    assert entry["id"] == "bundle_demo"
    assert entry["source"] == "custom"
    assert entry["status"] == "draft"          # 未声明归一为 draft
    assert entry["file_path"].endswith("bundle_demo.py")
    # 备份的关键: 源码必须完整进包
    assert "META" in entry["code"] and "EXECUTION_BACKEND" in entry["code"]
    assert entry["meta"]["name"] == "导出演示"
    assert body["code_missing"] == []


def test_export_all_不被动态路由抢走(client):
    """`/export-all` 若被 `@router.get("/{strategy_id}")` 抢先匹配会 404。"""
    r = client.get("/api/strategy-bundle/export-all")
    assert r.status_code == 200
    body = r.json()
    assert body["count"] == 1
    assert body["strategies"][0]["id"] == "bundle_demo"
    assert body["exported_at"]


def test_按来源过滤(client):
    assert client.get("/api/strategy-bundle/export-all?source=custom").json()["count"] == 1
    assert client.get("/api/strategy-bundle/export-all?source=builtin").json()["count"] == 0


def test_按状态过滤(client):
    assert client.get("/api/strategy-bundle/export-all?status=draft").json()["count"] == 1
    assert client.get("/api/strategy-bundle/export-all?status=active").json()["count"] == 0


def test_源码文件缺失时降级不中断(tmp_path: Path):
    """引擎已把策略加载进内存, 之后源文件被删 -> code=None 且列入 code_missing。

    不能抛异常: 一次误删不该让整包导出失败(那正是最需要备份的时刻)。
    注意必须"先建引擎再删文件" —— 文件不在时引擎压根不会注册它, 那是另一条路径。
    """
    c = _client(tmp_path, write_file=True)
    (tmp_path / "strategies" / "custom" / "bundle_demo.py").unlink()

    r = c.get("/api/strategy-bundle/export-all")
    assert r.status_code == 200
    body = r.json()
    assert body["count"] == 1
    assert body["strategies"][0]["code"] is None
    assert body["code_missing"] == ["bundle_demo"]


def test_不存在的策略404(client):
    assert client.get("/api/strategy-bundle/nope_xxx").status_code == 404


def test_包内含恢复所需的全部字段(client):
    """备份要能还原: 源码 + META + 用户参数覆盖 + 身份信息, 缺一不可。"""
    entry = client.get("/api/strategy-bundle/bundle_demo").json()["strategies"][0]
    for key in ("id", "name", "source", "status", "file_path", "meta", "overrides", "code"):
        assert key in entry, f"导出条目缺少 {key}"
