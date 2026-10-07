"""策略生命周期巡检调度测试 — 幂等/开关/降级落盘。

调度层的风险点与判定层不同: 巡检会**改策略文件**, 所以要锁住
「默认只报告不落盘」「同一天只跑一次」「开关关闭时不注册 job」。

不测 APScheduler 本身(那是三方库), 只测被调用的 `_strategy_lifecycle_task`
的行为边界 —— 调度注册的正误由daily_pipeline 的 import 与
get_strategy_lifecycle_schedule 覆盖。
"""
from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

import pytest

from app.jobs import daily_pipeline as dp
from app.services import preferences, walkforward_store


STRATEGY_CODE = '''"""测试策略"""
import polars as pl

META = {
    "id": "custom_lc",
    "name": "巡检测试",
    "description": "d",
    "asset_types": ["stock"],
    "timeframes": ["1d"],
    "params": [],
    "scoring": {"momentum_20d": 1.0},
}
EXECUTION_BACKEND = "polars_expr"
ENTRY_SIGNALS = ["signal_test"]
EXIT_SIGNALS = ["signal_test_exit"]


def filter(df: pl.DataFrame, params: dict):
    return df
'''


@pytest.fixture
def env(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    """构造 app_state: 真实策略引擎 + tmp data_dir + 隔离的 preferences 存储。

    preferences 的路径来自 `app.config.settings.data_dir`, 且 `load()` 带
    mtime 签名进程内缓存 —— 必须同时改 data_dir 并 `_invalidate_cache()`,
    否则测试之间会互相读到上一个用例的配置(已实测踩过)。
    """
    from app.config import settings
    from app.strategy.engine import StrategyEngine

    strategy_dir = tmp_path / "strategies" / "custom"
    strategy_dir.mkdir(parents=True)
    (strategy_dir / "custom_lc.py").write_text(STRATEGY_CODE, encoding="utf-8")
    engine = StrategyEngine(strategy_dirs=[strategy_dir])

    monkeypatch.setattr(settings, "data_dir", tmp_path)
    preferences._invalidate_cache()

    app_state = SimpleNamespace(
        strategy_engine=engine,
        repo=SimpleNamespace(store=SimpleNamespace(data_dir=tmp_path)),
    )
    return SimpleNamespace(app_state=app_state, tmp_path=tmp_path, engine=engine)


def _activate(app_state, tmp_path, engine):
    """把策略置为 active 并种一份高退化的 walkforward 缓存。"""
    from app.api.strategy import StrategyStatusRequest, set_strategy_status

    req = SimpleNamespace(app=SimpleNamespace(state=app_state))
    set_strategy_status(
        "custom_lc", StrategyStatusRequest(status="active", reason="test"), req
    )
    walkforward_store.save_walkforward_result(
        tmp_path,
        "custom_lc",
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
            "summary": {"n_folds": 4, "avg_is_objective": 2.5,
                        "avg_oos_objective": 1.0, "degradation": 1.5,
                        "consistency": 1.0},
        },
    )


def _claim_exists(tmp_path):
    return any(tmp_path.joinpath("strategy_lifecycle").glob("_claim_*.json"))


# ── 默认关闭 ───────────────────────────────────────────────────────


def test_默认关闭时不巡检(env, monkeypatch):
    """enabled=False -> 不产生任何副作用(连 claim 都不写)。"""
    monkeypatch.setattr(
        dp, "_holiday_skip", lambda label: False
    )
    result = dp._strategy_lifecycle_task(env.app_state)
    assert result is None
    assert not _claim_exists(env.tmp_path), "关闭时不该写 claim"


# ── 节假日门控 ─────────────────────────────────────────────────────


def test_节假日跳过不写claim(env, monkeypatch):
    monkeypatch.setattr(dp, "_holiday_skip", lambda label: True)
    preferences.set_strategy_lifecycle_schedule(True, 16, 30, False)
    result = dp._strategy_lifecycle_task(env.app_state)
    assert result is None
    assert not _claim_exists(env.tmp_path)


# ── 依赖缺失 fail-closed ────────────────────────────────────────────


def test_引擎未初始化时跳过(env, monkeypatch):
    monkeypatch.setattr(dp, "_holiday_skip", lambda label: False)
    preferences.set_strategy_lifecycle_schedule(True, 16, 30, False)
    empty = SimpleNamespace(app_state=SimpleNamespace(), tmp_path=env.tmp_path)
    assert dp._strategy_lifecycle_task(empty.app_state) is None


# ── 幂等 claim ─────────────────────────────────────────────────────


def test_同一天只巡检一次(env, monkeypatch):
    monkeypatch.setattr(dp, "_holiday_skip", lambda label: False)
    preferences.set_strategy_lifecycle_schedule(True, 16, 30, False)

    first = dp._strategy_lifecycle_task(env.app_state)
    assert first is not None, "首次应执行"
    assert _claim_exists(env.tmp_path)

    second = dp._strategy_lifecycle_task(env.app_state)
    assert second is None, "同日第二次应被 claim 拦下"


# ── 只报告不落盘(默认) ─────────────────────────────────────────────


def test_auto_degrade关闭时只报告不降级(env, monkeypatch):
    """两级闸的关键: enabled=True 但 auto_degrade=False -> 只报告。"""
    monkeypatch.setattr(dp, "_holiday_skip", lambda label: False)
    preferences.set_strategy_lifecycle_schedule(True, 16, 30, False)
    _activate(env.app_state, env.tmp_path, env.engine)

    report = dp._strategy_lifecycle_task(env.app_state)
    assert report is not None
    assert len(report["degraded"]) == 1, "应识别出衰退策略"
    # 但不落盘
    assert not report.get("applied")
    assert env.engine.get("custom_lc").meta.get("status") == "active"


# ── 自动降级落盘 ───────────────────────────────────────────────────


def test_auto_degrade开启时真正降级(env, monkeypatch):
    monkeypatch.setattr(dp, "_holiday_skip", lambda label: False)
    preferences.set_strategy_lifecycle_schedule(True, 16, 30, True)
    _activate(env.app_state, env.tmp_path, env.engine)

    report = dp._strategy_lifecycle_task(env.app_state)
    assert report is not None
    assert report.get("applied") is True
    assert report["applied_detail"] == ["custom_lc"]
    assert report["failed"] == []
    assert env.engine.get("custom_lc").meta.get("status") == "watch"


def test_降级后策略文件被改写(env, monkeypatch):
    """必须落到 .py 文件的 META 里, 而不是只在内存里改。"""
    monkeypatch.setattr(dp, "_holiday_skip", lambda label: False)
    preferences.set_strategy_lifecycle_schedule(True, 16, 30, True)
    _activate(env.app_state, env.tmp_path, env.engine)
    dp._strategy_lifecycle_task(env.app_state)

    code = env.tmp_path.joinpath("strategies/custom/custom_lc.py").read_text(
        encoding="utf-8"
    )
    assert '"watch"' in code
    assert code.count('"status"') == 1


# ── preferences 开关 ───────────────────────────────────────────────


@pytest.fixture(autouse=True)
def _isolated_prefs(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    """把所有 preferences 测试隔离到 tmp_path 并清掉进程内缓存。"""
    from app.config import settings

    monkeypatch.setattr(settings, "data_dir", tmp_path)
    preferences._invalidate_cache()
    yield
    preferences._invalidate_cache()


def test_schedule默认值全关():
    assert preferences.get_strategy_lifecycle_schedule() == {
        "enabled": False,
        "auto_degrade": False,
        "hour": 16,
        "minute": 30,
    }


def test_schedule保存读取往返():
    saved = preferences.set_strategy_lifecycle_schedule(True, 9, 5, True)
    assert saved == {"enabled": True, "auto_degrade": True, "hour": 9, "minute": 5}
    assert preferences.get_strategy_lifecycle_schedule() == saved


def test_schedule时间越界被夹紧():
    saved = preferences.set_strategy_lifecycle_schedule(True, 99, 99, False)
    assert saved["hour"] == 23
    assert saved["minute"] == 59


def test_schedule_损坏值回退默认():
    """脏数据不该让调度崩, 也不该意外开启自动降级。"""
    preferences.save({"strategy_lifecycle_schedule": "not-a-dict"})
    cfg = preferences.get_strategy_lifecycle_schedule()
    assert cfg["enabled"] is False
    assert cfg["auto_degrade"] is False


def test_schedule_非bool_enabled被纠正():
    """字符串 'yes' 会被 bool('yes') 判成 True —— 必须是真bool 才算开启。"""
    preferences.save(
        {"strategy_lifecycle_schedule": {"enabled": "yes", "auto_degrade": 1}}
    )
    cfg = preferences.get_strategy_lifecycle_schedule()
    assert cfg["enabled"] is False, "字符串 'yes' 不应被当成 True"
    assert cfg["auto_degrade"] is False