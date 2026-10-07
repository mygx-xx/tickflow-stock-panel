"""walkforward 结果落盘缓存测试 — 判定数据可持久性的回归防护。

背景: walkforward 结果原本只经 SSE 返回、从不落盘, 导致后台巡检读不到任何
绩效数据 → 每次判定都以「无数据」弃权 → 判据静默失效(不报错, 永不触发)。
本测试锁住「跑过一次 walkforward 后, 判定必须能读到」的链路。
"""
from __future__ import annotations

import json

from app.services import walkforward_store as ws
from app.services.strategy_lifecycle import judge


def _wf_result() -> dict:
    """模拟 WalkForwardService.run() 的返回(含会被丢弃的大字段)。"""
    return {
        "objective": "sortino",
        "direction": "max",
        "n_folds": 3,
        "n_skipped": 1,
        "n_planned_folds": 4,
        "folds": [
            {
                "index": i,
                "test_end": f"2025-0{i}-31",
                "best_params": {"lookback": [10, 20, 30]},  # 应被丢弃
                "is_score": 2.0,
                "oos_objective": 1.0,
                "oos_degraded": True,
                "oos_stats": {
                    "total_return": 0.03,
                    "n_trades": 30,
                    "equity_curve": [1.0, 1.03],  # 应被丢弃(大字段)
                },
            }
            for i in (1, 2, 3)
        ],
        "summary": {
            "n_folds": 3,
            "avg_is_objective": 2.0,
            "avg_oos_objective": 1.0,
            "degradation": 1.0,
            "consistency": 1.0,
            "oos_equity_curve": [{"fold": 1, "value": 1.03}],  # 应被丢弃
        },
        "cache_telemetry": {"hits": 10},  # 应被丢弃
        "shared_market_data_bytes": 123456,  # 应被丢弃
        "elapsed_ms": 9999,  # 应被丢弃
    }


def test_落盘后可读回(tmp_path):
    ws.save_walkforward_result(tmp_path, "s1", _wf_result())
    loaded = ws.load_walkforward_result(tmp_path, "s1")
    assert loaded is not None
    assert loaded["objective"] == "sortino"
    assert loaded["n_folds"] == 3


def test_落盘丢弃遥测与大字段(tmp_path):
    """遥测/运行时字段不落盘 —— 否则单文件会随标的数快速膨胀。"""
    ws.save_walkforward_result(tmp_path, "s1", _wf_result())
    loaded = ws.load_walkforward_result(tmp_path, "s1")
    for key in ("cache_telemetry", "shared_market_data_bytes", "elapsed_ms",
                "n_skipped", "n_planned_folds"):
        assert key not in loaded, f"{key} 不该落盘"
    assert "oos_equity_curve" not in loaded["summary"]
    assert "best_params" not in loaded["folds"][0]
    assert "equity_curve" not in loaded["folds"][0]["oos_stats"]


def test_保留判定必需字段(tmp_path):
    """degradation / consistency / n_trades 是判定的全部输入, 少一个就失效。"""
    ws.save_walkforward_result(tmp_path, "s1", _wf_result())
    loaded = ws.load_walkforward_result(tmp_path, "s1")
    assert loaded["summary"]["degradation"] == 1.0
    assert loaded["summary"]["consistency"] == 1.0
    assert loaded["folds"][0]["oos_stats"]["n_trades"] == 30
    assert loaded["folds"][0]["oos_degraded"] is True


def test_n_trades求和可驱动PSR(tmp_path):
    """端到端: 落盘 → 读回 → judge 能算出 PSR(证明 n_trades 没在链路里丢失)。

    若 _total_oos_trades 读错位置, 这里会是 None → psr_unavailable。
    """
    ws.save_walkforward_result(tmp_path, "s1", _wf_result())
    loaded = ws.load_walkforward_result(tmp_path, "s1")
    v = judge(
        strategy_id="s1",
        walkforward_result=loaded,
        current_status="active",
        n_trials=50,
        sharpe_variance=0.25,
    )
    assert v.metrics["n_trades"] == 90, "3 折 × 30 笔应汇总为 90"
    # degradation=1.0 > sortino 阈值 0.6 -> 降级
    assert v.should_degrade
    assert v.target_status == "watch"


def test_不存在的策略返回None(tmp_path):
    assert ws.load_walkforward_result(tmp_path, "never_ran") is None


def test_损坏文件只跳过不抛(tmp_path):
    """单文件损坏不该中断整个巡检(对齐 CONTRIBUTING 插件隔离要求)。"""
    ws.save_walkforward_result(tmp_path, "s1", _wf_result())
    ws._path(tmp_path, "s1").write_text("{ broken json", encoding="utf-8")
    assert ws.load_walkforward_result(tmp_path, "s1") is None


def test_内容不是dict返回None(tmp_path):
    ws.save_walkforward_result(tmp_path, "s1", _wf_result())
    ws._path(tmp_path, "s1").write_text("[1,2,3]", encoding="utf-8")
    assert ws.load_walkforward_result(tmp_path, "s1") is None


def test_旧版缓存缺folds不崩(tmp_path):
    """老结构可能没有 folds —— 补空列表而不是 KeyError。"""
    p = ws._path(tmp_path, "s1")
    p.write_text(json.dumps({"objective": "sortino", "summary": {}}), encoding="utf-8")
    loaded = ws.load_walkforward_result(tmp_path, "s1")
    assert loaded["folds"] == []


def test_空结果不落盘(tmp_path):
    assert ws.save_walkforward_result(tmp_path, "s1", {}) is None
    assert ws.save_walkforward_result(tmp_path, "s1", None) is None


def test_clear_删除缓存(tmp_path):
    ws.save_walkforward_result(tmp_path, "s1", _wf_result())
    assert ws.clear_result(tmp_path, "s1") is True
    assert ws.load_walkforward_result(tmp_path, "s1") is None
    assert ws.clear_result(tmp_path, "s1") is False


def test_不同策略互不覆盖(tmp_path):
    ws.save_walkforward_result(tmp_path, "s1", _wf_result())
    other = _wf_result()
    other["summary"]["degradation"] = 0.1
    ws.save_walkforward_result(tmp_path, "s2", other)
    assert ws.load_walkforward_result(tmp_path, "s1")["summary"]["degradation"] == 1.0
    assert ws.load_walkforward_result(tmp_path, "s2")["summary"]["degradation"] == 0.1


def test_无新数据时判定弃权而非误判(tmp_path):
    """从未跑过 walkforward 的策略 -> 弃权, 绝不降级。

    这是安全性的关键: 无数据 ≠ 坏策略。
    """
    v = judge(
        strategy_id="fresh",
        walkforward_result=ws.load_walkforward_result(tmp_path, "fresh"),
        current_status="active",
    )
    assert not v.should_degrade
    assert "walkforward_missing" in v.abstained