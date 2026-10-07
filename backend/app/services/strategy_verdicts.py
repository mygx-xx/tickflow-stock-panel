"""策略自述验证结论 —— 从策略清单 markdown 解析出「哪个策略验过、证据是什么」。

为什么需要
----------
178 个策略在 UI 上全是 draft / selectable=0, 生命周期状态只表达「是否被人工激活」,
不表达「这个策略到底验过没有」。而 data/strategies/custom/README-策略清单.md 里
已经记录了真实的回测结论 (通过粗筛 / 回测失败 / 未标注) 与证据 (夏普、超额、OOS 一致率),
这些结论只存在于那个 md 文件里, 界面上完全看不到。

为什么解析 md 而不是解析源码 docstring
--------------------------------------
结论确实源自各策略源码头部, 但 152 个文件的措辞有 7 种以上变体
(「单独使用未通过门槛」「未跑赢基准, 不建议部署」…), 靠正则抽语义会不断漏判和误判,
比没有更糟。而清单 md 是已经人工/脚本汇总过的规范化表格, 表格结构稳定, 可靠得多。

取舍与边界
----------
- 这是**自述结论**, 不是本系统跑出来的回测 —— UI 上必须如实标注, 不得冒充已验证。
- 清单文件缺失、被删或格式变化时返回空, 不抛异常、不猜测 (它自己也写着「可随时删除本文件」)。
- 未知结论一律归为 ``untagged``, 绝不臆断成通过或失败。
"""

from __future__ import annotations

import logging
import re
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

# verdict 是三类互斥的结论 (与清单自报的统计一一对应); verified 是叠加的重点关注标记。
VERDICT_LABELS: dict[str, str] = {
    "screened": "通过粗筛",
    "failed": "回测失败",
    "untagged": "未标注结论",
}

# 清单里「全部策略」表用这 3 种自述结论。
_SCREENED = "通过粗筛"
_FAILED = "回测失败"
_UNTAGGED = "未标注结论"


def _read_manifest(data_dir: Path) -> str:
    """读取策略清单文本。文件不存在返回空串(它是可选文件, 不该让接口失败)。"""
    path = data_dir / "strategies" / "custom" / "README-策略清单.md"
    try:
        return path.read_text(encoding="utf-8")
    except FileNotFoundError:
        return ""
    except OSError as e:  # 权限/编码类问题同样降级为「无清单」
        logger.warning("读取策略清单失败 %s: %s", path, e)
        return ""


def _split_row(line: str) -> list[str]:
    """拆 markdown 表格行, 并去掉首尾的 `` ` `` 与空白。"""
    cells = line.strip().strip("|").split("|")
    return [c.strip().strip("`").strip() for c in cells]


def _is_table_row(line: str) -> bool:
    return line.lstrip().startswith("|") and line.rstrip().endswith("|")


def _section_of(line: str, current: str) -> str:
    """跟踪当前所处的二级标题。"""
    s = line.strip()
    return current


def parse_manifest(text: str) -> dict[str, dict[str, Any]]:
    """解析清单文本 -> {strategy_id: {verdict, verified, evidence, note}}

    两张表都要读:
    - 「已验证可用」表: | `id` | 证据 |          -> 叠加 verified=True 与证据原文
    - 「全部策略」表:   | id | 显示名 | 来源 | 结论 |  -> 三类互斥的 verdict

    为什么 verified 不做成第四类 verdict: 它在「全部策略」表里也是按 通过粗筛/回测失败/
    未标注 归类的, 若把 verified 覆盖上去, 分类计数就会与清单自报的统计对不上
    (实测 9+2+81 vs 自报 5/82/65)。做成**叠加标记**, 两组数字就各自自洽:
    verdict 分布恒等于清单自报, verified 则是清单单列的重点关注名单。
    """
    out: dict[str, dict[str, Any]] = {}
    if not text:
        return out

    section = ""
    for raw in text.splitlines():
        if raw.startswith("## "):
            section = raw[3:].strip()
            continue
        if not _is_table_row(raw):
            continue
        # 跳过表头与分隔行
        if raw.lstrip().startswith("| :") or set(raw) <= set("|:- "):
            continue
        cells = _split_row(raw)
        if not cells or not cells[0]:
            continue
        sid = cells[0]
        if sid in ("id", "策略"):  # 表头
            continue

        if "已验证可用" in section and len(cells) >= 2:
            row = out.setdefault(sid, {"verdict": _UNTAGGED, "evidence": "", "note": ""})
            row["verified"] = True
            row["evidence"] = cells[1]
        elif "全部策略" in section and len(cells) >= 4:
            label = cells[-1]
            if label == _SCREENED:
                verdict = "screened"
            elif label == _FAILED:
                verdict = "failed"
            elif label == _UNTAGGED:
                verdict = "untagged"
            else:
                # 出现未登记的措辞时不猜, 归 untagged 并留下原文供人工核对
                verdict = "untagged"
            row = out.setdefault(sid, {"verified": False, "evidence": "", "note": ""})
            row["verdict"] = verdict
            if label not in (VERDICT_LABELS[verdict],):
                row["note"] = label      # 未登记措辞 → 留原文, 供人工核对
            elif not row.get("note"):
                row["note"] = ""

    for row in out.values():
        row.setdefault("verified", False)
        row.setdefault("evidence", "")
        row.setdefault("note", "")
    return out


def _parse_counts(text: str) -> dict[str, int]:
    """从「分类统计」表取各结论数量, 用于自检清单与解析结果是否一致。"""
    counts: dict[str, int] = {}
    in_stats = False
    for raw in text.splitlines():
        if raw.startswith("## "):
            in_stats = "分类统计" in raw
            continue
        if not in_stats or not _is_table_row(raw):
            continue
        if raw.lstrip().startswith("| :"):
            continue
        cells = _split_row(raw)
        if len(cells) < 2 or cells[0] in ("自述结论",):
            continue
        try:
            counts[cells[0]] = int(cells[1])
        except ValueError:
            continue
    return counts


def load_verdicts(data_dir: Path) -> dict[str, dict[str, Any]]:
    """读清单并返回 {strategy_id: {...}}; 附带 ``__meta__`` 说明。

    ``__meta__`` 里的 manifest_count / declared_counts / parsed_by_verdict 用来暴露
    「清单自报的统计」与「实际解析到的分布」是否一致 —— 不一致说明清单已过期或被手工改过,
    界面应提示而不是装作正常。
    """
    text = _read_manifest(data_dir)
    parsed = parse_manifest(text)
    counts = _parse_counts(text)

    by_verdict: dict[str, int] = {}
    verified = 0
    for row in parsed.values():
        by_verdict[row["verdict"]] = by_verdict.get(row["verdict"], 0) + 1
        if row.get("verified"):
            verified += 1

    # 自报 vs 实测: 三类结论逐一比对 (verified 是叠加项, 不参与比对)
    declared_verdict = {
        "screened": counts.get(_SCREENED, 0),
        "failed": counts.get(_FAILED, 0),
        "untagged": counts.get(_UNTAGGED, 0),
    }
    mismatches = {
        k: {"declared": declared_verdict[k], "parsed": by_verdict.get(k, 0)}
        for k in declared_verdict
        if declared_verdict[k] != by_verdict.get(k, 0)
    }
    unknown_notes = sum(1 for r in parsed.values() if r["note"])

    parsed["__meta__"] = {
        "available": bool(text),
        "manifest_count": len(parsed),
        "declared_counts": counts,
        "declared_total": sum(counts.values()),
        "parsed_by_verdict": by_verdict,
        "verified_count": verified,
        "mismatches": mismatches,
        "consistent": not mismatches and (not counts or sum(declared_verdict.values()) == len(parsed)),
        "unknown_wording": unknown_notes,
    }
    return parsed


def summarize(data_dir: Path, known_ids: set[str] | None = None) -> dict[str, Any]:
    """给接口用的聚合结果。

    known_ids 传入引擎已加载的策略 id 时, 会标出「清单里有但引擎没加载」的孤儿 ——
    通常意味着策略文件被删了或加载失败, 值得暴露。
    """
    data = load_verdicts(data_dir)
    meta = data.pop("__meta__", {})
    orphan = sorted(set(data) - known_ids) if known_ids is not None else []
    unlabeled = sorted(known_ids - set(data)) if known_ids is not None else []
    return {
        "source": "data/strategies/custom/README-策略清单.md",
        "verdict_labels": VERDICT_LABELS,
        "by_verdict": meta.get("parsed_by_verdict", {}),
        "meta": meta,
        "orphan_ids": orphan[:50],
        "orphan_total": len(orphan),
        "unlabeled_total": len(unlabeled),
        "items": data,
    }
