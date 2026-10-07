#!/usr/bin/env python
"""策略迭代协议辅助 —— 冷启动/每轮收尾的机械化部分。

对应 docs/strategy-iteration.md:
  §2  建 .iterations/<策略>/ 目录 + LEDGER.md 骨架 + v1 快照
  §4  封存留出集(最后 20%), 并给出「迭代区间 / 留出区间」两段边界
  §5  版本快照 / 回退

设计约束(与协议一致):
  - 留出集**只计算不消费**。本脚本不会跑留出区间回测, 也不会自动判定。
    留出集必须由人在终审时手动指定 --end, 这是纪律的最后一道人工闸。
  - 所有写操作都在 data/strategies/custom/.iterations/ 下, 不碰引擎扫描的顶层。
    引擎 `engine.py:267` 只 glob 顶层 *.py, 所以 .iterations/ 天然不会被当成策略加载。

用法:
  # 冷启动: 建目录 + 快照 v1 + 打印留出边界
  python scripts/iter_workbench.py init <strategy_id> --start 2021-01-04

  # 跑一轮取证(自动带 --stage-all, 自动收JSON)
  python scripts/iter_workbench.py run <strategy_id> --r R1 --phase 粗调 \
      --profile lenient --max-positions 100 [--regime-states strong weak]

  # 从上一轮 JSON 生成台账节草稿(粘贴到 LEDGER.md)
  python scripts/iter_workbench.py draft <strategy_id> --from-json <report.json>

  # 每轮接受后: 快照当前版本
  python scripts/iter_workbench.py snapshot <strategy_id> --note "接受 R2"

  # 看状态: 留出边界 / 已有快照 / 台账轮次数
  python scripts/iter_workbench.py status <strategy_id>

  # 回退: 把 vN.py 复制回当前文件(需 --yes 二次确认)
  python scripts/iter_workbench.py restore <strategy_id> <version> --yes
"""

from __future__ import annotations

import argparse
import json
import re
import shutil
import subprocess
import sys
from datetime import date, timedelta
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
CUSTOM_DIR = REPO_ROOT / "data" / "strategies" / "custom"
ITER_ROOT = CUSTOM_DIR / ".iterations"

# 协议 §4: 迭代开始即封存最后 20% 区间
HOLDOUT_RATIO = 0.20

VERSION_RE = re.compile(r"^v(\d+)\.py$")


def iter_dir(strategy_id: str) -> Path:
    return ITER_ROOT / strategy_id


def find_current(strategy_id: str) -> Path:
    """定位策略当前生效文件。优先精确匹配, 否则按META.id 找唯一候选。"""
    exact = CUSTOM_DIR / f"{strategy_id}.py"
    if exact.is_file():
        return exact
    hits = []
    for f in sorted(CUSTOM_DIR.glob("*.py")):
        if f.name.startswith("_"):
            continue
        try:
            if f'"{strategy_id}"' in f.read_text(encoding="utf-8")[:2000]:
                hits.append(f)
        except Exception:  # noqa: BLE001
            continue
    if len(hits) == 1:
        return hits[0]
    if not hits:
        raise SystemExit(f"[error] 在 {CUSTOM_DIR} 找不到策略 {strategy_id}")
    raise SystemExit(
        f"[error] {strategy_id} 匹配到多个文件, 请改用文件名: "
        + ", ".join(f.name for f in hits)
    )


def compute_holdout(start: date, end: date) -> tuple[date, date, date]:
    """返回 (holdout_start, iter_start, iter_end)。

    协议 §4: 最后 20% 封存。切点用自然日线性切分(与文档示例口径一致),
    再退到最近一个交易日, 避免切在非交易日产生空洞。
    """
    span = (end - start).days
    if span <= 0:
        raise SystemExit(f"[error] 区间非法: {start} ~ {end}")
    holdout_start = end - timedelta(days=int(span * HOLDOUT_RATIO))
    return holdout_start, start, holdout_start - timedelta(days=1)


def read_holdout(strategy_id: str) -> dict | None:
    f = iter_dir(strategy_id) / "holdout.json"
    if not f.is_file():
        return None
    try:
        return json.loads(f.read_text(encoding="utf-8"))
    except Exception:  # noqa: BLE001
        return None


def write_holdout(strategy_id: str, holdout_start: date, holdout_end: date,
                  iter_start: date, iter_end: date) -> None:
    """把留出集边界持久化。

    协议 §4 要求「迭代开始即封存最后 20%」, 但系统层面没有任何 holdout 概念
    (全仓库 grep holdout 只有 walk-forward 的折外指标, 语义不同)。
    → 边界必须由本文件记录下来, 否则每轮手打 --start/--end 极易误跑留出集。
    本文件只**计算与记录**, 绝不消费留出集: 不提供「跑留出集」的封装。
    """
    payload = {
        "holdout_start": holdout_start.isoformat(),
        "holdout_end": holdout_end.isoformat(),
        "iteration_start": iter_start.isoformat(),
        "iteration_end": iter_end.isoformat(),
        "ratio": HOLDOUT_RATIO,
        "note": "封存区。任何迭代轮次都不得使用; 定型版本只在此区间跑一次终审。",
    }
    (iter_dir(strategy_id) / "holdout.json").write_text(
        json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8"
    )


def validate_tool() -> Path | None:
    """定位 validate.py。它在 gitignore 的 scripts/strategy-tools/ 里, 可能缺失。"""
    for c in (REPO_ROOT / "scripts" / "strategy-tools" / "validate.py",
              Path(__file__).resolve().parent / "strategy-tools" / "validate.py"):
        if c.is_file():
            return c
    return None


def stage_all_supported(tool: Path) -> bool:
    try:
        out = subprocess.run(
            [sys.executable, str(tool), "--help"],
            capture_output=True, text=True, timeout=120,
        ).stdout
    except Exception:  # noqa: BLE001
        return False
    return "--stage-all" in out


def list_versions(strategy_id: str) -> list[tuple[int, Path]]:
    d = iter_dir(strategy_id)
    if not d.is_dir():
        return []
    out = []
    for f in d.glob("v*.py"):
        m = VERSION_RE.match(f.name)
        if m:
            out.append((int(m.group(1)), f))
    return sorted(out)


def next_version(strategy_id: str) -> int:
    vs = list_versions(strategy_id)
    return (vs[-1][0] + 1) if vs else 1


LEDGER_TEMPLATE = """# {sid} 迭代台账

<!-- 本文件由 scripts/iter_workbench.py 初始化后手工维护。 -->
<!-- 格式约定见 docs/strategy-iteration.md §2「每轮追加一节」。 -->

- 思路: {idea}
- 迭代区间: {iter_start} ~ {iter_end}
- **留出集(封存): {holdout_start} ~ {holdout_end}** —— 任何一轮都不许用, 定型版本只在上面跑一次
- 判定门槛(先于结果写下, 数字示例, 需按思路改):
  - 全区间夏普 >= {base_sharpe} + 0.2
  - 分年最差 > 0
  - 样本外降幅 < 10%
  - 命中数变化 < 50%
- 回退线: 样本外夏普降幅 > 20% 或 胜率 < 45% 或 敏感性陡变(±20% 扰动指标波动 > 30%)
- 基线(v1): 夏普 __ / 回撤 __ / 胜率 __ / 盈亏比 __ / 命中 __
- 待试清单:
  1. 
  2. 
  3. 

<!-- ============ 以下为逐轮记录, 追加勿改 ============ -->
"""


def cmd_init(args: argparse.Namespace) -> None:
    start = date.fromisoformat(args.start)
    end = date.fromisoformat(args.end) if args.end else date.today()
    holdout_start, iter_start, iter_end = compute_holdout(start, end)

    src = find_current(args.strategy_id)
    d = iter_dir(args.strategy_id)
    d.mkdir(parents=True, exist_ok=True)
    (d / "evidence").mkdir(exist_ok=True)

    # init 必须幂等: 已初始化过就只刷新留出边界, 绝不动快照链与台账。
    # (否则重复 init 会把 v1 悄悄复制成 v2, 版本链就废了)
    already = (d / "LEDGER.md").is_file() or bool(list_versions(args.strategy_id))
    v = next_version(args.strategy_id)
    if not already:
        shutil.copy2(src, d / f"v{v}.py")

    ledger = d / "LEDGER.md"
    if ledger.exists():
        print(f"[warn] LEDGER.md 已存在, 未覆盖: {ledger}")
    elif already:
        print(f"[warn] 快照已存在但台账缺失, 请手工补: {ledger}")
    else:
        ledger.write_text(
            LEDGER_TEMPLATE.format(
                sid=args.strategy_id,
                idea=args.idea or "(待填: 一句话说清思路)",
                iter_start=iter_start,
                iter_end=iter_end,
                holdout_start=holdout_start,
                holdout_end=end,
                base_sharpe="__",
            ),
            encoding="utf-8",
        )

    write_holdout(args.strategy_id, holdout_start, end, iter_start, iter_end)

    print(f"[ok] 工作目录 {d}")
    if not already:
        print(f"[ok] v{v} 快照 <- {src.name}")
    else:
        print(f"[ok] 已初始化过, 快照链未动(现有 {len(list_versions(args.strategy_id))} 个)")
    print(f"[ok] 台账 {ledger}")
    print(f"[ok] 留出边界已记录 -> {(d / 'holdout.json')}")
    print()
    print("  迭代区间(每轮都用这个):  --start {s} --end {e}".format(s=iter_start, e=iter_end))
    print("  留出集(封存, 平时不许跑):  --start {s} --end {e}".format(s=holdout_start, e=end))
    print()
    print("  下一步: 跑基线回测并把数字填进台账首页")
    print("    ./backend/.venv/Scripts/python.exe scripts/strategy-tools/validate.py \\")
    print(f"      --ids {args.strategy_id} --profile official \\")
    print(f"      --start {iter_start} --end {iter_end} \\")
    print(f"      --json /tmp/{args.strategy_id}_v{v}.json")


def cmd_snapshot(args: argparse.Namespace) -> None:
    src = find_current(args.strategy_id)
    d = iter_dir(args.strategy_id)
    d.mkdir(parents=True, exist_ok=True)
    v = next_version(args.strategy_id)
    shutil.copy2(src, d / f"v{v}.py")
    print(f"[ok] v{v} <- {src.name}")
    if args.note:
        print(f"[note] {args.note}")
    print(f"[remind] 别忘了在 LEDGER.md 追加本轮: 假设 / 改动 / 证据 / 判定 / 负知识 / 下一步")


def cmd_run(args: argparse.Namespace) -> None:
    """跑一轮取证。

    两个关键行为, 都是为了封住手动跑 CLI 时最容易犯的错:
      1. **强制用迭代区间**: 从 holdout.json 读边界, 不接受任意 --start/--end,
         所以本命令在结构上**不可能**误跑留出集(终审请手工跑 validate.py)。
      2. **自动 --stage-all**: 前置关失败时也把分年/敏感性/样本外跑出来。
         没有它, 策略最需要诊断的时候反而没有诊断数据(validate.py 早停逻辑)。
         --stage-all 只放宽「是否执行」, 判定仍按 required 阻断。
    """
    tool = validate_tool()
    if tool is None:
        raise SystemExit(
            "[error] 找不到 validate.py。它在 gitignore 的 scripts/strategy-tools/ 里, "
            "新 worktree 需先从主树复制(只复制 .py 与 .md, 别拷 600M 产物)。"
        )
    ho = read_holdout(args.strategy_id)
    if ho is None:
        raise SystemExit(
            f"[error] 未初始化(无 holdout.json)。先跑: iter_workbench.py init "
            f"{args.strategy_id} --start <日期>"
        )
    if args.full:  # 终审: 允许显式跑留出集, 但必须 --full 明示
        start, end = ho["holdout_start"], ho["holdout_end"]
        print(f"[warn] 终审模式: 使用留出集 {start} ~ {end} —— 这一步只能用一次")
    else:
        start, end = ho["iteration_start"], ho["iteration_end"]
        print(f"迭代区间: {start} ~ {end}(留出集 {ho['holdout_start']} ~ {ho['holdout_end']} 已封存)")

    d = iter_dir(args.strategy_id)
    d.mkdir(parents=True, exist_ok=True)
    (d / "evidence").mkdir(exist_ok=True)
    tag = args.r or "adhoc"
    out_json = d / "evidence" / f"{tag}.json"

    cmd = [
        sys.executable, str(tool),
        "--ids", args.strategy_id,
        "--profile", args.profile,
        "--start", start, "--end", end,
        "--max-positions", str(args.max_positions),
        "--json", str(out_json),
    ]
    if args.holding_days:
        cmd += ["--holding-days", str(args.holding_days)]
    if args.regime_states:
        cmd += ["--regime-states", *args.regime_states]
    if args.require_oos:
        cmd.append("--require-oos")
    if not args.no_stage_all:
        if not stage_all_supported(tool):
            print("[warn] 该 validate.py 不支持 --stage-all, 分年/敏感性/样本外可能被跳过"
                  "(前置关失败时)。建议从主树同步最新版。")
        else:
            cmd.append("--stage-all")

    print()
    print(" ".join(cmd))
    print()
    print("[remind] 运行配置(逐轮必须一致, 否则指标不可比):")
    print(f"        profile={args.profile} max_positions={args.max_positions} "
          f"holding_days={args.holding_days or '(未传)'} "
          f"regime_states={args.regime_states or '(无)'}")
    print()
    rc = subprocess.call(cmd)
    print()
    if out_json.is_file():
        print(f"[ok] 报告已存 {out_json}")
        print(f"[next] 生成台账节: python scripts/iter_workbench.py draft "
              f"{args.strategy_id} --from-json {out_json}")
    else:
        print("[warn] 未产出 JSON, 检查上面的报错")
    raise SystemExit(rc)


def _fmt_pct(x) -> str:
    return "n/a" if x is None else f"{x * 100:.2f}%"


def _fmt(x, nd: int = 2) -> str:
    return "n/a" if x is None else f"{x:.{nd}f}"


def _year_cell(y: dict) -> str:
    """分年单元格: 2021 +2.56%(夏普0.24)"""
    return "%s %s(夏普%s)" % (
        y.get("year"), _fmt_pct(y.get("total_return")), _fmt(y.get("sharpe")),
    )


def cmd_draft(args: argparse.Namespace) -> None:
    """从 validate.py 的 JSON 报告生成台账节草稿(协议 §3 的「证据包」)。

    痛点: 手抄 metrics 极易抄错, 且会漏字段。本命令保证字段齐全 + 可粘贴。
    """
    src = Path(args.from_json)
    if not src.is_file():
        raise SystemExit(f"[error] 报告不存在: {src}")
    data = json.loads(src.read_text(encoding="utf-8"))
    reports = data if isinstance(data, list) else [data]
    report = next((r for r in reports if r.get("strategy_id") == args.strategy_id), None)
    if report is None:
        raise SystemExit(f"[error] 报告里没有策略 {args.strategy_id}")

    stages = {s["name"]: s for s in report.get("stages", [])}
    m2 = stages.get("2.交易规则", {}).get("metrics", {})
    m3 = stages.get("3.基准对照", {}).get("metrics", {})
    m4 = stages.get("4.风险指标", {}).get("metrics", {})
    m5 = stages.get("5.分年稳定", {}).get("metrics", {})
    m6 = stages.get("6.参数敏感", {})
    m7 = stages.get("7.样本外", {})

    tag = args.r or "adhoc"
    head = f"## {tag} · {date.today().isoformat()}({args.phase})"
    lines = [head, ""]
    lines.append(f"- 假设: {args.hypothesis or '(待填)'}".replace("()", ""))
    lines.append(f"- 改动: {args.change or '(待填)'}".replace("()", ""))
    lines.append(f"- 预期: {args.expect or '(待填)'}".replace("()", ""))
    lines.append("- 证据(由 iter_workbench.py draft 自动生成, 勿手抄):")
    lines.append("")
    lines.append("  | 指标 | 值 |")
    lines.append("  | :-- | --: |")
    lines.append(f"  | 全区间夏普 | {_fmt(m4.get('sharpe'))} |")
    lines.append(f"  | 索提诺 | {_fmt(m4.get('sortino'))} |")
    lines.append(f"  | 卡玛 | {_fmt(m4.get('calmar'))} |")
    lines.append(f"  | 最大回撤 | {_fmt_pct(m4.get('max_drawdown'))} |")
    lines.append(f"  | 胜率 | {_fmt_pct(m4.get('win_rate'))} |")
    lines.append(f"  | 盈亏比 | {_fmt(m4.get('profit_factor'))} |")
    lines.append(f"  | 蒙卡95%回撤 | {_fmt_pct(m4.get('mc_maxdd_p95'))} |")
    lines.append(f"  | 基准收益 | {_fmt_pct(m3.get('benchmark_return'))} |")
    lines.append(f"  | 超额收益 | {_fmt_pct(m3.get('excess'))} |")
    lines.append(f"  | 命中数 | {m2.get('n_trades')} |")
    lines.append(f"  | 成交受阻 | {m2.get('blocked_orders')} ({_fmt_pct(m2.get('blocked_orders_ratio'))}) |")
    lines.append(f"  | 均仓 | {_fmt_pct(m2.get('avg_exposure'))} |")

    years = m5.get("years") or []
    if years:
        lines.append(f"  | 分年 | {' / '.join(_year_cell(y) for y in years)} |")
    if m6.get("detail"):
        lines.append(f"  | 参数敏感 | {m6['detail']} |")
    if m7.get("detail"):
        lines.append(f"  | 样本外 | {m7['detail']} |")

    lines.append("")
    verdict = {"accepted": "✅ 通过", "rejected": "❌ 拒绝", "error": "⚠️ 错误"}.get(
        report.get("verdict", ""), report.get("verdict", "?")
    )
    lines.append(f"- 判定: {verdict}(耗时 {report.get('elapsed_s', 0):.1f}s)")
    for st in report.get("stages", []):
        if st.get("passed") is False:
            reasons = "; ".join(st.get("reasons") or [])
            lines.append(f"  - {st['name']}: {reasons or '未通过'}")
    lines.append(f"- 负知识: (待填 —— 什么没用、为什么。这是台账里最值钱的部分)")
    lines.append(f"- 下一步: (待填)")
    lines.append("")
    lines.append(f"<!-- 来源: {src.name} | profile={report.get('profile')} -->")

    out = "\n".join(lines)
    print(out)
    d = iter_dir(args.strategy_id)
    ledger = d / "LEDGER.md"
    if args.append and ledger.is_file():
        with ledger.open("a", encoding="utf-8") as fh:
            fh.write("\n\n" + out + "\n")
        print(f"\n[ok] 已追加到 {ledger}")
    else:
        print("\n[hint] 加 --append 直接追加到 LEDGER.md")


def cmd_status(args: argparse.Namespace) -> None:
    sid = args.strategy_id
    d = iter_dir(sid)
    if not d.is_dir():
        print(f"未初始化。跑: iter_workbench.py init {sid} --start <日期>")
        return
    vs = list_versions(sid)
    ledger = d / "LEDGER.md"
    rounds = 0
    if ledger.is_file():
        txt = ledger.read_text(encoding="utf-8")
        rounds = len(re.findall(r"^## R\d+", txt, re.M))
    print(f"策略     {sid}")
    print(f"快照     {len(vs)} 个" + (f" (最新 v{vs[-1][0]})" if vs else ""))
    for n, f in vs:
        print(f"         v{n:<3} {f.stat().st_size:>7}B  {f.name}")
    print(f"台账轮次 {rounds}")
    ho = read_holdout(sid)
    if ho:
        print(f"迭代区间 {ho['iteration_start']} ~ {ho['iteration_end']}")
        print(f"留出集   {ho['holdout_start']} ~ {ho['holdout_end']}  ★ 封存, 平时不许跑")
        last = (d / "evidence")
        runs = sorted(last.glob("*.json")) if last.is_dir() else []
        if runs:
            print(f"证据包   {len(runs)} 份-> " + ", ".join(p.stem for p in runs))
    else:
        print("留出集   未记录(该策略未 init)")


def cmd_restore(args: argparse.Namespace) -> None:
    if not args.yes:
        raise SystemExit("[error] 回退会覆盖当前策略文件。确认无误后加 --yes 重跑。")
    d = iter_dir(args.strategy_id)
    snap = d / f"v{args.version}.py"
    if not snap.is_file():
        raise SystemExit(f"[error] 快照不存在: {snap}")
    dst = find_current(args.strategy_id)
    shutil.copy2(snap, dst)
    print(f"[ok] 已回退: {dst.name} <- v{args.version}.py")
    print(f"[remind] 在 LEDGER.md 追加回退原因(协议 §5 要求)")


def main() -> None:
    ap = argparse.ArgumentParser(description="策略迭代协议辅助(留出集计算/快照/回退)")
    sub = ap.add_subparsers(dest="cmd", required=True)

    p = sub.add_parser("init", help="冷启动: 建目录 + v1 快照 + 打印留出边界")
    p.add_argument("strategy_id")
    p.add_argument("--start", required=True, help="研究起点 YYYY-MM-DD")
    p.add_argument("--end", default=None, help="数据终点, 默认今天")
    p.add_argument("--idea", default=None)
    p.set_defaults(func=cmd_init)

    p = sub.add_parser("snapshot", help="接受一轮后: 快照当前版本")
    p.add_argument("strategy_id")
    p.add_argument("--note", default="")
    p.set_defaults(func=cmd_snapshot)

    p = sub.add_parser("run", help="跑一轮取证(自动留出集边界 + 自动 --stage-all)")
    p.add_argument("strategy_id")
    p.add_argument("--r", default="adhoc", help="轮次标签, 如 R2")
    p.add_argument("--phase", default="粗调", help="粗调/ 精调")
    p.add_argument("--profile", default="lenient", choices=["lenient", "official", "strict"])
    p.add_argument("--max-positions", type=int, default=100)
    p.add_argument("--holding-days", type=int, default=None)
    p.add_argument("--regime-states", nargs="*", default=None)
    p.add_argument("--require-oos", action="store_true")
    p.add_argument("--no-stage-all", action="store_true", help="不追加 --stage-all")
    p.add_argument("--full", action="store_true",
                   help="终审模式: 跑留出集(只能用一次, 会明确警告)")
    p.set_defaults(func=cmd_run)

    p = sub.add_parser("draft", help="从报告 JSON 生成台账节(证据包)")
    p.add_argument("strategy_id")
    p.add_argument("--from-json", required=True)
    p.add_argument("--r", default=None, help="轮次标签")
    p.add_argument("--phase", default="粗调")
    p.add_argument("--hypothesis", default=None)
    p.add_argument("--change", default=None)
    p.add_argument("--expect", default=None)
    p.add_argument("--append", action="store_true", help="追加到 LEDGER.md")
    p.set_defaults(func=cmd_draft)

    p = sub.add_parser("status", help="看留出边界/快照/轮次")
    p.add_argument("strategy_id")
    p.set_defaults(func=cmd_status)

    p = sub.add_parser("restore", help="回退: vN 覆盖当前文件")
    p.add_argument("strategy_id")
    p.add_argument("version", type=int)
    p.add_argument("--yes", action="store_true")
    p.set_defaults(func=cmd_restore)

    args = ap.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()