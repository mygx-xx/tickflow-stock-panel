"""策略生命周期状态机 — 对标 factors/store.py 的 STATUSES 四态。

方向二: 策略加状态字段 + 绩效自动判定。生命周期 draft → active → watch →
retired, 与因子侧 `app.factors.store.STATUSES` 同构(直接复用该常量, 避免两套
四态各自漂移)。

## 与因子侧的关键差异(务必先读)

因子是**JSON 存盘**(`data/user_data/custom_factors/*.json`), 改status 就是改
一个 JSON 字段; **策略是 `.py` 文件存盘**(`data/strategies/{custom|ai|composite}/
{id}.py`), 生命周期字段住在模块级 `META` 字典里。因此:

- 本模块**只负责状态语义**(合法迁移 / 默认值 / 可写性判定), **不碰文件 I/O**;
  真正的文本改写 + 引擎重载 + 失败回滚在 `app/api/strategy.py`
  (`set_strategy_status`), 沿用 `publish_ai_strategy` 的四段式安全范式。
- 保持"语义与 I/O 分离"是因为策略文件改写有真实失败风险(改坏META 语法会让
  该策略整个加载失败), 回滚逻辑必须留在有 engine 上下文的地方。

## 与既有 research_only 的关系

策略侧原有的 `research_only`(布尔, 人工标记AI 草稿)与本状态机的 `draft`
语义重叠。二者**不合并**, 职责切分如下:

- `research_only`: 「是否对外公开」—— 只由 publish 端点的**人的显式动作**驱动,
  控制 API 层可见性(`_get_public_strategy` 对草稿返回 404,列表/run-all 默认跳过)。
- `status`: 「绩效可信度」—— 由 walkforward degradation / deflated_sharpe_psr
  **自动**判定与迁移, 控制是否参与自动选股/推荐。

即: research_only 管**可见性**, status 管**可信度**。一个已publish 但绩效
衰退的策略是 `research_only=False, status="watch"` —— 对外可见, 但不进自动
选股池; 一个 `status="draft"` 的新策略仍按 research_only 决定是否出现在列表里。

## 迁移规则(单向衰减, 人工可回升)

合法边(完整清单, 与 `TRANSITIONS` 一一对应):

    draft   → active    人工激活 / 首次检验通过
    draft   → watch     跳过激活直接标记观察(人工)
    draft   → retired   直接归档(人工)
    active  → watch     绩效衰减(**唯一允许自动触发的边**)
    active  → draft     人工撤回激活(2026-10-07 新增, 缘由见下)
    active  → retired   人工归档
    watch   → active    人工复核通过(须重跑检验)
    watch   → retired   人工归档
    retired → draft     人工复活(retired 的唯一出路)

- **自动降级只允许 active → watch**。这是本模块的核心约束: 绩效判定可以
  「保守」但不能「激进」—— 永远不自动晋升(draft→active / watch→active),
  也不自动retired。前者防止用不可靠的单次walkforward 结果把半成品策略
  放出去, 后者防止因指标抖动把好策略永久枪毙(无申诉通道)。
- `retired` 是终态, 只能人工回到 draft。
- watch → active 的回升(人工)必须重新走检验, 故实现上视为 draft→active 的
  同一条路径。

### 为什么允许 active → draft(2026-10-07 修订)

原设计不允许这条边, 由 `test_active_不能直接回draft` 锁住, 理由是"不能一步
回退抹掉衰减历史"。实战用下来该理由不成立、代价却真实:

- **历史本来就没存**。status 是单字段, 不记流转日志; 走 `active→watch→
  retired→draft` 三步同样不留痕(第一步就把 active 抹了)。想留痕得靠审计日志,
  不是靠迁移图卡着。
- **真实场景是误操作**。用户把还没准备好的策略点成 active 想撤回, 却被逼先
  "降级"再"归档"再"复活" —— 三个语义都不对, 用户只会以为程序坏了。

故放开这条边, 但**仍只走人工路径**(不在 `AUTOMATIC_TRANSITIONS` 里) ——
自动判定最多降到 watch 这条底线不变。
"""
from __future__ import annotations

import logging

from app.factors.store import STATUSES

logger = logging.getLogger(__name__)

__all__ = [
    "STATUSES",
    "DEFAULT_STATUS",
    "TRANSITIONS",
    "AUTOMATIC_TRANSITIONS",
    "normalize_status",
    "can_transition",
    "is_transition_automatic",
    "is_selectable",
    "is_visible",
    "describe_status",
]

#: 未显式声明 status 的策略一律视为 draft(新建/历史策略的保守默认,
#: 不能因为"没写这个字段"就当成 active 混进自动选股池)。
DEFAULT_STATUS = "draft"

#: 合法迁移图。key=当前状态, value=允许迁移到的目标状态集合。
#: 注意 draft→watch / draft→retired 允许 —— 人工可以跳过激活直接归档;
#: active→draft 也允许 —— 人工撤回误激活(详见模块 docstring 的修订说明)。
#: 但**自动判定绝不走这些边**(见 AUTOMATIC_TRANSITIONS)。
TRANSITIONS: dict[str, frozenset[str]] = {
    "draft": frozenset({"active", "watch", "retired"}),
    "active": frozenset({"draft", "watch", "retired"}),
    "watch": frozenset({"active", "retired"}),
    "retired": frozenset({"draft"}),
}

#: 自动判定唯一允许的迁移。绩效衰退 → 降级为 watch, 其余一律拒绝。
#: 刻意保守: 晋升必须由人显式确认(walkforward 单次结果不足以证明策略有效),
#: 永久淘汰必须走人工复核(避免指标抖动导致误杀且无法申诉)。
AUTOMATIC_TRANSITIONS: frozenset[tuple[str, str]] = frozenset({("active", "watch")})


def normalize_status(raw: object) -> str:
    """把 META 里的原始值规整成合法状态。

    非法值**不静默回退**为 draft 而**回退 draft + 告警**: 与
    `app.services.preferences` 的白名单回退不同, 生命周期状态回退会让一个
    正在运行的 active 策略悄悄变成"未激活", 属于状态丢失, 必须可见。
    判定入口只允许 None(未声明, 合法的默认)触发静默默认。
    """
    if raw is None:
        return DEFAULT_STATUS
    text = str(raw).strip().lower()
    if not text:
        return DEFAULT_STATUS
    if text not in STATUSES:
        logger.warning(
            "非法策略 status %r, 回退 %s; 合法值=%s",
            text, DEFAULT_STATUS, sorted(STATUSES),
        )
        return DEFAULT_STATUS
    return text


def can_transition(current: str, target: str) -> bool:
    """该迁移是否在合法迁移图内(不考虑是否自动)。"""
    return target in TRANSITIONS.get(normalize_status(current), frozenset())


def is_transition_automatic(current: str, target: str) -> bool:
    """该迁移是否可由自动判定触发。

    只有 active → watch 为 True。API 端点调用时应据此区分「绩效降级」与
    「人工操作」两条路径 —— 后者要留操作者语义, 前者要记审计来源。
    """
    return (normalize_status(current), normalize_status(target)) in AUTOMATIC_TRANSITIONS


def is_selectable(status: object) -> bool:
    """是否应参与自动选股 / 推荐池。

    只有 active 进池: draft 未经检验, watch 已衰减待复核, retired 已归档。
    这是 status 与 research_only 的核心区别 —— **watch 仍然对外可见**
    (人工可查、可手动跑), 只是不进自动池。
    """
    return normalize_status(status) == "active"


def is_visible(status: object) -> bool:
    """是否应出现在默认列表中(与 is_selectable 区分)。

    retired 不在默认列表; 其余状态(含 watch/draft)均可见, 便于人工复核。
    真正的对外可见性由 research_only 控制, 这里只管状态维度的展示。
    """
    return normalize_status(status) != "retired"


_DESCRIPTIONS: dict[str, str] = {
    "draft": "草稿: 未经检验, 不参与自动选股",
    "active": "已激活: 检验通过, 参与自动选股",
    "watch": "观察中: 绩效衰退待复核, 仍可见但不进自动选股池",
    "retired": "已归档: 退出全部自动流程, 仅人工可复活",
}


def describe_status(status: object) -> str:
    """状态的中文说明(供 API/前端徽标与日志复用)。"""
    return _DESCRIPTIONS.get(normalize_status(status), "未知状态")