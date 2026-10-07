"""策略自述验证结论解析的测试。

锚定几件容易做错的事:
1. verdict 是三类互斥值, verified 是**叠加**标记 —— 若把 verified 做成第四类,
   分类计数会与清单自报的统计对不上(实测 9+2+81 vs 自报 5/82/65)。
2. 清单自报 vs 实测不一致时必须暴露 mismatches, 而不是装作正常。
3. 未登记的措辞归 untagged 并留原文, 绝不臆断成通过/失败。
4. 清单文件缺失/为空 → 空结果, 不抛异常(该文件自己也写着"可随时删除本文件")。
"""
from __future__ import annotations

from pathlib import Path

from app.services import strategy_verdicts as sv

MANIFEST = """# 自定义策略清单(自动生成,可随时删除本文件)

共 **152** 个策略文件。

## 一、分类统计

| 自述结论 | 数量 |
| :--- | ---: |
| 回测失败 | 1 |
| 未标注结论 | 3 |
| 通过粗筛 | 1 |

## 二、已验证可用(优先使用)

| 策略 | 证据 |
| :--- | :--- |
| `alpha_one` | 全七关通过: OOS夏普1.4 |
| `beta_two` | 超额 +5.13pp、夏普0.60 |

## 三、全部策略

| id | 显示名 | 来源 | 自述结论 |
| :--- | :--- | :--- | :--- |
| `alpha_one` | 甲 | 本次导入 | 未标注结论 |
| `beta_two` | 乙 | 原有 | 通过粗筛 |
| `gamma_three` | 丙 | 本次导入 | 回测失败 |
| `delta_four` | 丁 | 原有 | 未标注结论 |
| `eps_five` | 戊 | 本次导入 | 某句没登记的结论 |
"""


def _write(data_dir: Path, text: str) -> None:
    p = data_dir / "strategies" / "custom" / "README-策略清单.md"
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(text, encoding="utf-8")


def test_解析两张表并统计(tmp_path: Path):
    _write(tmp_path, MANIFEST)

    s = sv.summarize(tmp_path)

    assert s["meta"]["manifest_count"] == 5
    # untagged 有 3 条: alpha_one(仅在已验证表出现, 类别按缺省) / delta_four / eps_five(未登记措辞)
    assert s["by_verdict"] == {"untagged": 3, "screened": 1, "failed": 1}
    assert s["meta"]["verified_count"] == 2
    assert s["meta"]["consistent"] is True


def test_verified是叠加标记不改verdict(tmp_path: Path):
    """beta_two 在「已验证」表出现, 但类别仍是「通过粗筛」—— 不该被改写成另一种。"""
    _write(tmp_path, MANIFEST)

    items = sv.summarize(tmp_path)["items"]

    assert items["beta_two"]["verdict"] == "screened"
    assert items["beta_two"]["verified"] is True
    # alpha_one 只在「已验证」表出现, 类别按缺省 untagged
    assert items["alpha_one"]["verdict"] == "untagged"
    assert items["alpha_one"]["verified"] is True


def test_证据取自已验证表原文(tmp_path: Path):
    _write(tmp_path, MANIFEST)

    items = sv.summarize(tmp_path)["items"]

    assert items["alpha_one"]["evidence"] == "全七关通过: OOS夏普1.4"
    assert items["gamma_three"]["evidence"] == ""      # 未列入已验证表


def test_未登记措辞不臆断且留原文(tmp_path: Path):
    """"某句没登记的结论" 不该被判成通过或失败, 也不能悄悄丢掉原文。"""
    _write(tmp_path, MANIFEST)

    items = sv.summarize(tmp_path)["items"]

    assert items["eps_five"]["verdict"] == "untagged"
    assert items["eps_five"]["note"] == "某句没登记的结论"
    assert sv.summarize(tmp_path)["meta"]["unknown_wording"] == 1


def test_自报与实测不一致时暴露mismatch(tmp_path: Path):
    """清单被手工改过(统计表说 1 个粗筛, 表里却全是失败) → 必须报不一致。"""
    text = MANIFEST.replace("| 通过粗筛 | 1 |", "| 通过粗筛 | 7 |")
    _write(tmp_path, text)

    m = sv.summarize(tmp_path)["meta"]

    assert m["consistent"] is False
    assert m["mismatches"]["screened"] == {"declared": 7, "parsed": 1}


def test_清单缺失返回空不抛异常(tmp_path: Path):
    """清单文件自己写着可随时删除 —— 删掉后接口必须仍可用。"""
    s = sv.summarize(tmp_path)

    assert s["items"] == {}
    assert s["meta"]["available"] is False
    assert s["meta"]["manifest_count"] == 0
    assert s["by_verdict"] == {}


def test_清单为空文本也安全(tmp_path: Path):
    _write(tmp_path, "")

    assert sv.summarize(tmp_path)["meta"]["available"] is False


def test_孤儿与未标注策略被标出(tmp_path: Path):
    """清单有但引擎没加载(文件被删) / 引擎有但清单没有(内置策略) 都要暴露。"""
    _write(tmp_path, MANIFEST)

    s = sv.summarize(tmp_path, known_ids={"alpha_one", "builtin_only"})

    assert s["orphan_ids"] == ["beta_two", "delta_four", "eps_five", "gamma_three"]
    assert s["orphan_total"] == 4
    assert s["unlabeled_total"] == 1        # builtin_only 不在清单里


def test_表头与分隔行不被当成数据(tmp_path: Path):
    _write(tmp_path, MANIFEST)

    items = sv.summarize(tmp_path)["items"]

    assert "id" not in items and "策略" not in items and "自述结论" not in items


def test_已验证表在后者仍不改verdict(tmp_path: Path):
    """两张表的**顺序不能影响结果**。

    真实清单恰好是「已验证可用」在前、「全部策略」在后, 所以若把 verified 实现成
    "改写 verdict 的第四类", 在真实数据上也会表现为数字正常 —— 缺陷会被掩盖。
    只有把两张表顺序颠倒的用例, 才能真正锚定"verified 不得改写 verdict"。
    """
    reordered = (
        "# 清单\n\n## 一、分类统计\n\n"
        "| 自述结论 | 数量 |\n| :--- | ---: |\n"
        "| 回测失败 | 1 |\n| 未标注结论 | 3 |\n| 通过粗筛 | 1 |\n\n"
        # 注意: 这里是「全部策略」在前
        "## 三、全部策略\n\n"
        "| id | 显示名 | 来源 | 自述结论 |\n| :--- | :--- | :--- | :--- |\n"
        "| `alpha_one` | 甲 | 本次导入 | 未标注结论 |\n"
        "| `beta_two` | 乙 | 原有 | 通过粗筛 |\n"
        "| `gamma_three` | 丙 | 本次导入 | 回测失败 |\n"
        "| `delta_four` | 丁 | 原有 | 未标注结论 |\n"
        "| `eps_five` | 戊 | 本次导入 | 某句没登记的结论 |\n\n"
        # 「已验证可用」挪到了后面
        "## 二、已验证可用(优先使用)\n\n"
        "| 策略 | 证据 |\n| :--- | :--- |\n"
        "| `alpha_one` | 全七关通过: OOS夏普1.4 |\n"
        "| `beta_two` | 超额 +5.13pp、夏普0.60 |\n"
    )
    _write(tmp_path, reordered)

    s = sv.summarize(tmp_path)
    items = s["items"]

    assert items["beta_two"]["verdict"] == "screened"          # 不得被改成 verified
    assert items["beta_two"]["verified"] is True
    assert items["beta_two"]["evidence"] == "超额 +5.13pp、夏普0.60"
    assert items["alpha_one"]["verdict"] == "untagged"
    assert s["by_verdict"] == {"untagged": 3, "screened": 1, "failed": 1}
    assert s["meta"]["consistent"] is True


def test_反引号与空格被清理(tmp_path: Path):
    _write(tmp_path, MANIFEST)

    assert "alpha_one" in sv.summarize(tmp_path)["items"]
