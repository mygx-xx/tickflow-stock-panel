"""子进程文本模式必须显式钉 encoding —— 防 Windows 本地编码 (gbk) 解码崩溃。

判据: 全仓 ``subprocess.run/Popen/call/check_call/check_output`` 只要出现 ``text=True``
(或 ``universal_newlines=True``), 就必须同时给出 ``encoding=``。否则 Windows 上按 cp936
解码 git / uv / npm 的 UTF-8 输出会抛 UnicodeDecodeError, ``stdout`` 变 None 使调用方崩溃。
先例: scripts/upgrade_check.py 的同类崩溃 (99c60f9), 本用例把该口径固化为门禁。
"""

from __future__ import annotations

import ast
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
SCAN_ROOTS = ("backend/app", "backend/tests", "scripts", "mcp-server")
SUBPROCESS_FUNCS = {"run", "Popen", "call", "check_call", "check_output"}


def _is_subprocess_call(call: ast.Call) -> bool:
    func = call.func
    if not isinstance(func, ast.Attribute) or func.attr not in SUBPROCESS_FUNCS:
        return False
    return isinstance(func.value, ast.Name) and func.value.id == "subprocess"


def _kwarg_names(call: ast.Call) -> set[str | None]:
    return {kw.arg for kw in call.keywords}


def _violations(root: Path) -> list[str]:
    bad: list[str] = []
    for path in sorted(root.rglob("*.py")):
        if "__pycache__" in path.parts or ".venv" in path.parts:
            continue
        try:
            tree = ast.parse(path.read_text(encoding="utf-8"))
        except (OSError, SyntaxError, UnicodeDecodeError):
            continue
        for node in ast.walk(tree):
            if not isinstance(node, ast.Call) or not _is_subprocess_call(node):
                continue
            args = _kwarg_names(node)
            if not ("text" in args or "universal_newlines" in args):
                continue
            if "encoding" in args:
                continue
            rel = path.relative_to(REPO_ROOT).as_posix()
            bad.append(f"{rel}:{node.lineno}")
    return bad


def test_text_mode_subprocess_must_pin_encoding():
    """文本模式子进程必须显式指定 encoding, 否则 Windows 上按 gbk 解码。

    (函数名用 ASCII: 中文测试名会触发 N802, 新增文件不留新告警。)
    """
    missing: list[str] = []
    for rel in SCAN_ROOTS:
        root = REPO_ROOT / rel
        if root.is_dir():
            missing.extend(_violations(root))
    assert not missing, (
        "subprocess 文本模式缺 encoding=, Windows 下会按本地编码 (gbk) 解码子进程输出: "
        + ", ".join(missing)
    )
