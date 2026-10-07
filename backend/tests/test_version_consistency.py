"""版本号一致性守卫 — VERSION / app.__version__ / pyproject / package.json 必须同源。

背景: `GET /api/data/version` 的取值顺序是
`app.__version__` (打包期注入, 唯一权威) → 项目根 `VERSION` 文件 (兜底) → v0.0.0。
前端侧栏读这个接口展示版本号。

历史上 `VERSION` 文件长期停在 `v0.2.2`, 而 `app.__version__` / `pyproject.toml` /
`package.json` 早已是 `0.3.2` —— 一旦 `app.__version__` 为空 (源码直跑、未走
PyInstaller 注入), 兜底就会亮出旧版本号。本测试把四处钉死, 任一漂移即失败。
"""
from __future__ import annotations

import json
import re
import tomllib
from pathlib import Path

import app as app_pkg

PROJECT_ROOT = Path(__file__).resolve().parents[2]
BACKEND_ROOT = PROJECT_ROOT / "backend"
FRONTEND_ROOT = PROJECT_ROOT / "frontend"


def _read_version_file() -> str:
    """读取 VERSION 文件 (去掉 BOM/空白/可能的前导 v)。"""
    return (PROJECT_ROOT / "VERSION").read_text(encoding="utf-8-sig").strip().lstrip("v")


def _read_pyproject_version() -> str:
    data = tomllib.loads((BACKEND_ROOT / "pyproject.toml").read_text(encoding="utf-8"))
    return str(data["project"]["version"])


def _read_package_json_version() -> str:
    data = json.loads((FRONTEND_ROOT / "package.json").read_text(encoding="utf-8"))
    return str(data["version"])


def test_版本号四处一致():
    authoritative = app_pkg.__version__.lstrip("v")
    version_file = _read_version_file()
    pyproject = _read_pyproject_version()
    package_json = _read_package_json_version()

    assert authoritative, "app.__version__ 不能为空 — 它是版本的唯一权威来源"
    assert version_file == authoritative, (
        f"VERSION 文件={version_file!r} 与 app.__version__={authoritative!r} 不一致; "
        "VERSION 是 /api/data/version 的兜底来源, 漂移会让源码直跑时显示旧版本"
    )
    assert pyproject == authoritative, (
        f"backend/pyproject.toml={pyproject!r} 与 app.__version__={authoritative!r} 不一致"
    )
    assert package_json == authoritative, (
        f"frontend/package.json={package_json!r} 与 app.__version__={authoritative!r} 不一致"
    )


def test_VERSION_文件格式规范():
    raw = (PROJECT_ROOT / "VERSION").read_text(encoding="utf-8-sig")
    assert raw.strip() == raw.rstrip("\r\n").lstrip(), f"VERSION 不应含首尾多余空白, 实际 {raw!r}"
    assert raw.count("\n") <= 1, f"VERSION 不应多行, 实际 {raw!r}"
    assert re.fullmatch(r"v\d+\.\d+\.\d+", raw.strip()), (
        f"VERSION 应为 vX.Y.Z 形式, 实际 {raw.strip()!r}"
    )
