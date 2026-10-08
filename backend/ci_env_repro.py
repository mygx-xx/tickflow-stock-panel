"""CI 等价环境插件 —— 在本地复现"只有 CI 才红"的失败。

GitHub ubuntu runner 与开发机有三处系统性差异, 每一处都曾造成长期只红在 CI 的
缺陷(2026-10-08 定位):

  1. **时区 = UTC**(开发机多为 UTC+8): 任何 ``datetime.fromtimestamp()``
     这类"隐式本地时区"的用法, 在 CI 上会得出与开发机不同的墙钟;
  2. **未配置 fuyao → 取不到交易日历**: ``trading_calendar()`` 返回 None
     (开发机有网时是 242 天的真实日历), 依赖它的逻辑会走"工作日近似"回退;
  3. **容器刚启动 → ``time.monotonic()`` 很小**(``CI_ENV_FRESH_BOOT=1`` 开启):
     monotonic 是机器/容器的运行时长。开发机常是数天(几十万秒), Actions
     容器可能只启动了几十~几百秒 —— 于是"把时间戳置 0 就当过期"这类写法在
     开发机成立、在 CI 不成立(0 秒 vs TTL 300s 并未过期)。

用法(在 backend/ 下):
  TZ=UTC ./.venv/Scripts/python.exe -m pytest tests -q -p ci_env_repro

只跑关心的文件更快:
  TZ=UTC ./.venv/Scripts/python.exe -m pytest tests -q -p ci_env_repro \\
      tests/test_data_integrity.py tests/test_eltdx_provider.py

第 3 项默认关闭(它只影响少数按 monotonic 判过期的用例, 且会改变计时口径),
需要时显式开启:
  TZ=UTC CI_ENV_FRESH_BOOT=1 ./.venv/Scripts/python.exe -m pytest tests -q -p ci_env_repro

不要把它做成 conftest.py: 那样会无条件改变所有本地测试的环境。
"""
import os


def pytest_configure(config):
    from app.data_providers import custom as custom_sources

    # 只对 fuyao 报"未配置"。不能整体换成 `lambda name: False` —— 那会连
    # stocksdk 等其它 custom provider 一起判否, 把插件注册类用例也弄红,
    # 制造出"复现工具自身引入的假失败"。
    custom_sources.is_custom_provider = lambda name: name != "fuyao"

    if os.environ.get("CI_ENV_FRESH_BOOT"):
        _install_fresh_boot_monotonic()


def _install_fresh_boot_monotonic() -> None:
    """把 eltdx http_client 看到的 ``monotonic()`` 平移成"容器刚启动 ~30s"。

    保留单调递增(只是整体平移), 所以基于 monotonic 差值的截止时间逻辑仍然成立;
    被影响的是"拿绝对量级与阈值比较"的写法 —— 例如
    ``time.monotonic() - _code_at >= _code_ttl_s`` 在 ``_code_at = 0.0`` 时,
    开发机(uptime 数十万秒)恒为真、CI 容器(几十秒)恒为假。
    """
    import time as _time

    from app.plugins.eltdx import http_client

    real = _time.monotonic
    shift = real() - 30.0

    class _ShiftedTime:
        """``time`` 的代理: 只有 monotonic 被平移, 其余属性(如 sleep)照旧。"""

        def __getattr__(self, name):
            return getattr(_time, name)

        @staticmethod
        def monotonic() -> float:
            return max(0.0, real() - shift)

    http_client.time = _ShiftedTime()
