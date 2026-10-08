"""CI 等价环境插件 —— 在本地复现"只有 CI 才红"的失败。

GitHub ubuntu runner 与开发机有两处系统性差异, 二者都曾造成长期只红在 CI 的
缺陷(2026-10-08 首次定位):

  1. **时区 = UTC**(开发机多为 UTC+8): 任何 ``datetime.fromtimestamp()``
     这类"隐式本地时区"的用法, 在 CI 上会得出与开发机不同的墙钟;
  2. **未配置 fuyao → 取不到交易日历**: ``trading_calendar()`` 返回 None
     (开发机有网时是 242 天的真实日历), 依赖它的逻辑会走"工作日近似"回退。

用法(在 backend/ 下):
  TZ=UTC ./.venv/Scripts/python.exe -m pytest tests -q -p ci_env_repro

只跑关心的文件更快:
  TZ=UTC ./.venv/Scripts/python.exe -m pytest tests -q -p ci_env_repro \\
      tests/test_data_integrity.py tests/test_eltdx_provider.py

不要把它做成 conftest.py: 那样会无条件改变所有本地测试的环境。
"""


def pytest_configure(config):
    from app.data_providers import custom as custom_sources

    # 只对 fuyao 报"未配置"。不能整体换成 `lambda name: False` —— 那会连
    # stocksdk 等其它 custom provider 一起判否, 把插件注册类用例也弄红,
    # 制造出"复现工具自身引入的假失败"。
    custom_sources.is_custom_provider = lambda name: name != "fuyao"
