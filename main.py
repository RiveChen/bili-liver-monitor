"""
bili-liver-monitor 入口

用法：
    python main.py                  # 使用 config.yml
    python main.py --config my.yml  # 使用自定义配置
"""

import sys

from live_monitor import Application


def main() -> None:
    config_path = "config.yml"

    # Parse --config argument
    args = sys.argv[1:]
    for i, arg in enumerate(args):
        if arg == "--config" and i + 1 < len(args):
            config_path = args[i + 1]

    app = Application(config_path=config_path)
    app.run()


if __name__ == "__main__":
    main()
