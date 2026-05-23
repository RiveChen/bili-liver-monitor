"""python -m live_monitor entry point."""

from .app import Application


def main() -> None:
    app = Application()
    app.run()


if __name__ == "__main__":
    main()
