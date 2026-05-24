# bili-liver-monitor

Bilibili 主播直播状态监控与推送工具。

All codes are vibed.

## Features

- [x] 开播/下播提醒（HTTP 轮询）
- [x] Bilibili 动态推送
- [x] 微博动态推送
- [x] NapCat Bot 命令
- [x] 直播封面推图
- [x] Bark iOS 告警通道
- [ ] WebSocket 实时监听（开播检测/礼物/SC）

## Quick Start

### Prerequisties

- Python 3.13+
- [uv](https://docs.astral.sh/uv/)（推荐）或 pip

### using uv (recommened)

```bash
# 克隆仓库
git clone git@github.com:RiveChen/bili-liver-monitor.git
cd bili-liver-monitor

# 安装依赖
uv sync

# 复制配置并编辑
cp config.yml config.local.yml
# 编辑 config.local.yml，填入你的配置

# 运行
uv run python main.py --config config.local.yml
# 或
uv run python -m live_monitor
```

### using pip (alternative)

```bash
pip install -e .
python main.py
```

### Config

项目通过 `config.yml` 进行配置，主要包含以下模块：

| 模块               | 说明                                               |
| ------------------ | -------------------------------------------------- |
| `monitor.bilibili` | Bilibili 直播与动态监控，配置主播 UID、轮询间隔等  |
| `monitor.weibo`    | （可选）微博动态监控，配置微博 UID、Cookie 等      |
| `pusher.napcat`    | NapCatQQ 推送，配置 API 地址、目标 QQ/群聊         |
| `pusher.bark`      | （可选）Bark iOS 推送（运营告警专用）              |
| `listener`         | NapCatQQ 事件监听，配置 WebSocket 连接与群聊白名单 |

详细配置项说明请参考 `config.yml` 中的注释。

## Credits

- [NaqCatQQ](https://github.com/NapNeko/NapCatQQ)
- [laplace-live/ws](https://github.com/laplace-live/ws)
- [aio-dynamic-push](https://github.com/nfe-w/aio-dynamic-push)
- [napcat-plugin-weibo-push](https://github.com/sanxi33/napcat-plugin-weibo-push)

## License

MIT.
