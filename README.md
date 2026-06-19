# Data Integration Process

基于 Prefect 3 的 XML 文件归档、分类、验证和发布流程。

## 脚本启动

三个脚本彼此独立，可按需分别运行。执行前会自动切换到项目根目录，并直接读取 `config/` 下的本地配置文件。

```bash
# 终端 1：Prefect 服务
uv run prefect server start

# 终端 2
export PREFECT_API_URL=http://127.0.0.1:4200/api

# 1. 配置刷新
uv run python -m scripts.init_config --prepare-dirs --overwrite

# 2. 集成循环
uv run python -m scripts.run_integration_loop --once

# 3. 发布循环
uv run python -m scripts.run_publish_loop --once
```

Prefect UI 中可见的顶层 Flow：

| Flow | 脚本 | 说明 |
|------|------|------|
| `config-refresh` | `scripts.init_config` | 配置校验 / 刷新 |
| `integration-loop` | `scripts.run_integration_loop` | 一轮集成；每个 source 是一个 task |
| `publish-loop` | `scripts.run_publish_loop` | 一轮发布；每个 source 是一个 task |

- `--dry-run`：只校验配置，不写入 Prefect Variables
- `--prepare-dirs`：创建配置里声明的运行目录
- `--overwrite`：覆盖已有 Prefect Variables