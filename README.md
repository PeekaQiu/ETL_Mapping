# Data Integration Process

基于 Prefect 3 的 XML 文件归档、分类、验证和发布流程。

## 上游 source 目录契约

`source_dirs` 模拟的是**上游用户维护的只读目录**。本系统只会扫描并复制其中的稳定 XML 文件，不会删除、移动或重命名 source 中的原文件。

- **取证快照**：archive 步骤会把稳定文件复制到 `archive_dir`，后续 classify / prepublish / publish 只处理这份不可变快照。
- **稳定窗口**：默认要求文件在 `file_stability_seconds`（默认 5 秒）内不再变化后才处理；稳定窗口前被覆盖的旧内容无法被系统感知。
- **Prepublish 观察窗口**：文件进入 `PREPUBLISHED` 后，需等待 `prepublish_observation_seconds`（默认 0 秒，即立即发布）才能进入 formal publish；时间基准为进入 `PREPUBLISHED` 时更新的 `FileRecord.updated_at`。
- **循环调度间隔**：`controller.loop.delay_seconds` 控制 integration / publish 循环的轮询间隔，与上述两个 per-file 时间窗口无关。
- **投递建议**：上游应使用“临时文件写入 + 原子 rename”，并尽量避免复用同一路径文件名。
- **内容替换**：同一路径文件内容发生变化时，必须在 Prefect UI 中同步提升 `runtime.config_revision`；否则系统会拒绝处理并将副本隔离到 `quarantine_dir`。
- **配置生效**：单个 source run 会冻结启动时读取到的配置；UI 中的修改只会在**下一批** source run 生效。

## 脚本启动

两个循环脚本彼此独立，可按需分别运行。执行前会自动切换到项目根目录。

运行时以 **Prefect Variables** 为配置真相源。首次运行时若 Variable 不存在，系统会自动从 `config/*.json` 创建并继续执行。在 Prefect UI 中修改 Variable 后，**下一批**定时任务会读取到新配置。

修改 `rules`、`source_dirs` 或发布路由时，必须同步提升 `runtime.config_revision`。修改 `sqlite_path`、`output_root`、`prepublish_dir` 前，需先清空对应库中的 `PREPUBLISHED` / `CLASSIFIED_STAGING` 积压。

```bash
# 终端 1：Prefect 服务
deactivate
py -m uv run prefect server start

# 终端 2
export PREFECT_API_URL=http://127.0.0.1:4200/api

# 1. 集成循环
py -m uv run python -m scripts.run_integration_loop --once

# 2. 发布循环
py -m uv run python -m scripts.run_publish_loop --once
```

Prefect UI 中可见的顶层 Flow：

| Flow | 脚本 | 说明 |
|------|------|------|
| `integration-loop` | `scripts.run_integration_loop` | 一轮集成；每个 source 是一个 task |
| `publish-loop` | `scripts.run_publish_loop` | 一轮发布；每个 source 是一个 task |

- `--controller`：controller 的 bootstrap 配置文件路径（默认 `config/bulk_sources_controller.json`），仅在 Variable 缺失时用于首次创建
- 本地 `config/*.json` 仅用于首次引导，运行期以 Prefect Variables 为准
