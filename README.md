# Data Integration Process

基于 Prefect 3 的 XML 文件归档、分类、验证和发布流程。


## 运行

如果需要 `source_1`、`source_2`、`source_3` 全部执行完之后再等待一段时间，使用 bulk controller：

```powershell
.venv/bin/python -c "from data_integration.flows.bulk_config_admin import initialize_bulk_sources_variables; initialize_bulk_sources_variables()
```

这会初始化 4 个 Prefect Variables；默认不覆盖已存在变量，避免抹掉 Prefect UI 中的修改。如需用 `config/` 下的文件重置这些变量，再显式传入 `overwrite=True`。

- `bulk_source_1_config`
- `bulk_source_2_config`
- `bulk_source_3_config`
- `bulk_sources_controller`

启动总控循环：

```powershell
.venv/bin/python -c "from data_integration.flows.bulk import run_bulk_sources_loop; run_bulk_sources_loop()"
```

`run_bulk_sources_loop()` 是轻量外部触发器：每轮触发一次 `bulk-sources-controller` Prefect Flow Run，等待 `bulk_sources_controller.loop.delay_seconds` 后再触发下一轮。Prefect UI 中不会保留一个长期 sleep 的 controller Flow Run。

总控逻辑是：

1. 读取 `bulk_sources_controller`。
2. 按顺序读取并执行 `bulk_source_1_config`、`bulk_source_2_config`、`bulk_source_3_config`。
3. 全部执行完成后等待 `bulk_sources_controller.loop.delay_seconds`。
4. 进入下一轮。

如需测试单轮，可把 `bulk_sources_controller.loop.cycles` 改成 `1`。