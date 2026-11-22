# 特征工程与数据挖掘工作台

面向特征构建、特征选择、数据切片、模型训练与挖掘结果管理的数据分析特征工程平台。

这是一个前后端分离的管理平台：前端 Vue 3 + Vite + TypeScript，后端 FastAPI（Python）。
两边各自独立启动，前端 dev server 已关掉自动打开页面，启动后按终端打印的地址手工打开。

## 目录结构

```text
.
├── frontend/                 Vue 3 + Vite + TypeScript 前端
│   ├── src/views/WorkbenchView.vue|tsx  全部模块共享工作台
│   ├── src/data/modules.ts              18个业务模块配置
│   ├── src/api/              统一请求封装
│   └── vite.config.ts        dev server 配置（open: false）
├── backend/                  FastAPI（Python） 后端
│   ├── app/routers/resource.py  通用查询/导出/登记/动作接口
│   ├── app/services/<key>.py    每个模块独立领域逻辑
│   ├── app/services/training_window.py  版本化训练窗口（迁移/清单/下发/回放）
│   ├── app/services/base.py     共享服务骨架
│   └── app/store.py          内存数据仓库与示例数据
├── .gitignore
└── docker-compose.yml
```

## 启动

### 后端

```bash
cd backend
python3 -m venv .venv && .venv/bin/pip install -r requirements.txt
./run.sh
```

健康检查：`curl http://127.0.0.1:8000/api/health`

### 前端

```bash
cd frontend
npm ci
npm run dev
```

前端默认监听 `http://127.0.0.1:5173/`，dev server 不会自动打开浏览器，
需要自己访问。`/api` 由 vite 代理到后端 `http://127.0.0.1:8000`。

## 业务模块

| 模块 | 目录 | 业务对象 | 主要字段 |
| --- | --- | --- | --- |
| 特征源管理 | `feature_source` | 特征源 | 源编号、源名称、数据表名 |
| 特征定义 | `feature_define` | 特征定义 | 特征编号、特征名称、特征类型 |
| 特征组管理 | `feature_group` | 特征组 | 组编号、组名称、包含特征 |
| 数据切片配置 | `data_slice` | 切片配置 | 切片编号、切片名称、数据来源 |
| 特征计算任务 | `feature_compute` | 计算任务 | 任务编号、关联特征、数据切片 |
| 特征筛选策略 | `feature_selection` | 筛选策略 | 策略编号、特征集、筛选方法 |
| 特征重要性分析 | `feature_iv` | 重要性评估 | 评估编号、关联特征、信息值IV |
| 特征快照管理 | `feature_snapshot` | 特征快照 | 快照编号、特征集、快照日期 |
| 训练流水线 | `train_pipeline` | 流水线配置 | 流水线编号、流水线名称、特征集 |
| 模型注册中心 | `model_registry` | 模型注册 | 模型编号、模型名称、算法类型 |
| 批量预测任务 | `predict_batch` | 预测任务 | 任务编号、模型编号、数据切片 |
| 挖掘结果管理 | `mining_result` | 挖掘结果 | 结果编号、分析主题、算法类型 |
| 聚类分析配置 | `cluster_config` | 聚类配置 | 配置编号、特征集、算法类型 |
| 特征相关性矩阵 | `feature_corr` | 相关性矩阵 | 矩阵编号、特征集、计算方法 |
| 特征漂移监控 | `drift_monitor` | 漂移监控 | 监控编号、关联特征、基准分布 |
| 数据导出管理 | `data_export` | 导出任务 | 任务编号、数据来源、导出范围 |
| 流水线调度 | `pipeline_schedule` | 调度配置 | 调度编号、关联流水线、Cron表达式 |
| 业务场景标注 | `scenario_label` | 场景标注 | 标注编号、场景名称、关联特征 |

## 训练窗口（版本化训练周期）

历史数据里训练周期有两种写法：`训练周期` 里的 `weekly`，以及直接填在
`训练周期` / `Cron表达式` 里的五段 Cron。`app/services/training_window.py`
把两者归一成带版本、时区、起止日期和分区范围的训练窗口，调度、训练流水线和
模型回放都消费同一份数据：

- 迁移：`migrate_schedule` / `migrate_pipeline` 生成新的周期版本 `v1`、`v2`…，
  旧窗口保留为“已替代”。不传时区时按 `Asia/Shanghai` 计算，但状态标成
  `待确认`，并在 `原表达式` / `待核项` 里保留旧表达式和需人工核对的原因。
- 已受理旧跑次：`accept_schedule` 冻结当时的周期版本和分区，跑次按原窗口回放。
- 换代重合：迁移时用 `policy` 决定重合日期归属——`carry_over_old`（默认，
  沿用旧计划）或 `handover_unconsumed`（已消费保留旧跑次，未消费分区交给新窗口）。
- 分区占用：`partition_claim` 保证同一分区同一时刻只有一个有效占用，换代期间
  重复下发会被跳过而不是重复触发。
- 训练输入时段清单：`build_manifest` 输出时区、起止日期、分区范围和逐日期分区。
- 训练：`train_and_register_model` / `start_training` 把实际读取的分区、周期版本
  和周期来源写进模型产物的 `训练输入快照`。
- 回放：`replay_model` 按模型冻结的快照只读回放原分区与原时间范围，并返回
  调度接口当时的周期版本（`schedule_link.cycle_version`）。

接口前缀 `/api/train-windows`，另有前端页面 `/training_window` 演示
“受理旧跑次 → 迁移 → 按清单下发 → 训练 → 回放”的完整流程。

## 约定

- 全部模块共享`frontend/src/views/WorkbenchView.vue|tsx`和一套资源路由。
- 每个模块仍保留独立`backend/app/services/<模块>.py`；纯前端保留
  `frontend/src/domain/<模块>.ts`，用于承载真实业务差异并维持36题位容量。
- 列表接口统一返回 `{ items, total, page, size }`，动作接口统一返回 `{ ok, message }`。
- 状态流转只允许在 `app/services` 里改，路由层不做业务判断。

领域：数据工程
子方向：数据分析与数据应用
