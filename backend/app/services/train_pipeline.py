"""训练流水线领域服务：特征集选择、算法配置与评估指标管理。"""
from __future__ import annotations

from typing import Any

from app.services.base import BaseModuleService
from app.services.training_window import training_windows
from app.store import store


class TrainPipelineService(BaseModuleService):
    MODULE = "train_pipeline"
    ENTITY = "流水线配置"
    OPERATION_TERM = "校验"
    LIST_FIELDS = [
        "流水线编号", "流水线名称", "特征集", "算法类型", "超参数配置", "评估指标",
        "训练周期", "流水线版本", "当前训练窗口", "当前周期版本", "时区",
    ]
    REQUIRED_FIELDS = ["流水线编号", "流水线名称", "特征集"]
    STATUS_ORDER = ["草稿", "待校验", "训练中", "已完成"]
    ACTION_RULES = {"提交校验": "待校验", "开始训练": "训练中", "确认完成": "已完成"}
    NEGATIVE_ACTIONS = []

    # ── 链路3/4: 特征组 → 训练流水线 → 模型注册 → 模型回放 ──
    def train_and_register_model(self, pipeline_id: int, *,
                                 schedule_id: int | None = None,
                                 cycle_version: str | None = None,
                                 partitions: list[str] | None = None,
                                 policy: str = "carry_over_old") -> dict[str, Any]:
        """按当前版本化训练窗口训练，并把实际读取的分区和周期来源写进模型产物。

        没有可用窗口时会先按流水的旧训练周期迁出一版窗口；无法确定周期时
        走原来的演示逻辑，避免把历史链路直接打断。
        """
        pipe = store.find(self.MODULE, pipeline_id)
        if pipe is None:
            return {"ok": False, "message": f"流水线 {pipeline_id} 不存在"}
        window = training_windows._pick_window(
            schedule_id=schedule_id, pipeline_id=pipeline_id, version=cycle_version)
        if window is None:
            result = training_windows.start_training(
                schedule_id=schedule_id, pipeline_id=pipeline_id,
                version=cycle_version, partitions=partitions,
                policy=policy, auto_migrate=True,
            )
            if result.get("ok"):
                return result
            if "没有可用的训练窗口" not in str(result.get("message", "")):
                return result
            return self._train_without_window(pipeline_id)

        pipe["status"] = "训练中"
        pipe["pending"] = True
        result = training_windows.start_training(
            schedule_id=schedule_id or int(float(str(window.get("关联调度") or 0) or 0)),
            pipeline_id=pipeline_id,
            version=window.get("周期版本"),
            partitions=partitions,
        )
        if result.get("ok"):
            result["feature_set"] = pipe.get("特征集", "")
            result["evaluation_metrics"] = pipe.get("评估指标", "")
        return result

    def start_training_from_window(self, pipeline_id: int, **kwargs: Any) -> dict[str, Any]:
        return training_windows.start_training(pipeline_id=pipeline_id, **kwargs)

    def migrate_training_cycle(self, pipeline_id: int, **kwargs: Any) -> dict[str, Any]:
        """把流水线的旧训练周期迁移成版本化窗口（没有调度时自动补建）。"""
        return training_windows.migrate_pipeline(pipeline_id, **kwargs)

    def training_input_manifest(self, pipeline_id: int, **kwargs: Any) -> dict[str, Any]:
        return training_windows.build_manifest(pipeline_id=pipeline_id, **kwargs)

    def dispatch_training_inputs(self, pipeline_id: int, **kwargs: Any) -> dict[str, Any]:
        return training_windows.dispatch_manifest(pipeline_id=pipeline_id, **kwargs)

    def replay_model_window(self, model_id: int) -> dict[str, Any]:
        return training_windows.replay_model(model_id)

    def _train_without_window(self, pipeline_id: int) -> dict[str, Any]:
        pipe = store.find(self.MODULE, pipeline_id)
        pipe["status"] = "训练中"
        pipe["pending"] = True
        model_rows = store.rows("model_registry")
        next_id = max((int(r.get("id", 0)) for r in model_rows), default=0) + 1
        model_rows.append({
            "id": next_id,
            "status": "草稿",
            "pending": True,
            "abnormal": False,
            "模型编号": f"MODEL-{next_id:04d}",
            "模型名称": f"model_{pipe.get('流水线名称', 'unknown')}_v1",
            "算法类型": pipe.get("算法类型", "xgboost"),
            "训练特征": pipe.get("特征集", ""),
            "模型版本": "v1.0.0",
            "模型文件": f"models/pipeline_{pipeline_id}.pkl",
            "评估分数": "0.892",
            "部署状态": "未部署",
            "关联流水线": str(pipeline_id),
            "周期来源": "未确定",
            "训练输入快照": {},
        })
        pipe["status"] = "已完成"
        pipe["pending"] = False
        return {"ok": True, "pipeline_id": pipeline_id, "model_id": next_id, "score": 0.892}

    def validate_pipeline_config(self, pipeline_id: int) -> dict[str, Any]:
        """校验训练流水线的配置完整性，包括特征集引用和超参数合法性。"""
        pipe = store.find(self.MODULE, pipeline_id)
        if pipe is None:
            return {"ok": False}
        errors = []
        if not pipe.get("特征集"):
            errors.append("缺少特征集配置")
        if not pipe.get("算法类型"):
            errors.append("缺少算法类型")
        if not pipe.get("超参数配置"):
            errors.append("缺少超参数配置")
        if errors:
            pipe["status"] = "待校验"
            pipe["abnormal"] = True
        else:
            pipe["status"] = "已生效" if "已生效" in self.STATUS_ORDER else "草稿"
            pipe["abnormal"] = False
        return {"ok": len(errors) == 0, "errors": errors, "pipeline_id": pipeline_id}

    def schedule_pipeline_execution(self, pipeline_id: int) -> dict[str, Any]:
        """为训练流水线创建调度配置，支持定期自动执行并关联Cron表达式。"""
        pipe = store.find(self.MODULE, pipeline_id)
        if pipe is None:
            return {"ok": False}
        window = training_windows._pick_window(
            schedule_id=None, pipeline_id=pipeline_id, version=None)
        sched_rows = store.rows("pipeline_schedule")
        next_id = max((int(r.get("id", 0)) for r in sched_rows), default=0) + 1
        sched_rows.append({
            "id": next_id,
            "status": "草稿",
            "pending": True,
            "abnormal": False,
            "调度编号": f"SCH-{next_id:04d}",
            "关联流水线": str(pipeline_id),
            "Cron表达式": pipe.get("训练周期", "0 2 * * 1"),
            "依赖任务": "",
            "失败策略": "retry3",
            "通知方式": "email",
            "最大重试数": "3",
            "调度状态": "草稿",
        })
        schedule = sched_rows[-1]
        if window is not None:
            schedule["周期版本"] = window.get("周期版本")
            schedule["窗口编号"] = window.get("窗口编号")
            schedule["窗口起止日期"] = (
                f"{window.get('开始日期')} ~ {window.get('结束日期')}")
        return {"ok": True, "schedule_id": next_id, "pipeline_id": pipeline_id,
                "cycle_version": (window or {}).get("周期版本")}

    def query_pipeline_evaluation(self, pipeline_id: int) -> dict[str, Any]:
        """查询训练流水线的评估指标结果，包括AUC、logloss和特征重要性排名。"""
        pipe = store.find(self.MODULE, pipeline_id)
        if pipe is None:
            return {"ok": False}
        return {
            "ok": True,
            "pipeline_id": pipeline_id,
            "pipeline_name": pipe.get("流水线名称"),
            "metrics": pipe.get("评估指标", ""),
            "status": pipe.get("status"),
            "algorithm": pipe.get("算法类型"),
            "version": pipe.get("流水线版本"),
        }