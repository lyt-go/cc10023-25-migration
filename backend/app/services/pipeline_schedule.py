# -*- 数据工程模块：pipeline_schedule.py 处理数据集、字段、批次等数据产物 -*-
"""流水线调度领域服务：模型训练与特征计算的定时调度、依赖编排。

调度不再直接把“训练周期”当成裸文本使用：所有定时语义都通过
``training_windows`` 归一成带版本、时区和分区范围的训练窗口。
"""
from __future__ import annotations

from typing import Any

from app.services.base import BaseModuleService
from app.services.training_window import (
    POLICY_CARRY_OVER,
    TrainingWindowService,
    parse_legacy_cycle,
    training_windows,
)
from app.store import store


class PipelineScheduleService(BaseModuleService):
    MODULE = "pipeline_schedule"
    ENTITY = "调度配置"
    OPERATION_TERM = "关联"
    LIST_FIELDS = [
        "调度编号", "关联流水线", "Cron表达式", "依赖任务", "失败策略",
        "通知方式", "最大重试数", "调度状态", "周期版本", "时区",
        "时区状态", "窗口起止日期", "分区范围",
    ]
    REQUIRED_FIELDS = ["调度编号", "关联流水线", "Cron表达式"]
    STATUS_ORDER = ["草稿", "待校验", "已激活", "已暂停"]
    ACTION_RULES = {"提交校验": "待校验", "激活调度": "已激活", "暂停调度": "已暂停"}
    NEGATIVE_ACTIONS = []

    @property
    def windows(self) -> TrainingWindowService:
        return training_windows

    def activate_and_trigger_pipeline(self, sched_id: int) -> dict[str, Any]:
        """激活调度配置并关联触发对应训练流水线的执行。"""
        sched = store.find(self.MODULE, sched_id)
        if sched is None:
            return {"ok": False}
        sched["status"] = "已激活"
        sched["调度状态"] = "已激活"
        sched["pending"] = False
        pipeline_id = self._pipeline_id(sched)
        pipeline = store.find("train_pipeline", pipeline_id)
        if pipeline:
            pipeline["status"] = "训练中"
            pipeline["pending"] = True
        window = self.windows.current_window(schedule_id=sched_id)
        manifest = None
        if window is not None:
            manifest = self.windows.build_manifest(
                schedule_id=sched_id, version=window.get("周期版本"), persist=False)
        return {
            "ok": True,
            "sched_id": sched_id,
            "pipeline_id": pipeline_id,
            "cron": sched.get("Cron表达式"),
            "cycle_version": (window or {}).get("周期版本"),
            "timezone": (window or {}).get("时区"),
            "manifest_available": (manifest or {}).get("可下发分区", []),
            "message": f"调度 {sched_id} 已激活并触发流水线 {pipeline_id}",
        }

    def validate_cron_expression(self, sched_id: int) -> dict[str, Any]:
        """校验调度配置的 Cron 表达式，并同时检查时区与分区窗口。"""
        sched = store.find(self.MODULE, sched_id)
        if sched is None:
            return {"ok": False}
        parsed = parse_legacy_cycle(
            sched.get("Cron表达式", "") or sched.get("训练周期", ""),
            timezone=sched.get("时区"),
        )
        if not parsed["cadence"] or parsed["errors"]:
            sched["abnormal"] = True
            return {
                "ok": False,
                "cron": sched.get("Cron表达式"),
                "raw_expression": parsed["raw"],
                "reason": "；".join(parsed["errors"]) or "Cron 表达式格式异常，需 5 段",
                "pending_items": parsed["pending"],
            }
        sched["abnormal"] = parsed["timezone_status"] == "待确认"
        sched["调度类型"] = parsed["cadence"]
        sched["时区"] = parsed["timezone"]
        sched["时区状态"] = parsed["timezone_status"]
        from datetime import datetime

        next_run = None
        if parsed["cron"]:
            from app.services.training_window import _next_run  # 内部工具，保持单一实现

            next_run = _next_run(parsed["cron"], datetime.now(), parsed["timezone"])
        return {
            "ok": True,
            "cron": parsed["cron"] or parsed["expression"],
            "cadence": parsed["cadence"],
            "timezone": parsed["timezone"],
            "timezone_status": parsed["timezone_status"],
            "raw_expression": parsed["raw"],
            "next_run": next_run,
            "pending_items": parsed["pending"],
        }

    def link_dependency_chain(self, sched_id: int) -> dict[str, Any]:
        """解析调度依赖关系并关联上下游调度任务，构建调度DAG。"""
        sched = store.find(self.MODULE, sched_id)
        if sched is None:
            return {"ok": False}
        dep_tasks = [d.strip() for d in str(sched.get("依赖任务", "")).split(",") if d.strip()]
        all_scheds = store.rows(self.MODULE)
        downstream = [s.get("id") for s in all_scheds if str(s.get("依赖任务", "")) and
                      str(sched.get("调度编号", "")) in str(s.get("依赖任务", ""))]
        return {"ok": True, "sched_id": sched_id, "dependencies": dep_tasks,
                "downstream_schedules": downstream, "retry_max": sched.get("最大重试数")}

    # ── 训练窗口迁移与下发 ──
    def migrate_schedule_timing(self, sched_id: int, new_cron: str | None = None, *,
                                timezone: str | None = None,
                                effective_date: str | None = None,
                                policy: str = POLICY_CARRY_OVER,
                                period_start: str | None = None,
                                period_end: str | None = None,
                                partition_field: str = "dt",
                                note: str | None = None,
                                activate: bool = True) -> dict[str, Any]:
        """把调度从旧周期写法迁移到新的版本化训练窗口。

        旧跑次仍然按它受理时的窗口回放；新窗口是否继承重合日期由 ``policy``
        决定，默认沿用旧计划（``carry_over_old``）。
        """
        sched = store.find(self.MODULE, sched_id)
        if sched is None:
            return {"ok": False}
        old_cron = sched.get("Cron表达式")
        result = self.windows.migrate_schedule(
            sched_id,
            new_cycle=new_cron,
            timezone=timezone or sched.get("时区") or None,
            effective_date=effective_date,
            policy=policy,
            period_start=period_start,
            period_end=period_end,
            partition_field=partition_field,
            note=note,
            activate=activate,
        )
        result["old_cron"] = old_cron
        result["new_cron"] = sched.get("Cron表达式")
        return result

    def accept_legacy_run(self, sched_id: int, **kwargs: Any) -> dict[str, Any]:
        """在旧周期刚受理时登记一次旧跑次，冻结它采用的窗口版本。"""
        return self.windows.accept_schedule(sched_id, **kwargs)

    def migrate_after_accept(self, sched_id: int, **kwargs: Any) -> dict[str, Any]:
        """先受理旧跑次，再迁移到新的窗口版本，便于对比重合日期归属。"""
        accept_kwargs = kwargs.pop("accept", None) or {}
        accepted = self.windows.accept_schedule(sched_id, **accept_kwargs)
        if not accepted.get("ok"):
            return accepted
        migrated = self.windows.migrate_schedule(sched_id, **kwargs)
        migrated["accepted_run"] = accepted
        return migrated

    def migrate_all_legacy_schedules(self, **kwargs: Any) -> dict[str, Any]:
        return self.windows.migrate_all_legacy(**kwargs)

    def generate_training_input_manifest(self, sched_id: int, **kwargs: Any) -> dict[str, Any]:
        return self.windows.build_manifest(schedule_id=sched_id, **kwargs)

    def dispatch_training_inputs(self, sched_id: int, **kwargs: Any) -> dict[str, Any]:
        return self.windows.dispatch_manifest(schedule_id=sched_id, **kwargs)

    def start_training_from_window(self, sched_id: int, **kwargs: Any) -> dict[str, Any]:
        return self.windows.start_training(schedule_id=sched_id, **kwargs)

    def replay_model(self, model_id: int) -> dict[str, Any]:
        return self.windows.replay_model(model_id)

    def training_timeline(self, sched_id: int) -> dict[str, Any]:
        return self.windows.schedule_timeline(sched_id)

    def list_training_windows(self, sched_id: int) -> list[dict[str, Any]]:
        return self.windows.list_windows(schedule_id=sched_id)

    def _pipeline_id(self, sched: dict[str, Any]) -> int | None:
        try:
            return int(float(str(sched.get("关联流水线") or 0)))
        except (TypeError, ValueError):
            return None
