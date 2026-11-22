"""训练窗口动作接口：迁移、受理、清单、下发、训练和回放。

通用资源路由只负责列表 / 登记，这里把跨模块的窗口动作单独挂在
``/api/train_pipeline``、``/api/pipeline_schedule``、``/api/model_registry``
和 ``/api/training_window`` 下，业务判断仍然留在领域服务里。
"""
from __future__ import annotations

from typing import Any

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field

from app.services import service_for
from app.services.pipeline_schedule import PipelineScheduleService
from app.services.training_window import TrainingWindowService, training_windows
from app.store import store

router = APIRouter(prefix="/api/train-windows", tags=["训练窗口"])


class AcceptPayload(BaseModel):
    accepted_at: str | None = None
    window_version: str | None = None
    consumed_through: str | None = None
    consumed_partitions: list[str] | None = None
    policy: str = "carry_over_old"


class MigratePayload(BaseModel):
    new_cycle: str | None = None
    timezone: str | None = None
    effective_date: str | None = None
    policy: str = "carry_over_old"
    period_start: str | None = None
    period_end: str | None = None
    partition_field: str = "dt"
    note: str | None = None
    activate: bool = True


class DispatchPayload(BaseModel):
    version: str | None = None
    run_id: int | None = None
    partitions: list[str] | None = None
    manifest_id: int | None = None


class TrainPayload(BaseModel):
    version: str | None = None
    run_id: int | None = None
    partitions: list[str] | None = None
    policy: str = "carry_over_old"


class TrainFromAcceptedPayload(BaseModel):
    new_cycle: str | None = None
    timezone: str | None = None
    effective_date: str | None = None
    policy: str = "carry_over_old"
    period_start: str | None = None
    period_end: str | None = None
    note: str | None = None
    accept: AcceptPayload = Field(default_factory=AcceptPayload)


class MigrateAllPayload(MigratePayload):
    pass


def _window_service() -> TrainingWindowService:
    return training_windows


def _schedule_service(schedule_id: int) -> PipelineScheduleService:
    service = service_for("pipeline_schedule")
    if not isinstance(service, PipelineScheduleService):
        raise HTTPException(status_code=500, detail="调度服务未注册")
    if service.get_entry(schedule_id) is None:
        raise HTTPException(status_code=404, detail=f"调度配置 {schedule_id} 不存在")
    return service


@router.get("/overview")
def overview() -> dict[str, Any]:
    """窗口总览：当前版本、待核项和仍占用分区的跑次。"""
    service = _window_service()
    windows = service.list_windows()
    runs = service.list_runs()
    reserved = {
        str(claim.get("跑次ID")): claim.get("分区")
        for claim in store.rows("partition_claim")
        if claim.get("state") == "reserved"
    }
    return {
        "ok": True,
        "window_count": len(windows),
        "run_count": len(runs),
        "pending_timezone": [
            {
                "window_id": row.get("id"),
                "window_number": row.get("窗口编号"),
                "schedule_id": row.get("关联调度"),
                "raw_expression": row.get("原表达式"),
                "timezone": row.get("时区"),
                "pending_items": row.get("待核项") or [],
            }
            for row in windows if row.get("时区状态") == "待确认"
        ],
        "unconsumed_runs": [
            {
                "run_id": row.get("id"),
                "run_code": row.get("运行编号"),
                "cycle_version": row.get("周期版本"),
                "status": row.get("status"),
                "reserved": row.get("预留分区") or [],
                "pending_partitions": reserved.get(str(row.get("id")), []),
                "consumed": row.get("已消费分区") or [],
            }
            for row in runs if str(row.get("id")) in reserved
        ],
    }


@router.post("/migrate-all")
def migrate_all(payload: MigrateAllPayload) -> dict[str, Any]:
    return _window_service().migrate_all_legacy(**payload.model_dump())


# ── 调度维度 ──
@router.post("/schedules/{schedule_id}/accept")
def accept_schedule(schedule_id: int, payload: AcceptPayload) -> dict[str, Any]:
    _schedule_service(schedule_id)
    return training_windows.accept_schedule(schedule_id, **payload.model_dump())


@router.post("/schedules/{schedule_id}/migrate")
def migrate_schedule(schedule_id: int, payload: MigratePayload) -> dict[str, Any]:
    service = _schedule_service(schedule_id)
    data = payload.model_dump()
    return service.migrate_schedule_timing(
        schedule_id, data.pop("new_cycle", None), **data)


@router.post("/schedules/{schedule_id}/migrate-after-accept")
def migrate_after_accept(schedule_id: int, payload: TrainFromAcceptedPayload) -> dict[str, Any]:
    service = _schedule_service(schedule_id)
    data = payload.model_dump()
    accept = data.pop("accept", None) or {}
    return service.migrate_after_accept(schedule_id, accept=accept, **data)


@router.get("/schedules/{schedule_id}/manifest")
def schedule_manifest(schedule_id: int, version: str | None = None) -> dict[str, Any]:
    _schedule_service(schedule_id)
    return training_windows.build_manifest(schedule_id=schedule_id, version=version)


@router.post("/schedules/{schedule_id}/dispatch")
def dispatch_schedule(schedule_id: int, payload: DispatchPayload) -> dict[str, Any]:
    _schedule_service(schedule_id)
    return training_windows.dispatch_manifest(schedule_id=schedule_id, **payload.model_dump())


@router.post("/schedules/{schedule_id}/train")
def train_schedule(schedule_id: int, payload: TrainPayload) -> dict[str, Any]:
    _schedule_service(schedule_id)
    return training_windows.start_training(schedule_id=schedule_id, **payload.model_dump())


@router.get("/schedules/{schedule_id}/timeline")
def schedule_timeline(schedule_id: int) -> dict[str, Any]:
    _schedule_service(schedule_id)
    return training_windows.schedule_timeline(schedule_id)


@router.get("/schedules/{schedule_id}/windows")
def schedule_windows(schedule_id: int) -> dict[str, Any]:
    _schedule_service(schedule_id)
    return {"ok": True, "items": training_windows.list_windows(schedule_id=schedule_id)}


# ── 流水线维度 ──
@router.post("/pipelines/{pipeline_id}/migrate")
def migrate_pipeline(pipeline_id: int, payload: MigratePayload) -> dict[str, Any]:
    if service_for("train_pipeline").get_entry(pipeline_id) is None:
        raise HTTPException(status_code=404, detail=f"流水线 {pipeline_id} 不存在")
    data = payload.model_dump()
    return training_windows.migrate_pipeline(
        pipeline_id,
        new_cycle=data.pop("new_cycle", None),
        **data,
    )


@router.get("/pipelines/{pipeline_id}/manifest")
def pipeline_manifest(pipeline_id: int, version: str | None = None) -> dict[str, Any]:
    if service_for("train_pipeline").get_entry(pipeline_id) is None:
        raise HTTPException(status_code=404, detail=f"流水线 {pipeline_id} 不存在")
    return training_windows.build_manifest(pipeline_id=pipeline_id, version=version)


@router.post("/pipelines/{pipeline_id}/dispatch")
def dispatch_pipeline(pipeline_id: int, payload: DispatchPayload) -> dict[str, Any]:
    if service_for("train_pipeline").get_entry(pipeline_id) is None:
        raise HTTPException(status_code=404, detail=f"流水线 {pipeline_id} 不存在")
    return training_windows.dispatch_manifest(pipeline_id=pipeline_id, **payload.model_dump())


@router.post("/pipelines/{pipeline_id}/train")
def train_pipeline(pipeline_id: int, payload: TrainPayload) -> dict[str, Any]:
    service = service_for("train_pipeline")
    if service.get_entry(pipeline_id) is None:
        raise HTTPException(status_code=404, detail=f"流水线 {pipeline_id} 不存在")
    return training_windows.start_training(pipeline_id=pipeline_id, **payload.model_dump())


# ── 跑次 / 模型回放 ──
@router.get("/runs/{run_id}")
def get_run(run_id: int) -> dict[str, Any]:
    run = training_windows.get_run(run_id)
    if run is None:
        raise HTTPException(status_code=404, detail=f"跑次 {run_id} 不存在")
    return run


@router.post("/runs/{run_id}/consume")
def consume_run(run_id: int, payload: AcceptPayload) -> dict[str, Any]:
    return training_windows.consume_run_partitions(
        run_id,
        partitions=payload.consumed_partitions,
        through=payload.consumed_through,
    )


@router.get("/runs/{run_id}/replay")
def replay_run(run_id: int) -> dict[str, Any]:
    return training_windows.replay_run(run_id)


@router.get("/models/{model_id}/replay")
def replay_model(model_id: int) -> dict[str, Any]:
    if service_for("model_registry").get_entry(model_id) is None:
        raise HTTPException(status_code=404, detail=f"模型 {model_id} 不存在")
    return training_windows.replay_model(model_id)


@router.get("/models/{model_id}/manifest")
def model_manifest(model_id: int) -> dict[str, Any]:
    service = service_for("model_registry")
    if service.get_entry(model_id) is None:
        raise HTTPException(status_code=404, detail=f"模型 {model_id} 不存在")
    return service.training_input_manifest(model_id)
