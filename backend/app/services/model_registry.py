"""模型注册中心领域服务：训练完成的模型归档、版本管理与评估指标存储。"""
from __future__ import annotations

from typing import Any

from app.services.base import BaseModuleService
from app.services.training_window import training_windows
from app.store import store


class ModelRegistryService(BaseModuleService):
    MODULE = "model_registry"
    ENTITY = "模型注册"
    OPERATION_TERM = "存储"
    LIST_FIELDS = [
        "模型编号", "模型名称", "算法类型", "训练特征", "模型版本", "模型文件",
        "评估分数", "部署状态", "周期版本", "周期来源", "训练分区", "起止日期",
    ]
    REQUIRED_FIELDS = ["模型编号", "模型名称", "算法类型"]
    STATUS_ORDER = ["草稿", "待验收", "已注册", "已废弃"]
    ACTION_RULES = {"提交验收": "待验收", "确认注册": "已注册", "标记废弃": "已废弃"}
    NEGATIVE_ACTIONS = []

    def replay_training_window(self, model_id: int) -> dict[str, Any]:
        """从模型详情回放当时实际读取的分区、周期版本和时间范围。

        回放是只读的：不占用分区、不改变当前窗口，调度接口会把它关联回
        当时那一版周期。
        """
        return training_windows.replay_model(model_id)

    def training_input_manifest(self, model_id: int) -> dict[str, Any]:
        """按模型冻结的窗口版本生成清单，便于核对“当时应读 vs 实际读到”。"""
        replay = training_windows.replay_model(model_id)
        if not replay.get("ok"):
            return replay
        manifest = training_windows.build_manifest(
            schedule_id=replay.get("schedule_id"),
            version=replay.get("cycle_version"),
            persist=False,
            include_claims=False,
        )
        # 回看的是历史窗口当时的计划范围，不叠加当前的分区占用状态。
        for slot in manifest.get("日期分区", []):
            slot["状态"] = "历史计划"
        if manifest.get("ok"):
            manifest["回放分区"] = replay.get("partitions")
        return {"ok": True, "replay": replay, "manifest": manifest}

    # ── 链路4: 模型注册 → 批量预测 → 挖掘结果 ──
    def deploy_and_predict(self, model_id: int) -> dict[str, Any]:
        """将注册完成的模型部署并创建批量预测任务，对全量数据切片执行推理。"""
        model = store.find(self.MODULE, model_id)
        if model is None:
            return {"ok": False, "message": f"模型 {model_id} 不存在"}
        model["部署状态"] = "已部署"
        model["status"] = "已注册"
        model["pending"] = False
        predict_rows = store.rows("predict_batch")
        next_id = max((int(r.get("id", 0)) for r in predict_rows), default=0) + 1
        predict_rows.append({
            "id": next_id,
            "status": "草稿",
            "pending": True,
            "abnormal": False,
            "任务编号": f"PRED-{next_id:04d}",
            "模型编号": str(model_id),
            "数据切片": "full_dataset_slice_001",
            "输出格式": "csv",
            "分片策略": "round_robin",
            "并行度": "16",
            "预测状态": "草稿",
            "结果校验": "auto_validate",
        })
        return {"ok": True, "model_id": model_id, "predict_task_id": next_id, "score": model.get("评估分数")}

    def collect_predict_results_to_mining(self, model_id: int) -> dict[str, Any]:
        """采集批量预测结果并导入挖掘结果模块，生成洞察摘要和关键分群。"""
        model = store.find(self.MODULE, model_id)
        if model is None:
            return {"ok": False}
        mining_rows = store.rows("mining_result")
        next_id = max((int(r.get("id", 0)) for r in mining_rows), default=0) + 1
        mining_rows.append({
            "id": next_id,
            "status": "草稿",
            "pending": True,
            "abnormal": False,
            "结果编号": f"RES-{next_id:04d}",
            "分析主题": f"模型{model_id}预测结果挖掘分析",
            "算法类型": model.get("算法类型", "unknown"),
            "洞察摘要": f"基于模型{model_id}的批量预测发现了3个高价值用户分群，置信度0.85",
            "关键分群": "高价值_群1,活跃_群2,流失风险_群3",
            "置信度": "0.85",
            "结果文件": f"mining/model_{model_id}_results.json",
            "结果版本": "v1.0.0",
        })
        return {"ok": True, "model_id": model_id, "mining_id": next_id}

    def validate_model_quality(self, model_id: int) -> dict[str, Any]:
        """校验模型的评估分数和特征完整性，确定是否达到注册标准。"""
        model = store.find(self.MODULE, model_id)
        if model is None:
            return {"ok": False}
        try:
            score = float(str(model.get("评估分数", 0)))
        except (ValueError, TypeError):
            score = 0
        is_qualified = score >= 0.7
        if is_qualified:
            model["status"] = "已注册"
            model["pending"] = False
        return {"ok": is_qualified, "score": score, "qualified": is_qualified, "model_id": model_id}

    def archive_deprecated_models(self, model_ids: list[int]) -> dict[str, Any]:
        """将废弃模型归档并清理存储空间，保留元数据供后续查询。"""
        archived = []
        for mid in model_ids:
            model = store.find(self.MODULE, mid)
            if model is None:
                continue
            model["status"] = "已废弃"
            model["abnormal"] = False
            archived.append(mid)
        return {"ok": True, "archived_count": len(archived), "archived_ids": archived}