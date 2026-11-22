"""训练窗口相关的示例数据：保留两种旧训练周期写法，供迁移和回放演示。

这些行会追加到 ``SEED_ROWS`` 里，id 从 4 开始，不影响原有三行业务样例。
"""
from __future__ import annotations

from typing import Any

CURRENT_WEEK = ("2026-11-16", "2026-11-22")
PREVIOUS_WEEK = ("2026-11-09", "2026-11-15")


def _partitions(start: str, end: str, field: str = "dt") -> list[str]:
    from datetime import date, timedelta

    cursor = date.fromisoformat(start)
    last = date.fromisoformat(end)
    rows: list[str] = []
    while cursor <= last:
        rows.append(f"{field}={cursor.isoformat()}")
        cursor += timedelta(days=1)
    return rows


def _range(start: str, end: str, field: str = "dt") -> dict[str, Any]:
    partitions = _partitions(start, end, field)
    return {
        "field": field,
        "start": start,
        "end": end,
        "count": len(partitions),
        "partitions": partitions,
    }


def _window(window_id: int, version: str, start: str, end: str, state: str) -> dict[str, Any]:
    return {
        "id": window_id,
        "status": state,
        "pending": False,
        "abnormal": False,
        "窗口编号": f"WIN-{window_id:04d}",
        "关联流水线": "4",
        "关联调度": "4",
        "周期版本": version,
        "周期来源": "cron",
        "调度类型": "weekly",
        "来源表达式": "0 2 * * 1",
        "原表达式": "0 2 * * 1",
        "Cron表达式": "0 2 * * 1",
        "时区": "Asia/Shanghai",
        "时区状态": "已确认",
        "时区来源": "expression",
        "时区按默认值计算": False,
        "开始日期": start,
        "结束日期": end,
        "起止日期": f"{start} ~ {end}",
        "分区字段": "dt",
        "分区范围": _range(start, end),
        "待核项": [],
        "冲突处理": "carry_over_old",
        "迁移说明": "",
        "窗口状态": state,
        "生效时间": f"{start}T00:00:00",
        "创建时间": f"{start}T00:00:00",
    }


_OLD_PARTITIONS = _partitions(*PREVIOUS_WEEK)
_OLD_SNAPSHOT = {
    "run_id": 1,
    "run_code": "RUN-0001",
    "schedule_id": 4,
    "pipeline_id": 4,
    "window_id": 1,
    "window_number": "WIN-0001",
    "cycle_version": "v0",
    "cycle_source": "cron",
    "source_expression": "0 2 * * 1",
    "raw_expression": "0 2 * * 1",
    "timezone": "Asia/Shanghai",
    "timezone_status": "已确认",
    "window_start": PREVIOUS_WEEK[0],
    "window_end": PREVIOUS_WEEK[1],
    "partitions": _OLD_PARTITIONS,
    "partition_range": _range(*PREVIOUS_WEEK),
    "manifest_id": None,
    "trained_at": "2026-11-15T03:20:00+08:00",
}

TRAINING_SEED_ROWS: dict[str, list[dict[str, Any]]] = {
    "train_pipeline": [{
        "id": 4,
        "status": "训练中",
        "pending": False,
        "abnormal": False,
        "流水线编号": "TRAI-1001",
        "流水线名称": "用户价值周训流水线",
        "特征集": "feature_age,feature_income,feature_city",
        "算法类型": "xgboost",
        "超参数配置": '{"max_depth":6,"learning_rate":0.1,"n_estimators":100}',
        "评估指标": "auc,logloss",
        "训练周期": "weekly",
        "流水线版本": "v1.2.0",
        "当前训练窗口": "WIN-0002",
        "当前周期版本": "v1",
        "时区": "Asia/Shanghai",
    }, {
        "id": 5,
        "status": "待校验",
        "pending": True,
        "abnormal": True,
        "流水线编号": "TRAI-1002",
        "流水线名称": "风险名单日更流水线",
        "特征集": "feature_age,feature_income",
        "算法类型": "lightgbm",
        "超参数配置": '{"num_leaves":31,"learning_rate":0.05}',
        "评估指标": "auc,ks",
        # 旧配置直接把五段 Cron 填在训练周期里，且没有声明时区。
        "训练周期": "0 3 * * 1",
        "流水线版本": "v1.0.0",
    }],
    "pipeline_schedule": [{
        "id": 4,
        "status": "已激活",
        "pending": False,
        "abnormal": False,
        "调度编号": "PIPE-1001",
        "关联流水线": "4",
        "Cron表达式": "0 2 * * 1",
        "依赖任务": "",
        "失败策略": "retry3",
        "通知方式": "email",
        "最大重试数": "3",
        "调度状态": "已激活",
        "周期版本": "v1",
        "窗口编号": "WIN-0002",
        "调度类型": "weekly",
        "时区": "Asia/Shanghai",
        "时区状态": "已确认",
        "窗口起止日期": f"{CURRENT_WEEK[0]} ~ {CURRENT_WEEK[1]}",
        "分区范围": _range(*CURRENT_WEEK),
    }, {
        "id": 5,
        "status": "已激活",
        "pending": False,
        "abnormal": True,
        "调度编号": "PIPE-1002",
        "关联流水线": "5",
        "Cron表达式": "0 3 * * 1",
        "依赖任务": "",
        "失败策略": "retry3",
        "通知方式": "email",
        "最大重试数": "3",
        "调度状态": "已激活",
        # 时区字段故意留空：迁移时必须把原表达式和待核项暴露出来。
        "时区": "",
        "时区状态": "待确认",
    }],
    "training_window": [
        _window(1, "v0", PREVIOUS_WEEK[0], PREVIOUS_WEEK[1], "已替代"),
        _window(2, "v1", CURRENT_WEEK[0], CURRENT_WEEK[1], "已激活"),
    ],
    "training_run": [{
        "id": 1,
        "status": "已完成",
        "pending": False,
        "abnormal": False,
        "运行编号": "RUN-0001",
        "关联流水线": "4",
        "关联调度": "4",
        "窗口ID": 1,
        "窗口编号": "WIN-0001",
        "周期版本": "v0",
        "周期来源": "cron",
        "来源表达式": "0 2 * * 1",
        "原表达式": "0 2 * * 1",
        "时区": "Asia/Shanghai",
        "时区状态": "已确认",
        "窗口开始": PREVIOUS_WEEK[0],
        "窗口结束": PREVIOUS_WEEK[1],
        "受理时间": "2026-11-09T02:00:00+08:00",
        "受理方式": "legacy_accepted",
        "预留分区": list(_OLD_PARTITIONS),
        "下发分区": list(_OLD_PARTITIONS),
        "已消费分区": list(_OLD_PARTITIONS),
        "分区范围": _range(*PREVIOUS_WEEK),
        "原计划分区": list(_OLD_PARTITIONS),
        "模型编号": "4",
        "模型快照": dict(_OLD_SNAPSHOT),
        "待核项": [],
        "完成时间": "2026-11-15T03:20:00+08:00",
    }],
    "partition_claim": [
        {
            "id": index,
            "status": "consumed",
            "pending": False,
            "abnormal": False,
            "分区": partition,
            "关联流水线": "4",
            "关联调度": "4",
            "跑次ID": 1,
            "运行编号": "RUN-0001",
            "窗口ID": 1,
            "窗口编号": "WIN-0001",
            "周期版本": "v0",
            "state": "consumed",
            "占用时间": "2026-11-09T02:00:00+08:00",
            "消费时间": "2026-11-15T03:20:00+08:00",
        }
        for index, partition in enumerate(_OLD_PARTITIONS, start=1)
    ],
    "model_registry": [{
        "id": 4,
        "status": "已注册",
        "pending": False,
        "abnormal": False,
        "模型编号": "MODEL-0004",
        "模型名称": "model_用户价值周训流水线_v1",
        "算法类型": "xgboost",
        "训练特征": "feature_age,feature_income,feature_city",
        "模型版本": "v1.2.0",
        "模型文件": "models/pipeline_4_run_1.pkl",
        "评估分数": "0.903",
        "部署状态": "已部署",
        "关联流水线": "4",
        "运行编号": "RUN-0001",
        "调度编号": "4",
        "窗口编号": "WIN-0001",
        "周期版本": "v0",
        "周期来源": "cron",
        "原表达式": "0 2 * * 1",
        "时区": "Asia/Shanghai",
        "训练分区": list(_OLD_PARTITIONS),
        "分区范围": _range(*PREVIOUS_WEEK),
        "起止日期": f"{PREVIOUS_WEEK[0]} ~ {PREVIOUS_WEEK[1]}",
        "训练开始": "2026-11-09T02:00:00+08:00",
        "训练完成": "2026-11-15T03:20:00+08:00",
        "训练输入快照": dict(_OLD_SNAPSHOT),
    }],
    "training_manifest": [],
    "training_window_period": [],
}
