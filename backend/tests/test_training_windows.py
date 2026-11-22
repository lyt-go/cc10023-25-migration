"""训练窗口迁移与换代集成测试。

覆盖需求里的三个关键动作：
1. weekly / 五段 Cron 两类旧配置迁移成带版本、时区、起止日期和分区范围的窗口；
2. 旧跑次刚受理就迁移窗口，新窗口训练时重合日期的归属（沿用 / 移交）；
3. 从旧模型详情回放原分区和原时间范围，调度接口关联它当时的周期版本。
"""
from __future__ import annotations

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from app.services import service_for
from app.services.training_window import (
    POLICY_CARRY_OVER,
    POLICY_HANDOVER,
    parse_legacy_cycle,
    training_windows,
)
from app.store import store


def _fresh():
    store.reset()


def _partitions(start: str, end: str) -> list[str]:
    from datetime import date, timedelta

    cursor, last = date.fromisoformat(start), date.fromisoformat(end)
    rows = []
    while cursor <= last:
        rows.append(f"dt={cursor.isoformat()}")
        cursor += timedelta(days=1)
    return rows


# ── 旧配置解析 ──
def test_parse_weekly_and_cron_legacy():
    weekly = parse_legacy_cycle("weekly", timezone="Asia/Shanghai")
    assert weekly["cadence"] == "weekly", weekly
    assert weekly["timezone"] == "Asia/Shanghai"
    assert weekly["timezone_status"] == "已确认"
    assert weekly["pending"] == [], weekly["pending"]

    cron = parse_legacy_cycle("0 2 * * 1", timezone="Asia/Shanghai")
    assert cron["cadence"] == "weekly"
    assert cron["cron"] == "0 2 * * 1"
    assert cron["cycle_source"] == "cron"

    daily = parse_legacy_cycle("30 4 * * *", timezone="UTC")
    assert daily["cadence"] == "daily", daily

    monthly = parse_legacy_cycle("0 6 1 * *", timezone="UTC")
    assert monthly["cadence"] == "monthly", monthly


def test_unconfirmed_timezone_exposes_raw_expression_and_pending():
    parsed = parse_legacy_cycle("0 3 * * 1")
    assert parsed["timezone_status"] == "待确认"
    assert parsed["timezone"] == "Asia/Shanghai"
    assert parsed["raw"] == "0 3 * * 1"
    pending = parsed["pending"]
    assert any(item["type"] == "timezone" for item in pending), pending
    assert pending[0]["raw_expression"] == "0 3 * * 1"

    # 表达式里声明的时区可用时，不再需要人工核对。
    declared = parse_legacy_cycle("CRON_TZ=America/New_York 0 3 * * 1")
    assert declared["timezone"] == "America/New_York"
    assert declared["timezone_status"] == "已确认"

    bad = parse_legacy_cycle("every monday", timezone="Asia/Shanghai")
    assert bad["cadence"] is None
    assert bad["errors"]
    assert bad["pending"][0]["raw_expression"] == "every monday"


# ── 迁移成版本化窗口 ──
def test_migrate_weekly_schedule_to_versioned_window():
    _fresh()
    schedule = service_for("pipeline_schedule")
    result = schedule.migrate_schedule_timing(
        4, timezone="Asia/Shanghai", effective_date="2026-11-16",
        policy=POLICY_CARRY_OVER)
    assert result["ok"], result
    # 示例数据里调度 4 已有 v1，本轮迁出 v2；Cron 写法被识别成 weekly 频率。
    assert result["window_version"] == "v2", result
    assert result["cycle_source"] == "cron"
    assert result["window"]["调度类型"] == "weekly"
    assert result["start_date"] == "2026-11-16"
    assert result["end_date"] == "2026-11-22"
    assert result["partition_range"]["count"] == 7
    assert result["partition_range"]["partitions"] == _partitions("2026-11-16", "2026-11-22")
    assert result["timezone"] == "Asia/Shanghai"

    # 流水线侧“weekly”写法同样能迁：先建一条只有 weekly 训练周期的新流水线。
    group = service_for("feature_group")
    created = group.create_training_pipeline(3)
    pipeline_id = created["pipeline_id"]
    assert store.find("train_pipeline", pipeline_id)["训练周期"] == "weekly"
    pipeline = service_for("train_pipeline")
    migrated = pipeline.migrate_training_cycle(pipeline_id, effective_date="2026-11-23")
    assert migrated["ok"], migrated
    assert migrated["cycle_source"] == "weekly"
    assert migrated["window_version"] == "v1"
    assert migrated["start_date"] == "2026-11-23"
    assert migrated["end_date"] == "2026-11-29"

    # 旧窗口保留成“已替代”，历史跑次和模型回放都还能引用。
    windows = training_windows.list_windows(schedule_id=4)
    assert [row["周期版本"] for row in windows] == ["v0", "v1", "v2"], windows
    assert windows[-1]["窗口状态"] == "已激活"
    assert all(row["窗口状态"] == "已替代" for row in windows[:-1])


def test_migrate_cron_without_timezone_flags_pending():
    _fresh()
    schedule = service_for("pipeline_schedule")
    result = schedule.migrate_schedule_timing(5, effective_date="2026-11-16")
    assert result["ok"], result
    assert result["cycle_source"] == "cron"
    assert result["timezone_status"] == "待确认"
    assert result["raw_expression"] == "0 3 * * 1"
    assert result["pending_items"], result
    assert result["window"]["原表达式"] == "0 3 * * 1"
    assert result["window"]["时区"] == "Asia/Shanghai"
    # 未确认时区不阻断迁移，但要在窗口和调度上留痕。
    sched = store.find("pipeline_schedule", 5)
    assert sched["时区状态"] == "待确认"
    assert sched["周期版本"] == "v1"

    manifest = schedule.generate_training_input_manifest(5)
    assert manifest["timezone_status"] == "待确认"
    assert manifest["raw_expression"] == "0 3 * * 1"
    assert manifest["pending_items"]


def test_manifest_has_timezone_dates_and_partition_range():
    _fresh()
    schedule = service_for("pipeline_schedule")
    manifest = schedule.generate_training_input_manifest(4)
    assert manifest["ok"], manifest
    assert manifest["cycle_version"] == "v1"
    assert manifest["timezone"] == "Asia/Shanghai"
    assert manifest["start_date"] == "2026-11-16"
    assert manifest["end_date"] == "2026-11-22"
    assert manifest["partition_range"]["partitions"] == _partitions(
        "2026-11-16", "2026-11-22")
    assert manifest["输入时段"][0]["周期版本"] == "v1"
    assert len(manifest["日期分区"]) == 7
    assert manifest["可下发数量"] == 7


# ── 受理旧跑次后立即换代 ──
def test_accept_then_migrate_carry_over_old():
    _fresh()
    schedule = service_for("pipeline_schedule")
    accepted = schedule.accept_legacy_run(4, accepted_at="2026-11-16T02:00:00+08:00")
    assert accepted["ok"], accepted
    assert accepted["cycle_version"] == "v1"
    assert accepted["reserved_partitions"] == _partitions("2026-11-16", "2026-11-22")
    run_id = accepted["run_id"]

    migrated = schedule.migrate_schedule_timing(
        4, "0 3 * * 2", timezone="Asia/Shanghai", effective_date="2026-11-16",
        policy=POLICY_CARRY_OVER)
    assert migrated["ok"], migrated
    assert migrated["window_version"] == "v2"
    # 7 个重合日期都写明仍归旧跑次。
    assert len(migrated["conflicts"]) == 7
    assert {c["resolution"] for c in migrated["conflicts"]} == {"沿用旧计划"}
    assert {c["owner_run_id"] for c in migrated["conflicts"]} == {run_id}

    manifest = migrated["manifest"]
    assert manifest["可下发数量"] == 0
    assert manifest["冲突数量"] == 7

    # 新窗口发起训练：没有可交付分区，且不能重复触发同一分区。
    trained = training_windows.start_training(
        schedule_id=4, version="v2", partitions=_partitions("2026-11-16", "2026-11-22"))
    assert trained["ok"] is False
    assert trained["blocked"] is True
    assert "没有可下发的日期分区" in trained["message"]

    # 旧跑次仍按原窗口回放；分区没有被动过。
    replay = training_windows.replay_run(run_id)
    assert replay["cycle_version"] == "v1"
    assert replay["start_date"] == "2026-11-16"
    assert replay["partitions"] == _partitions("2026-11-16", "2026-11-22")
    claims = [row for row in store.rows("partition_claim")
              if str(row.get("跑次ID")) == str(run_id) and row["state"] == "reserved"]
    assert len(claims) == 7


def test_full_handover_keeps_original_run_window_for_replay():
    _fresh()
    schedule = service_for("pipeline_schedule")
    accepted = schedule.accept_legacy_run(4, accepted_at="2026-11-16T02:00:00+08:00")
    run_id = accepted["run_id"]

    migrated = schedule.migrate_schedule_timing(
        4, "0 3 * * 2", timezone="Asia/Shanghai", effective_date="2026-11-16",
        policy=POLICY_HANDOVER)
    assert migrated["window_version"] == "v2"
    assert migrated["manifest"]["可下发数量"] == 7
    # 旧跑次一个分区都没消费，全部移交；它仍按受理时冻结的时间窗回放。
    replay = training_windows.replay_run(run_id)
    assert replay["run_status"] == "已移交"
    assert replay["partition_scope"] == "原计划分区（未消费）"
    assert replay["partitions"] == _partitions("2026-11-16", "2026-11-22")


def test_accept_then_migrate_handover_unconsumed_partitions():
    _fresh()
    schedule = service_for("pipeline_schedule")
    accepted = schedule.accept_legacy_run(
        4, accepted_at="2026-11-16T02:00:00+08:00",
        consumed_through="2026-11-18")
    assert accepted["ok"], accepted
    run_id = accepted["run_id"]
    consumed = accepted["run"]["已消费分区"]
    assert consumed == _partitions("2026-11-16", "2026-11-18"), consumed

    migrated = schedule.migrate_schedule_timing(
        4, "0 3 * * 2", timezone="Asia/Shanghai", effective_date="2026-11-16",
        policy=POLICY_HANDOVER)
    assert migrated["ok"], migrated
    resolved = {c["partition"]: c for c in migrated["conflicts"]}
    # 已消费的 3 天保留旧跑次，未消费的 4 天交给新窗口。
    for partition in _partitions("2026-11-16", "2026-11-18"):
        assert resolved[partition]["resolution"] == "已消费保留旧跑次", resolved[partition]
    for partition in _partitions("2026-11-19", "2026-11-22"):
        assert resolved[partition]["resolution"] == "移交给新窗口", resolved[partition]
        assert resolved[partition]["assigned_cycle_version"] == "v2"

    assert migrated["manifest"]["可下发分区"] == _partitions("2026-11-19", "2026-11-22")
    trained = training_windows.start_training(schedule_id=4, version="v2")
    assert trained["ok"], trained
    assert trained["partitions"] == _partitions("2026-11-19", "2026-11-22")
    assert trained["cycle_version"] == "v2"

    # 同一批分区再触发一次，全部跳过，不会重复训练。
    again = training_windows.dispatch_manifest(schedule_id=4, version="v2")
    assert again["dispatched_partitions"] == []
    reasons = {item["reason"] for item in again["skipped"]}
    assert any("不重复触发" in reason for reason in reasons), again["skipped"]

    # 旧跑次剩下未消费的预留分区也已被释放，回放只看到自己消费过的。
    old_replay = training_windows.replay_run(run_id)
    assert old_replay["partitions"] == _partitions("2026-11-16", "2026-11-18")

    # 模型产物保留了新窗口实际读取的分区和周期来源。
    model = store.find("model_registry", trained["model_id"])
    snapshot = model["训练输入快照"]
    assert snapshot["cycle_version"] == "v2"
    assert snapshot["cycle_source"] == "cron"
    assert snapshot["partitions"] == _partitions("2026-11-19", "2026-11-22")
    assert snapshot["window_start"] == "2026-11-16"
    assert snapshot["window_end"] == "2026-11-22"
    assert model["训练分区"] == _partitions("2026-11-19", "2026-11-22")


# ── 回放旧模型 ──
def test_replay_legacy_model_links_original_cycle_version():
    _fresh()
    registry = service_for("model_registry")
    replay = registry.replay_training_window(4)
    assert replay["ok"], replay
    assert replay["cycle_version"] == "v0"
    assert replay["partitions"] == _partitions("2026-11-09", "2026-11-15")
    assert replay["start_date"] == "2026-11-09"
    assert replay["end_date"] == "2026-11-15"
    assert replay["mode"] == "read_only"
    assert replay["replay"]["reuses_partition_claims"] is False
    # 调度当前已经换到 v1，回放仍然关联模型训练时的 v0。
    assert replay["schedule_link"]["schedule_id"] == 4
    assert replay["schedule_link"]["cycle_version"] == "v0"
    assert replay["schedule_link"]["current_cycle_version"] == "v1"
    assert replay["schedule_link"]["cycle_changed"] is True

    schedule = service_for("pipeline_schedule")
    via_schedule = schedule.replay_model(4)
    assert via_schedule["cycle_version"] == "v0"
    assert via_schedule["schedule_link"]["cycle_version"] == "v0"

    # 回放不写分区占用。
    before = len(store.rows("partition_claim"))
    registry.replay_training_window(4)
    assert len(store.rows("partition_claim")) == before


def test_training_manifest_matches_replayed_model():
    _fresh()
    registry = service_for("model_registry")
    result = registry.training_input_manifest(4)
    assert result["ok"], result
    manifest = result["manifest"]
    assert manifest["window_version"] == "v0"
    assert manifest["partition_range"]["partitions"] == _partitions(
        "2026-11-09", "2026-11-15")


# ── 流水线侧入口 ──
def test_pipeline_training_records_cycle_source():
    _fresh()
    pipeline = service_for("train_pipeline")
    result = pipeline.train_and_register_model(5, partitions=_partitions("2026-11-16", "2026-11-17"))
    assert result["ok"], result
    model = store.find("model_registry", result["model_id"])
    snapshot = model["训练输入快照"]
    assert snapshot["cycle_version"]
    assert snapshot["cycle_source"] in ("cron", "weekly")
    assert snapshot["partitions"] == _partitions("2026-11-16", "2026-11-17")
    assert model["时区"] == "Asia/Shanghai"


if __name__ == "__main__":
    tests = [
        test_parse_weekly_and_cron_legacy,
        test_unconfirmed_timezone_exposes_raw_expression_and_pending,
        test_migrate_weekly_schedule_to_versioned_window,
        test_migrate_cron_without_timezone_flags_pending,
        test_manifest_has_timezone_dates_and_partition_range,
        test_accept_then_migrate_carry_over_old,
        test_full_handover_keeps_original_run_window_for_replay,
        test_accept_then_migrate_handover_unconsumed_partitions,
        test_replay_legacy_model_links_original_cycle_version,
        test_training_manifest_matches_replayed_model,
        test_pipeline_training_records_cycle_source,
    ]
    passed = failed = 0
    for test in tests:
        _fresh()
        try:
            test()
            print(f"[PASS] {test.__name__}")
            passed += 1
        except Exception as exc:  # noqa: BLE001 - 测试入口需要汇总失败
            print(f"[FAIL] {test.__name__}: {exc}")
            failed += 1
    print(f"\n总测试: {len(tests)}, 通过: {passed}, 失败: {failed}")
    if failed:
        sys.exit(1)
