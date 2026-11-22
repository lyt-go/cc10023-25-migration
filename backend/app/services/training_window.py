"""版本化训练窗口：旧训练周期迁移、训练输入时段清单、分区占用与历史回放。

历史上训练周期有两种写法：``weekly`` 这样的周期关键字，以及直接写在
``训练周期`` / ``Cron表达式`` 里的五段 Cron。本模块把它们归一成带版本的
训练窗口，并为调度、训练流水线和模型回放提供同一份数据：

* 窗口（training_window）：一个周期版本，带时区、起止日期和分区范围；
* 时段清单（training_manifest）：调度可直接下发到日期分区的训练输入；
* 跑次（training_run）：一次受理/训练，冻结当时的周期版本与分区；
* 分区占用（partition_claim）：保证换代期间同一分区不会被触发两次。

模块只依赖内存仓库，业务状态的变化全部收敛在这里。
"""
from __future__ import annotations

import re
from calendar import monthrange
from datetime import date, datetime, timedelta
from typing import Any, Iterable
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from app.services.base import BaseModuleService
from app.store import store

DEFAULT_TIMEZONE = "Asia/Shanghai"
POLICY_CARRY_OVER = "carry_over_old"
POLICY_HANDOVER = "handover_unconsumed"
POLICY_ALIASES = {
    POLICY_CARRY_OVER: POLICY_CARRY_OVER,
    "carry_over": POLICY_CARRY_OVER,
    "old": POLICY_CARRY_OVER,
    "沿用旧计划": POLICY_CARRY_OVER,
    "旧计划沿用": POLICY_CARRY_OVER,
    "从旧计划沿用": POLICY_CARRY_OVER,
    POLICY_HANDOVER: POLICY_HANDOVER,
    "handover": POLICY_HANDOVER,
    "new_window": POLICY_HANDOVER,
    "移交新窗口": POLICY_HANDOVER,
    "交给新窗口": POLICY_HANDOVER,
    "把部分分区交给新窗口": POLICY_HANDOVER,
}
ACTIVE_CLAIM_STATES = ("reserved", "consumed")
WINDOW_TABLE = "training_window"
PERIOD_TABLE = "training_window_period"
RUN_TABLE = "training_run"
CLAIM_TABLE = "partition_claim"
MANIFEST_TABLE = "training_manifest"

_WEEKDAYS = {"mon": 0, "tue": 1, "wed": 2, "thu": 3, "fri": 4, "sat": 5, "sun": 6}
_MONTHS = {
    "jan": 1, "feb": 2, "mar": 3, "apr": 4, "may": 5, "jun": 6,
    "jul": 7, "aug": 8, "sep": 9, "oct": 10, "nov": 11, "dec": 12,
}
_TZ_IN_TEXT = re.compile(r"(?:CRON_TZ|TZ)\s*=\s*([A-Za-z0-9_+\-/]+)")
_TZ_SUFFIX = re.compile(r"@\s*([A-Za-z0-9_+\-/]+)\s*$")
_TZ_BRACKET = re.compile(r"\[\s*([A-Za-z0-9_+\-/]+)\s*\]")


# ── 基础工具 ──────────────────────────────────────────────────────────
def _next_id(rows: list[dict[str, Any]]) -> int:
    return max((int(row.get("id", 0)) for row in rows), default=0) + 1


def _iso(value: date) -> str:
    return value.isoformat()


def _parse_date(value: str | date | None) -> date | None:
    if value is None or value == "":
        return None
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    return date.fromisoformat(str(value)[:10])


def _parse_datetime(value: str | datetime | None, tz: str | None) -> datetime | None:
    if value is None or value == "":
        return None
    if isinstance(value, datetime):
        return value
    text = str(value).replace("Z", "+00:00")
    parsed = datetime.fromisoformat(text)
    if parsed.tzinfo is None and tz:
        parsed = parsed.replace(tzinfo=ZoneInfo(tz))
    return parsed


def _dates(start: date, end: date) -> list[date]:
    result: list[date] = []
    cursor = start
    while cursor <= end:
        result.append(cursor)
        cursor += timedelta(days=1)
    return result


def _partition(day: date, field: str = "dt") -> str:
    return f"{field}={_iso(day)}"


def _partitions(start: date, end: date, field: str = "dt") -> list[str]:
    return [_partition(day, field) for day in _dates(start, end)]


def _partition_range(partitions: list[str], field: str = "dt") -> dict[str, Any]:
    if not partitions:
        return {"field": field, "start": None, "end": None, "count": 0, "partitions": []}
    values = [item.split("=", 1)[-1] for item in partitions]
    return {
        "field": field,
        "start": min(values),
        "end": max(values),
        "count": len(partitions),
        "partitions": list(partitions),
    }


def _normalize_policy(policy: str | None) -> str:
    return POLICY_ALIASES.get(str(policy or "").strip(), POLICY_CARRY_OVER)


def _valid_timezone(name: str | None) -> bool:
    if not name:
        return False
    try:
        ZoneInfo(name)
        return True
    except (ZoneInfoNotFoundError, ValueError):
        return False


def _extract_timezone(raw: str) -> tuple[str | None, str | None]:
    """返回 (时区, 去掉时区声明后的表达式)。"""
    text = str(raw or "").strip()
    match = _TZ_IN_TEXT.search(text)
    if match:
        return match.group(1), _TZ_IN_TEXT.sub("", text).strip()
    match = _TZ_BRACKET.search(text)
    if match:
        return match.group(1), _TZ_BRACKET.sub("", text).strip()
    match = _TZ_SUFFIX.search(text)
    if match:
        return match.group(1), _TZ_SUFFIX.sub("", text).strip()
    return None, text


# ── Cron 解析（只需要支持五段标准格式） ────────────────────────────────
def _cron_value(text: str, names: dict[str, int] | None) -> int:
    lowered = text.strip().lower()
    if names and lowered in names:
        return names[lowered]
    return int(lowered)


def _expand_field(text: str, low: int, high: int,
                  names: dict[str, int] | None = None) -> set[int]:
    result: set[int] = set()
    for chunk in str(text).split(","):
        chunk = chunk.strip()
        if not chunk:
            continue
        base, _, step_text = chunk.partition("/")
        step = int(step_text) if step_text else 1
        if step <= 0:
            raise ValueError("Cron 步长必须为正整数")
        if base in ("*", "?"):
            start, end = low, high
        elif "-" in base:
            left, right = base.split("-", 1)
            start, end = _cron_value(left, names), _cron_value(right, names)
        else:
            start = end = _cron_value(base, names)
        if start > end:
            raise ValueError("Cron 区间起止颠倒")
        for value in range(start, end + 1, step):
            normalized = 0 if (high == 6 and value == 7) else value
            if not low <= normalized <= high:
                raise ValueError("Cron 取值越界")
            result.add(normalized)
    if not result:
        raise ValueError("Cron 字段为空")
    return result


def parse_cron_expression(text: str) -> dict[str, Any]:
    """解析五段 Cron；不合法时抛 ValueError。"""
    parts = str(text or "").strip().split()
    if len(parts) != 5:
        raise ValueError("Cron 表达式需 5 段")
    minute, hour, dom, month, dow = parts
    return {
        "minute": _expand_field(minute, 0, 59),
        "hour": _expand_field(hour, 0, 23),
        "day_of_month": _expand_field(dom, 1, 31),
        "month": _expand_field(month, 1, 12, _MONTHS),
        "day_of_week": _expand_field(dow, 0, 6, _WEEKDAYS),
        "parts": parts,
    }


def _infer_cadence(parts: list[str]) -> str:
    dom, dow = parts[2], parts[4]
    if dom not in ("*", "?"):
        return "monthly"
    if dow not in ("*", "?"):
        return "weekly"
    return "daily"


def _next_run(text: str, after: datetime, tz: str | None) -> str | None:
    try:
        cron = parse_cron_expression(text)
    except ValueError:
        return None
    zone = ZoneInfo(tz) if _valid_timezone(tz) else None
    if zone is not None:
        base = after.astimezone(zone) if after.tzinfo else after.replace(tzinfo=zone)
    else:
        base = after
    for day_offset in range(0, 370):
        day = base.date() + timedelta(days=day_offset)
        if day.month not in cron["month"]:
            continue
        if day.day not in cron["day_of_month"]:
            continue
        if day.weekday() not in cron["day_of_week"]:
            continue
        for hour in sorted(cron["hour"]):
            for minute in sorted(cron["minute"]):
                candidate = datetime(day.year, day.month, day.day, hour, minute, tzinfo=zone)
                if candidate > base:
                    return candidate.isoformat()
    return None


# ── 旧配置解析 ────────────────────────────────────────────────────────
def parse_legacy_cycle(raw_value: str, timezone: str | None = None) -> dict[str, Any]:
    """把 weekly / 五段 Cron 归一成窗口所需的周期描述。

    无法从旧配置确认时区时，仍然给出可计算的默认时区，但把状态标成
    ``待确认``，并把原表达式和待核项一起返回，调用方可以要求人工核对。
    """
    raw = str(raw_value or "").strip()
    pending: list[dict[str, Any]] = []
    errors: list[str] = []
    declared_tz, expression = _extract_timezone(raw)
    if raw == "":
        return {
            "raw": raw, "expression": expression, "cadence": None, "cron": None,
            "timezone": None, "timezone_status": "待确认", "timezone_source": None,
            "pending": [{"type": "expression", "field": "训练周期",
                         "raw_expression": raw, "reason": "旧配置没有训练周期表达式"}],
            "errors": ["旧配置没有训练周期表达式"],
        }

    lowered = expression.casefold()
    cron_text = expression
    if lowered in {"weekly", "week", "每周", "每周一次", "按周"} or lowered.startswith("weekly"):
        cadence = "weekly"
        cron = None
        cycle_source = "weekly"
    else:
        cadence = None
        cron = None
        cycle_source = "cron"
        try:
            parsed = parse_cron_expression(expression)
        except (ValueError, TypeError) as exc:
            errors.append(str(exc))
            pending.append({
                "type": "expression", "field": "训练周期",
                "raw_expression": raw, "reason": f"无法解析旧周期表达式：{exc}",
            })
        else:
            cadence = _infer_cadence(parsed["parts"])
            cron = " ".join(parsed["parts"])

    timezone_source = None
    if declared_tz:
        if _valid_timezone(declared_tz):
            timezone_value = declared_tz
            timezone_status = "已确认"
            timezone_source = "expression"
        else:
            timezone_value = timezone if _valid_timezone(timezone) else DEFAULT_TIMEZONE
            timezone_status = "待确认"
            pending.append({
                "type": "timezone", "field": "时区",
                "raw_expression": raw,
                "reason": f"旧表达式声明的时区 {declared_tz} 无法识别",
                "candidates": [declared_tz, DEFAULT_TIMEZONE],
            })
    elif timezone:
        if _valid_timezone(timezone):
            timezone_value = timezone
            timezone_status = "已确认"
            timezone_source = "parameter"
        else:
            timezone_value = DEFAULT_TIMEZONE
            timezone_status = "待确认"
            pending.append({
                "type": "timezone", "field": "时区",
                "raw_expression": raw,
                "reason": f"传入的时区 {timezone} 无法识别",
                "candidates": [timezone, DEFAULT_TIMEZONE],
            })
    else:
        timezone_value = DEFAULT_TIMEZONE
        timezone_status = "待确认"
        pending.append({
            "type": "timezone", "field": "时区",
            "raw_expression": raw,
            "reason": "旧配置未声明时区，按默认时区计算，需人工核对",
            "candidates": [DEFAULT_TIMEZONE],
        })

    if not cadence and not errors:
        errors.append("无法识别训练周期")
    return {
        "raw": raw,
        "expression": expression,
        "cadence": cadence,
        "cron": cron,
        "cycle_source": cycle_source,
        "timezone": timezone_value,
        "timezone_status": timezone_status,
        "timezone_source": timezone_source,
        "timezone_assumed": timezone_status == "待确认",
        "pending": pending,
        "errors": errors,
    }


def _period_for(cadence: str | None, anchor: date,
                start: date | None = None, end: date | None = None) -> tuple[date, date]:
    if start or end:
        if not (start and end):
            raise ValueError("起止日期必须同时给出")
        if start > end:
            raise ValueError("窗口起始日期不能晚于结束日期")
        return start, end
    if cadence == "weekly":
        week_start = anchor - timedelta(days=anchor.weekday())
        return week_start, week_start + timedelta(days=6)
    if cadence == "monthly":
        first = anchor.replace(day=1)
        return first, anchor.replace(day=monthrange(anchor.year, anchor.month)[1])
    return anchor, anchor


# ── 领域服务 ──────────────────────────────────────────────────────────
class TrainingWindowService(BaseModuleService):
    """版本化训练窗口服务；同时作为 training_window 模块的通用资源服务。"""

    MODULE = WINDOW_TABLE
    ENTITY = "训练窗口"
    OPERATION_TERM = "迁移"
    LIST_FIELDS = [
        "窗口编号", "关联流水线", "关联调度", "周期版本", "周期来源", "时区",
        "时区状态", "起止日期", "分区范围", "窗口状态",
    ]
    REQUIRED_FIELDS = ["窗口编号", "关联流水线"]
    STATUS_ORDER = ["草稿", "已激活", "已替代", "已作废"]
    ACTION_RULES = {"激活窗口": "已激活", "作废窗口": "已作废"}
    NEGATIVE_ACTIONS = ["作废窗口"]

    # ── 查询 ──
    def current_window(self, schedule_id: int | None = None,
                       pipeline_id: int | None = None) -> dict[str, Any] | None:
        rows = self._scoped_windows(schedule_id=schedule_id, pipeline_id=pipeline_id)
        if not rows:
            return None
        live = [row for row in rows if row.get("窗口状态") != "已作废"]
        active = [row for row in live if row.get("窗口状态") == "已激活"]
        return (active or live or rows)[-1]

    def find_window(self, window_id: int) -> dict[str, Any] | None:
        return store.find(WINDOW_TABLE, window_id)

    def find_window_version(self, version: str, schedule_id: int | None = None,
                            pipeline_id: int | None = None) -> dict[str, Any] | None:
        for row in self._scoped_windows(schedule_id=schedule_id, pipeline_id=pipeline_id):
            if str(row.get("周期版本")) == str(version):
                return row
        return None

    def _scoped_windows(self, schedule_id: int | None = None,
                        pipeline_id: int | None = None) -> list[dict[str, Any]]:
        rows = list(store.rows(WINDOW_TABLE))
        if schedule_id is not None:
            rows = [row for row in rows if str(row.get("关联调度")) == str(schedule_id)]
        if pipeline_id is not None:
            rows = [row for row in rows if str(row.get("关联流水线")) == str(pipeline_id)]
        return sorted(rows, key=lambda row: int(row.get("id", 0)))

    def list_windows(self, schedule_id: int | None = None,
                     pipeline_id: int | None = None) -> list[dict[str, Any]]:
        return self._scoped_windows(schedule_id=schedule_id, pipeline_id=pipeline_id)

    def list_runs(self, schedule_id: int | None = None,
                  pipeline_id: int | None = None) -> list[dict[str, Any]]:
        rows = list(store.rows(RUN_TABLE))
        if schedule_id is not None:
            rows = [row for row in rows if str(row.get("关联调度")) == str(schedule_id)]
        if pipeline_id is not None:
            rows = [row for row in rows if str(row.get("关联流水线")) == str(pipeline_id)]
        return sorted(rows, key=lambda row: int(row.get("id", 0)))

    def get_run(self, run_id: int) -> dict[str, Any] | None:
        return store.find(RUN_TABLE, run_id)

    def schedule_timeline(self, schedule_id: int) -> dict[str, Any]:
        sched = store.find("pipeline_schedule", schedule_id)
        if sched is None:
            return {"ok": False, "message": f"调度配置 {schedule_id} 不存在"}
        windows = self.list_windows(schedule_id=schedule_id)
        runs = self.list_runs(schedule_id=schedule_id)
        return {
            "ok": True,
            "schedule_id": schedule_id,
            "current_cycle_version": sched.get("周期版本"),
            "current_window_id": sched.get("窗口编号"),
            "windows": windows,
            "runs": runs,
            "history": [
                {
                    "run_id": run.get("id"),
                    "run_code": run.get("运行编号"),
                    "cycle_version": run.get("周期版本"),
                    "window_id": run.get("窗口编号"),
                    "status": run.get("status"),
                    "start_date": run.get("窗口开始"),
                    "end_date": run.get("窗口结束"),
                    # 已消费优先；刚受理就换代的跑次回退到受理时冻结的原计划。
                    "partitions": (
                        run.get("已消费分区")
                        or run.get("下发分区")
                        or run.get("原计划分区", [])
                    ),
                    "consumed_partitions": run.get("已消费分区", []),
                    "planned_partitions": run.get("原计划分区", []),
                }
                for run in runs
            ],
        }

    # ── 迁移 ──
    def migrate_schedule(
        self,
        schedule_id: int,
        *,
        new_cycle: str | None = None,
        timezone: str | None = None,
        effective_date: str | date | None = None,
        policy: str = POLICY_CARRY_OVER,
        period_start: str | date | None = None,
        period_end: str | date | None = None,
        partition_field: str = "dt",
        note: str | None = None,
        activate: bool = True,
    ) -> dict[str, Any]:
        """把一个调度（及其流水线）的旧周期迁移成新的窗口版本。

        兼容已受理跑次：旧跑次保留自己的窗口快照和分区占用；新版本根据
        ``policy`` 决定重合日期是沿用旧跑次，还是把未消费部分交给新窗口。
        """
        sched = store.find("pipeline_schedule", schedule_id)
        if sched is None:
            return {"ok": False, "message": f"调度配置 {schedule_id} 不存在"}
        pipeline_id = self._pipeline_id(sched)
        pipeline = store.find("train_pipeline", pipeline_id) if pipeline_id else None

        raw = (new_cycle or "").strip()
        if not raw:
            raw = str(sched.get("Cron表达式") or "").strip()
        parsed = parse_legacy_cycle(raw, timezone=timezone)
        if parsed["errors"] and not parsed["cadence"]:
            # 调度里的表达式不可用时，再看流水线的训练周期。
            fallback = str((pipeline or {}).get("训练周期") or "").strip()
            if fallback and fallback != raw:
                fallback_parsed = parse_legacy_cycle(fallback, timezone=timezone)
                if fallback_parsed["cadence"]:
                    raw = fallback
                    parsed = fallback_parsed
        if not parsed["cadence"]:
            return {
                "ok": False,
                "message": "旧训练周期无法迁移：表达式无法识别",
                "schedule_id": schedule_id,
                "pipeline_id": pipeline_id,
                "raw_expression": parsed["raw"],
                "pending_items": parsed["pending"],
                "errors": parsed["errors"],
            }

        previous = self.current_window(schedule_id=schedule_id)
        previous_runs = self._active_runs(schedule_id=schedule_id, pipeline_id=pipeline_id)
        anchor = _parse_date(effective_date)
        if anchor is None:
            if previous:
                anchor = _parse_date(previous.get("开始日期")) or date.today()
            else:
                anchor = date.today()
        try:
            start, end = _period_for(
                parsed["cadence"], anchor,
                _parse_date(period_start), _parse_date(period_end),
            )
        except ValueError as exc:
            return {"ok": False, "message": str(exc), "schedule_id": schedule_id}

        scoped = self._scoped_windows(schedule_id=schedule_id)
        version_number = max(
            (self._version_number(row.get("周期版本")) for row in scoped), default=0) + 1
        version = f"v{version_number}"
        new_partitions = _partitions(start, end, partition_field)
        policy_key = _normalize_policy(policy)

        # 换代前先处理与旧跑次重合的分区。
        conflicts = self._resolve_overlap(
            previous_runs, new_partitions, version, policy_key)

        # 旧窗口只做版本替换，不删除；历史跑次和模型回放都还要用它。
        for old in self._scoped_windows(schedule_id=schedule_id):
            if old.get("窗口状态") not in ("已作废",):
                old["窗口状态"] = "已替代"

        now = datetime.now().isoformat(timespec="seconds")
        window_rows = store.rows(WINDOW_TABLE)
        window_id = _next_id(window_rows)
        window = {
            "id": window_id,
            "status": "已激活" if activate else "草稿",
            "pending": not activate,
            "abnormal": parsed["timezone_status"] == "待确认",
            "窗口编号": f"WIN-{window_id:04d}",
            "关联流水线": str(pipeline_id or ""),
            "关联调度": str(schedule_id),
            "周期版本": version,
            "周期来源": parsed["cycle_source"],
            "调度类型": parsed["cadence"],
            "来源表达式": parsed["cron"] or parsed["expression"],
            "原表达式": parsed["raw"],
            "Cron表达式": parsed["cron"] or "",
            "时区": parsed["timezone"],
            "时区状态": parsed["timezone_status"],
            "时区来源": parsed["timezone_source"],
            "时区按默认值计算": parsed.get("timezone_assumed", False),
            "开始日期": _iso(start),
            "结束日期": _iso(end),
            "起止日期": f"{_iso(start)} ~ {_iso(end)}",
            "分区字段": partition_field,
            "分区范围": _partition_range(new_partitions, partition_field),
            "待核项": parsed["pending"],
            "冲突处理": policy_key,
            "迁移说明": note or "",
            "窗口状态": "已激活" if activate else "草稿",
            "生效时间": now,
            "创建时间": now,
        }
        window_rows.append(window)
        store.rows(PERIOD_TABLE).append({
            "id": _next_id(store.rows(PERIOD_TABLE)),
            "status": window["status"],
            "pending": window["pending"],
            "abnormal": window["abnormal"],
            "窗口编号": window["窗口编号"],
            "窗口ID": window_id,
            "周期版本": version,
            "开始日期": _iso(start),
            "结束日期": _iso(end),
            "时区": parsed["timezone"],
            "分区字段": partition_field,
            "分区范围": _partition_range(new_partitions, partition_field),
            "时段状态": "待下发",
        })

        sched["周期版本"] = version
        sched["窗口编号"] = window["窗口编号"]
        sched["调度类型"] = parsed["cadence"]
        sched["时区"] = parsed["timezone"]
        sched["时区状态"] = parsed["timezone_status"]
        sched["窗口起止日期"] = window["起止日期"]
        sched["分区范围"] = window["分区范围"]
        if parsed["cron"]:
            sched["Cron表达式"] = parsed["cron"]
        if activate:
            sched["status"] = "已激活"
            sched["调度状态"] = "已激活"
            sched["pending"] = False
        if pipeline is not None:
            pipeline["当前训练窗口"] = window["窗口编号"]
            pipeline["当前周期版本"] = version
            pipeline["时区"] = parsed["timezone"]

        manifest = self.build_manifest(schedule_id=schedule_id, version=version)
        return {
            "ok": True,
            "schedule_id": schedule_id,
            "pipeline_id": pipeline_id,
            "window_id": window_id,
            "window_number": window["窗口编号"],
            "window_version": version,
            "cycle_source": parsed["cycle_source"],
            "cadence": parsed["cadence"],
            "timezone": parsed["timezone"],
            "timezone_status": parsed["timezone_status"],
            "timezone_assumed": parsed.get("timezone_assumed", False),
            "start_date": _iso(start),
            "end_date": _iso(end),
            "partition_range": window["分区范围"],
            "raw_expression": parsed["raw"],
            "pending_items": parsed["pending"],
            "policy": policy_key,
            "conflicts": conflicts,
            "manifest": manifest,
            "window": window,
            "message": (
                f"已迁移调度 {schedule_id} 到训练窗口 {version}"
                + ("，时区待人工核对" if parsed["timezone_status"] == "待确认" else "")
            ),
        }

    def migrate_pipeline(self, pipeline_id: int, **kwargs: Any) -> dict[str, Any]:
        """按流水线迁移：优先使用已关联的调度，没有则补建一条调度配置。"""
        pipeline = store.find("train_pipeline", pipeline_id)
        if pipeline is None:
            return {"ok": False, "message": f"流水线 {pipeline_id} 不存在"}
        schedule = self._schedule_for_pipeline(pipeline_id)
        if schedule is None:
            schedule = self._create_legacy_schedule(pipeline)
        result = self.migrate_schedule(int(schedule["id"]), **kwargs)
        result["pipeline_id"] = pipeline_id
        return result

    def migrate_all_legacy(self, **kwargs: Any) -> dict[str, Any]:
        """批量迁移现有调度；已迁移过的调度与新周期参数一起时才再迁一版。"""
        results: list[dict[str, Any]] = []
        for sched in list(store.rows("pipeline_schedule")):
            if not kwargs.get("new_cycle") and self.current_window(
                    schedule_id=int(sched["id"])) is not None:
                results.append({
                    "ok": False,
                    "skipped": True,
                    "schedule_id": sched["id"],
                    "message": "该调度已有训练窗口，如需再迁一版请显式提供新周期",
                })
                continue
            result = self.migrate_schedule(int(sched["id"]), **kwargs)
            results.append(result)
        migrated = [item for item in results if item.get("ok")]
        skipped = [item for item in results if not item.get("ok")]
        return {
            "ok": True,
            "migrated_count": len(migrated),
            "skipped_count": len(skipped),
            "migrated": migrated,
            "skipped": skipped,
        }

    def _resolve_overlap(self, runs: list[dict[str, Any]], new_partitions: list[str],
                         new_version: str, policy: str) -> list[dict[str, Any]]:
        """处理新窗口与旧跑次的重合分区，返回逐分区归属说明。"""
        new_set = set(new_partitions)
        conflicts: list[dict[str, Any]] = []
        for run in runs:
            for claim in list(store.rows(CLAIM_TABLE)):
                if str(claim.get("跑次ID")) != str(run.get("id")):
                    continue
                if claim.get("state") not in ACTIVE_CLAIM_STATES:
                    continue
                partition = str(claim.get("分区"))
                if partition not in new_set:
                    continue
                state = claim.get("state")
                if state == "reserved" and policy == POLICY_HANDOVER:
                    claim["state"] = "released"
                    claim["释放时间"] = datetime.now().isoformat(timespec="seconds")
                    claim["释放给版本"] = new_version
                    self._remove_from_run(run, partition, "预留分区")
                    resolution = "移交给新窗口"
                    owner = new_version
                elif state == "consumed":
                    resolution = "已消费保留旧跑次"
                    owner = run.get("周期版本")
                else:
                    resolution = "沿用旧计划"
                    owner = run.get("周期版本")
                conflicts.append({
                    "partition": partition,
                    "date": partition.split("=", 1)[-1],
                    "owner_run_id": run.get("id"),
                    "owner_run_code": run.get("运行编号"),
                    "owner_cycle_version": run.get("周期版本"),
                    "owner_status": run.get("status"),
                    "claim_state": state,
                    "policy": policy,
                    "resolution": resolution,
                    "assigned_cycle_version": owner,
                })
        return sorted(conflicts, key=lambda item: item["partition"])

    def _remove_from_run(self, run: dict[str, Any], partition: str, field: str) -> None:
        for key in ("预留分区", "下发分区"):
            values = list(run.get(key) or [])
            if partition in values:
                values.remove(partition)
                run[key] = values
        if not (run.get("预留分区") or run.get("下发分区")):
            run["status"] = "部分移交" if run.get("已消费分区") else "已移交"
        run["分区范围"] = _partition_range(list(run.get("预留分区") or []))
        run["更新时间"] = datetime.now().isoformat(timespec="seconds")

    # ── 受理旧跑次 ──
    def accept_schedule(self, schedule_id: int, *,
                        accepted_at: str | datetime | None = None,
                        window_version: str | None = None,
                        consumed_through: str | date | None = None,
                        consumed_partitions: Iterable[str] | None = None,
                        policy: str = POLICY_CARRY_OVER) -> dict[str, Any]:
        """受理一次调度（旧跑次），冻结当时的窗口版本并预留分区。"""
        sched = store.find("pipeline_schedule", schedule_id)
        if sched is None:
            return {"ok": False, "message": f"调度配置 {schedule_id} 不存在"}
        accepted_dt = _parse_datetime(accepted_at, sched.get("时区")) or datetime.now()
        if window_version:
            window = self.find_window_version(window_version, schedule_id=schedule_id)
            if window is None:
                return {
                    "ok": False,
                    "message": f"调度 {schedule_id} 没有周期版本 {window_version}",
                    "schedule_id": schedule_id,
                    "window_version": window_version,
                }
        else:
            window = self.current_window(schedule_id=schedule_id)
        if window is None:
            migration = self.migrate_schedule(
                schedule_id, effective_date=accepted_dt.date(), policy=policy,
                activate=True,
            )
            if not migration.get("ok"):
                return migration
            window = self.find_window(int(migration["window_id"]))
        partitions = list((window.get("分区范围") or {}).get("partitions") or [])
        run_rows = store.rows(RUN_TABLE)
        run_id = _next_id(run_rows)
        run = {
            "id": run_id,
            "status": "已受理",
            "pending": True,
            "abnormal": False,
            "运行编号": f"RUN-{run_id:04d}",
            "关联流水线": window.get("关联流水线"),
            "关联调度": str(schedule_id),
            "窗口ID": window.get("id"),
            "窗口编号": window.get("窗口编号"),
            "周期版本": window.get("周期版本"),
            "周期来源": window.get("周期来源"),
            "来源表达式": window.get("来源表达式"),
            "原表达式": window.get("原表达式"),
            "时区": window.get("时区"),
            "时区状态": window.get("时区状态"),
            "窗口开始": window.get("开始日期"),
            "窗口结束": window.get("结束日期"),
            "受理时间": accepted_dt.isoformat(),
            "受理方式": "legacy_accepted",
            "预留分区": [],
            "下发分区": [],
            "已消费分区": [],
            # 受理时冻结的原计划分区，换代移交后仍按它回放原时间窗。
            "原计划分区": list(partitions),
            "模型编号": "",
            "待核项": window.get("待核项") or [],
        }
        run_rows.append(run)
        reserved, skipped = self._reserve(run, partitions, window)
        run["预留分区"] = reserved
        run["下发分区"] = reserved
        run["分区范围"] = _partition_range(reserved, window.get("分区字段", "dt"))
        sched["已受理跑次"] = run_id
        sched["受理时间"] = run["受理时间"]
        sched["status"] = "已激活"
        sched["调度状态"] = "已激活"
        sched["pending"] = False
        if consumed_through or consumed_partitions:
            self.consume_run_partitions(
                run_id,
                partitions=(list(consumed_partitions) if consumed_partitions else None),
                through=consumed_through,
            )
        run = store.find(RUN_TABLE, run_id) or run
        return {
            "ok": True,
            "schedule_id": schedule_id,
            "pipeline_id": self._pipeline_id(sched),
            "run_id": run_id,
            "run_code": run["运行编号"],
            "cycle_version": run.get("周期版本"),
            "window_version": run.get("周期版本"),
            "window_id": run.get("窗口ID"),
            "start_date": run.get("窗口开始"),
            "end_date": run.get("窗口结束"),
            "timezone": run.get("时区"),
            "reserved_partitions": reserved,
            "skipped_partitions": skipped,
            "run": run,
            "message": f"调度 {schedule_id} 已受理为旧跑次 {run['运行编号']}",
        }

    def accept_legacy_run(self, schedule_id: int, **kwargs: Any) -> dict[str, Any]:
        return self.accept_schedule(schedule_id, **kwargs)

    # ── 训练输入时段清单 ──
    def build_manifest(self, *, schedule_id: int | None = None,
                       pipeline_id: int | None = None,
                       version: str | None = None,
                       run_id: int | None = None,
                       persist: bool = True,
                       include_claims: bool = True) -> dict[str, Any]:
        window = self._pick_window(schedule_id, pipeline_id, version)
        if window is None:
            return self._legacy_manifest_hint(
                schedule_id=schedule_id, pipeline_id=pipeline_id)
        schedule_id = int(window.get("关联调度") or 0) or schedule_id
        pipeline_id = int(window.get("关联流水线") or 0) or pipeline_id
        field = window.get("分区字段", "dt")
        partitions = list((window.get("分区范围") or {}).get("partitions") or [])
        claims = self._claim_map(pipeline_id) if include_claims else {}
        own_run = store.find(RUN_TABLE, run_id) if run_id else None

        conflicts: list[dict[str, Any]] = []
        slots: list[dict[str, Any]] = []
        available: list[str] = []
        for partition in partitions:
            claim = claims.get(partition)
            slot = {
                "日期": partition.split("=", 1)[-1],
                "分区": partition,
                "周期版本": window.get("周期版本"),
                "时区": window.get("时区"),
            }
            if claim is None:
                slot["状态"] = "可下发"
                available.append(partition)
            elif own_run is not None and str(claim.get("跑次ID")) == str(own_run.get("id")):
                slot["状态"] = "本跑次已占用"
                slot["跑次ID"] = claim.get("跑次ID")
                conflicts.append({
                    "partition": partition,
                    "date": slot["日期"],
                    "owner_run_id": claim.get("跑次ID"),
                    "owner_cycle_version": claim.get("周期版本"),
                    "claim_state": claim.get("state"),
                    "resolution": "本跑次已占用",
                    "assigned_cycle_version": claim.get("周期版本"),
                })
            else:
                slot["状态"] = "冲突未下发"
                slot["跑次ID"] = claim.get("跑次ID")
                slot["占用状态"] = claim.get("state")
                slot["占用周期版本"] = claim.get("周期版本")
                conflicts.append({
                    "partition": partition,
                    "date": slot["日期"],
                    "owner_run_id": claim.get("跑次ID"),
                    "owner_run_code": claim.get("运行编号"),
                    "owner_cycle_version": claim.get("周期版本"),
                    "claim_state": claim.get("state"),
                    "resolution": "由其他跑次占用",
                    "assigned_cycle_version": claim.get("周期版本"),
                })
            slots.append(slot)

        manifest_id = _next_id(store.rows(MANIFEST_TABLE))
        manifest = {
            "ok": True,
            "manifest_id": manifest_id,
            "清单编号": f"MF-{manifest_id:04d}",
            "schedule_id": schedule_id,
            "pipeline_id": pipeline_id,
            "window_id": window.get("id"),
            "window_version": window.get("周期版本"),
            "cycle_version": window.get("周期版本"),
            "cycle_source": window.get("周期来源"),
            "raw_expression": window.get("原表达式"),
            "source_expression": window.get("来源表达式"),
            "timezone": window.get("时区"),
            "timezone_status": window.get("时区状态"),
            "timezone_assumed": window.get("时区按默认值计算", False),
            "pending_items": window.get("待核项") or [],
            "start_date": window.get("开始日期"),
            "end_date": window.get("结束日期"),
            "partition_field": field,
            "partition_range": window.get("分区范围"),
            "输入时段": [{
                "周期版本": window.get("周期版本"),
                "周期来源": window.get("周期来源"),
                "时区": window.get("时区"),
                "开始日期": window.get("开始日期"),
                "结束日期": window.get("结束日期"),
                "分区范围": window.get("分区范围"),
            }],
            "日期分区": slots,
            "可下发分区": available,
            "可下发数量": len(available),
            "冲突分区": conflicts,
            "冲突数量": len(conflicts),
            "状态": "可下发" if available else "无可下发分区",
            "生成时间": datetime.now().isoformat(timespec="seconds"),
        }
        if persist:
            row = dict(manifest)
            row.update({
                "id": manifest_id,
                "status": manifest["状态"],
                "pending": bool(available),
                "abnormal": manifest["timezone_status"] == "待确认",
                "关联流水线": str(pipeline_id or ""),
                "关联调度": str(schedule_id or ""),
                "周期版本": manifest["cycle_version"],
            })
            store.rows(MANIFEST_TABLE).append(row)
            manifest["persisted"] = True
        return manifest

    def generate_manifest(self, *, schedule_id: int | None = None,
                          pipeline_id: int | None = None,
                          version: str | None = None,
                          run_id: int | None = None) -> dict[str, Any]:
        return self.build_manifest(
            schedule_id=schedule_id, pipeline_id=pipeline_id,
            version=version, run_id=run_id)

    def _legacy_manifest_hint(self, *, schedule_id: int | None = None,
                              pipeline_id: int | None = None) -> dict[str, Any]:
        """还没有窗口时，仍然把旧表达式和待核项摊开给调用方确认。"""
        sched = store.find("pipeline_schedule", schedule_id) if schedule_id else None
        pipeline = store.find("train_pipeline", pipeline_id) if pipeline_id else None
        if sched is None and pipeline_id is not None:
            sched = self._schedule_for_pipeline(pipeline_id)
        if sched is not None and sched.get("周期版本"):
            # 调度上写着版本、窗口却找不到，说明数据被清理过，直接提示重建。
            return {
                "ok": False,
                "reason": "window_missing",
                "message": f"调度 {schedule_id or sched.get('id')} 声明的窗口版本 "
                           f"{sched.get('周期版本')} 不存在，请重新迁移",
                "schedule_id": schedule_id or sched.get("id"),
                "pipeline_id": pipeline_id,
                "cycle_version": sched.get("周期版本"),
            }
        raw = ""
        timezone = None
        if sched is not None:
            raw = str(sched.get("Cron表达式") or "").strip()
            timezone = sched.get("时区") or None
        if not raw and pipeline is not None:
            raw = str(pipeline.get("训练周期") or "").strip()
            timezone = timezone or pipeline.get("时区") or None
        parsed = parse_legacy_cycle(raw, timezone=timezone)
        return {
            "ok": False,
            "reason": "window_not_migrated",
            "message": "旧训练周期尚未迁移成训练窗口，可直接调用迁移接口",
            "schedule_id": schedule_id or (sched or {}).get("id"),
            "pipeline_id": pipeline_id,
            "raw_expression": parsed["raw"],
            "expression": parsed["expression"],
            "legacy_cadence": parsed["cadence"],
            "timezone": parsed["timezone"],
            "timezone_status": parsed["timezone_status"],
            "pending_items": parsed["pending"],
            "errors": parsed["errors"],
        }

    def _claim_map(self, pipeline_id: int | None) -> dict[str, dict[str, Any]]:
        result: dict[str, dict[str, Any]] = {}
        for claim in store.rows(CLAIM_TABLE):
            if claim.get("state") not in ACTIVE_CLAIM_STATES:
                continue
            if pipeline_id is not None and str(claim.get("关联流水线")) != str(pipeline_id):
                continue
            result[str(claim.get("分区"))] = claim
        return result

    # ── 下发 / 训练 ──
    def dispatch_manifest(self, *, schedule_id: int | None = None,
                          pipeline_id: int | None = None,
                          version: str | None = None,
                          run_id: int | None = None,
                          partitions: Iterable[str] | None = None,
                          manifest_id: int | None = None) -> dict[str, Any]:
        """按训练输入时段清单逐日期分区下发；同一分区只允许一个有效占用。"""
        window = self._pick_window(schedule_id, pipeline_id, version)
        if window is None:
            return self._legacy_manifest_hint(
                schedule_id=schedule_id, pipeline_id=pipeline_id)
        schedule_id = int(window.get("关联调度") or 0) or schedule_id
        pipeline_id = int(window.get("关联流水线") or 0) or pipeline_id
        window_partitions = list(
            (window.get("分区范围") or {}).get("partitions") or [])
        requested = list(partitions) if partitions is not None else list(window_partitions)
        manifest = self.build_manifest(
            schedule_id=schedule_id, pipeline_id=pipeline_id,
            version=window.get("周期版本"), run_id=run_id,
        )
        if manifest_id is not None:
            manifest["manifest_id"] = manifest_id
        own_run = store.find(RUN_TABLE, run_id) if run_id else None
        if own_run is not None and str(own_run.get("周期版本")) != str(window.get("周期版本")):
            return {
                "ok": False,
                "message": "跑次所属周期版本与待下发窗口不一致，历史跑次只读回放",
                "run_id": own_run.get("id"),
                "run_cycle_version": own_run.get("周期版本"),
                "window_version": window.get("周期版本"),
            }

        deliveries: list[dict[str, Any]] = []
        skipped: list[dict[str, Any]] = []
        claims = self._claim_map(pipeline_id)
        for partition in requested:
            if partition not in window_partitions:
                skipped.append({
                    "partition": partition,
                    "date": partition.split("=", 1)[-1],
                    "reason": "分区不在本周期窗口范围内",
                    "run_id": None,
                    "cycle_version": window.get("周期版本"),
                })
                continue
            claim = claims.get(partition)
            if claim is not None:
                if own_run is not None and str(claim.get("跑次ID")) == str(own_run.get("id")):
                    skipped.append({
                        "partition": partition,
                        "date": partition.split("=", 1)[-1],
                        "reason": "本跑次已占用，幂等跳过",
                        "run_id": own_run.get("id"),
                        "cycle_version": claim.get("周期版本"),
                    })
                else:
                    skipped.append({
                        "partition": partition,
                        "date": partition.split("=", 1)[-1],
                        "reason": "分区已被其他跑次占用，换代期间不重复触发",
                        "run_id": claim.get("跑次ID"),
                        "cycle_version": claim.get("周期版本"),
                    })
                continue
            if own_run is None:
                # 只有确实有分区可下发时才落一次新跑次，空清单不制造空跑次。
                own_run = self._create_run(
                    window, status="已下发", accepted_at=None,
                    source="manifest_dispatch")
            claim = self._add_claim(own_run, window, partition)
            claims[partition] = claim
            deliveries.append({
                "date": partition.split("=", 1)[-1],
                "partition": partition,
                "run_id": own_run.get("id"),
                "run_code": own_run.get("运行编号"),
                "cycle_version": window.get("周期版本"),
                "cycle_source": window.get("周期来源"),
                "timezone": window.get("时区"),
                "window_id": window.get("id"),
                "manifest_id": manifest.get("manifest_id"),
            })
        if own_run is None:
            return {
                "ok": True,
                "manifest_id": manifest.get("manifest_id"),
                "清单编号": manifest.get("清单编号"),
                "schedule_id": schedule_id,
                "pipeline_id": pipeline_id,
                "run_id": None,
                "run_code": None,
                "cycle_version": window.get("周期版本"),
                "cycle_source": window.get("周期来源"),
                "timezone": window.get("时区"),
                "timezone_status": window.get("时区状态"),
                "start_date": window.get("开始日期"),
                "end_date": window.get("结束日期"),
                "partition_range": window.get("分区范围"),
                "dispatched": [],
                "dispatched_partitions": [],
                "skipped": skipped,
                "skipped_count": len(skipped),
                "manifest_available": manifest.get("可下发分区"),
                "manifest_conflicts": manifest.get("冲突分区"),
                "message": (
                    f"清单 {manifest.get('清单编号')} 没有可下发的日期分区，"
                    f"跳过 {len(skipped)} 个已占用分区"),
            }

        run_partitions = list(own_run.get("下发分区") or [])
        for item in deliveries:
            if item["partition"] not in run_partitions:
                run_partitions.append(item["partition"])
        own_run["下发分区"] = run_partitions
        own_run["分区范围"] = _partition_range(run_partitions, window.get("分区字段", "dt"))
        own_run["清单编号"] = manifest.get("清单编号")
        own_run["更新时间"] = datetime.now().isoformat(timespec="seconds")
        if deliveries:
            own_run["status"] = "已下发"
            own_run["pending"] = True

        # 清单实例同步回填实际下发结果。
        for row in store.rows(MANIFEST_TABLE):
            if int(row.get("id", 0)) == int(manifest.get("manifest_id", -1)):
                row["已下发分区"] = [item["partition"] for item in deliveries]
                row["下发明细"] = deliveries
                row["下发跑次"] = own_run.get("id")
                row["下发时间"] = datetime.now().isoformat(timespec="seconds")
                row["状态"] = "已下发" if deliveries else "无可下发分区"
                break

        return {
            "ok": True,
            "manifest_id": manifest.get("manifest_id"),
            "清单编号": manifest.get("清单编号"),
            "schedule_id": schedule_id,
            "pipeline_id": pipeline_id,
            "run_id": own_run.get("id"),
            "run_code": own_run.get("运行编号"),
            "cycle_version": window.get("周期版本"),
            "cycle_source": window.get("周期来源"),
            "timezone": window.get("时区"),
            "timezone_status": window.get("时区状态"),
            "start_date": window.get("开始日期"),
            "end_date": window.get("结束日期"),
            "partition_range": window.get("分区范围"),
            "dispatched": deliveries,
            "dispatched_partitions": [item["partition"] for item in deliveries],
            "skipped": skipped,
            "skipped_count": len(skipped),
            "manifest_available": manifest.get("可下发分区"),
            "manifest_conflicts": manifest.get("冲突分区"),
            "message": (
                f"已按清单 {manifest.get('清单编号')} 下发 {len(deliveries)} 个日期分区"
                + (f"，跳过 {len(skipped)} 个已占用分区" if skipped else "")
            ),
        }

    def dispatch_schedule(self, schedule_id: int, **kwargs: Any) -> dict[str, Any]:
        return self.dispatch_manifest(schedule_id=schedule_id, **kwargs)

    def start_training(self, *, schedule_id: int | None = None,
                       pipeline_id: int | None = None,
                       version: str | None = None,
                       partitions: Iterable[str] | None = None,
                       run_id: int | None = None,
                       policy: str = POLICY_CARRY_OVER,
                       auto_migrate: bool = True) -> dict[str, Any]:
        """发起一次新版窗口的训练：下发 -> 消费 -> 产出带分区快照的模型。"""
        if run_id is not None:
            return self._train_existing_run(run_id, partitions=partitions)
        window = self._pick_window(schedule_id, pipeline_id, version)
        schedule_missing = False
        if schedule_id is not None and store.find("pipeline_schedule", schedule_id) is None:
            schedule_missing = True
        if window is None and auto_migrate and not schedule_missing:
            target_schedule = schedule_id
            if target_schedule is None and pipeline_id is not None:
                sched = self._schedule_for_pipeline(pipeline_id)
                target_schedule = int(sched["id"]) if sched else None
            if target_schedule is not None:
                migrated = self.migrate_schedule(target_schedule, policy=policy)
                if migrated.get("ok"):
                    window = self.find_window(int(migrated["window_id"]))
        if window is None:
            hint = self._legacy_manifest_hint(
                schedule_id=schedule_id, pipeline_id=pipeline_id)
            hint["message"] = "没有可用的训练窗口，请先迁移旧训练周期"
            return hint
        schedule_id = int(window.get("关联调度") or 0) or schedule_id
        pipeline_id = int(window.get("关联流水线") or 0) or pipeline_id

        dispatch = self.dispatch_manifest(
            schedule_id=schedule_id, pipeline_id=pipeline_id,
            version=window.get("周期版本"), partitions=partitions,
        )
        run_id = dispatch.get("run_id")
        delivered = list(dispatch.get("dispatched_partitions") or [])
        if not delivered:
            return {
                "ok": False,
                "message": "首批训练没有可下发的日期分区，重合分区仍由旧跑次占用",
                "schedule_id": schedule_id,
                "pipeline_id": pipeline_id,
                "run_id": run_id,
                "cycle_version": window.get("周期版本"),
                "blocked": True,
                "manifest": dispatch,
            }
        consumed = self.consume_run_partitions(run_id, partitions=delivered)
        model = self._create_model_artifact(
            pipeline_id, schedule_id, window, run_id, delivered,
            manifest_id=dispatch.get("manifest_id"),
        )
        run = store.find(RUN_TABLE, run_id) or {}
        run["status"] = "已完成"
        run["pending"] = False
        run["模型编号"] = str(model.get("id"))
        run["完成时间"] = datetime.now().isoformat(timespec="seconds")
        pipeline = store.find("train_pipeline", pipeline_id)
        if pipeline is not None:
            pipeline["status"] = "已完成"
            pipeline["pending"] = False
            pipeline["当前周期版本"] = window.get("周期版本")
            pipeline["最近运行编号"] = run.get("运行编号")
        return {
            "ok": True,
            "schedule_id": schedule_id,
            "pipeline_id": pipeline_id,
            "run_id": run_id,
            "run_code": run.get("运行编号"),
            "model_id": model.get("id"),
            "model_code": model.get("模型编号"),
            "cycle_version": window.get("周期版本"),
            "cycle_source": window.get("周期来源"),
            "timezone": window.get("时区"),
            "start_date": window.get("开始日期"),
            "end_date": window.get("结束日期"),
            "partitions": delivered,
            "partition_range": _partition_range(delivered, window.get("分区字段", "dt")),
            "consumed": consumed,
            "manifest_id": dispatch.get("manifest_id"),
            "skipped": dispatch.get("skipped"),
            "message": (
                f"流水线 {pipeline_id} 已按 {window.get('周期版本')} 窗口读取 "
                f"{len(delivered)} 个日期分区并产出模型 {model.get('模型编号')}"
            ),
        }

    def train_from_window(self, pipeline_id: int, **kwargs: Any) -> dict[str, Any]:
        return self.start_training(pipeline_id=pipeline_id, **kwargs)

    def _train_existing_run(self, run_id: int,
                            partitions: Iterable[str] | None = None) -> dict[str, Any]:
        """用已下发的跑次继续训练；只读取本跑次占用的分区。"""
        run = store.find(RUN_TABLE, run_id)
        if run is None:
            return {"ok": False, "message": f"跑次 {run_id} 不存在"}
        window = self.find_window(int(run.get("窗口ID") or 0))
        if window is None:
            return {"ok": False, "message": f"跑次 {run_id} 找不到对应的训练窗口"}
        schedule_id = int(float(str(run.get("关联调度") or 0) or 0))
        pipeline_id = int(float(str(run.get("关联流水线") or 0) or 0))
        if run.get("status") == "已完成":
            return {
                "ok": False,
                "message": f"跑次 {run.get('运行编号')} 已完成训练，不能重复消费",
                "run_id": run_id,
                "models": [run.get("模型编号")],
            }
        delivered = list(run.get("下发分区") or [])
        if partitions is not None:
            requested = [item for item in partitions if item in delivered]
        else:
            requested = delivered
        consumed = self.consume_run_partitions(run_id, partitions=requested)
        model = self._create_model_artifact(
            pipeline_id, schedule_id, window, run_id, requested,
            manifest_id=run.get("清单编号"))
        run["status"] = "已完成"
        run["pending"] = False
        run["模型编号"] = str(model.get("id"))
        run["完成时间"] = datetime.now().isoformat(timespec="seconds")
        pipeline = store.find("train_pipeline", pipeline_id)
        if pipeline is not None:
            pipeline["status"] = "已完成"
            pipeline["pending"] = False
        return {
            "ok": True,
            "schedule_id": schedule_id,
            "pipeline_id": pipeline_id,
            "run_id": run_id,
            "run_code": run.get("运行编号"),
            "model_id": model.get("id"),
            "model_code": model.get("模型编号"),
            "cycle_version": window.get("周期版本"),
            "cycle_source": window.get("周期来源"),
            "timezone": window.get("时区"),
            "start_date": window.get("开始日期"),
            "end_date": window.get("结束日期"),
            "partitions": requested,
            "dispatched": [],
            "dispatched_partitions": requested,
            "partition_range": _partition_range(requested, window.get("分区字段", "dt")),
            "consumed": consumed,
            "manifest_id": run.get("清单编号"),
            "message": (
                f"跑次 {run.get('运行编号')} 已按 {window.get('周期版本')} 窗口训练 "
                f"{len(requested)} 个日期分区并产出模型 {model.get('模型编号')}"
            ),
        }

    def consume_run_partitions(self, run_id: int, *,
                               partitions: Iterable[str] | None = None,
                               through: str | date | None = None) -> dict[str, Any]:
        run = store.find(RUN_TABLE, run_id)
        if run is None:
            return {"ok": False, "message": f"跑次 {run_id} 不存在"}
        requested = set(partitions or [])
        through_date = _parse_date(through)
        consumed_now: list[str] = []
        for claim in store.rows(CLAIM_TABLE):
            if str(claim.get("跑次ID")) != str(run_id):
                continue
            if claim.get("state") != "reserved":
                continue
            partition = str(claim.get("分区"))
            claim_date = _parse_date(partition.split("=", 1)[-1])
            if requested and partition not in requested:
                continue
            if through_date and claim_date and claim_date > through_date:
                continue
            claim["state"] = "consumed"
            claim["消费时间"] = datetime.now().isoformat(timespec="seconds")
            consumed_now.append(partition)
        consumed = list(run.get("已消费分区") or [])
        for partition in consumed_now:
            if partition not in consumed:
                consumed.append(partition)
        run["已消费分区"] = consumed
        run["消费分区范围"] = _partition_range(consumed)
        run["状态更新"] = "部分消费"
        run["更新时间"] = datetime.now().isoformat(timespec="seconds")
        return {
            "ok": True,
            "run_id": run_id,
            "consumed_now": consumed_now,
            "consumed_partitions": consumed,
            "remaining_reserved": list(run.get("预留分区") or []),
        }

    def _create_model_artifact(self, pipeline_id: int | None, schedule_id: int | None,
                               window: dict[str, Any], run_id: int,
                               partitions: list[str],
                               manifest_id: int | None = None) -> dict[str, Any]:
        pipeline = store.find("train_pipeline", pipeline_id) if pipeline_id else None
        run = store.find(RUN_TABLE, run_id) or {}
        rows = store.rows("model_registry")
        model_id = _next_id(rows)
        now = datetime.now().isoformat(timespec="seconds")
        snapshot = {
            "run_id": run_id,
            "run_code": run.get("运行编号"),
            "schedule_id": schedule_id,
            "pipeline_id": pipeline_id,
            "window_id": window.get("id"),
            "window_number": window.get("窗口编号"),
            "cycle_version": window.get("周期版本"),
            "cycle_source": window.get("周期来源"),
            "source_expression": window.get("来源表达式"),
            "raw_expression": window.get("原表达式"),
            "timezone": window.get("时区"),
            "timezone_status": window.get("时区状态"),
            "window_start": window.get("开始日期"),
            "window_end": window.get("结束日期"),
            "partitions": list(partitions),
            "partition_range": _partition_range(partitions, window.get("分区字段", "dt")),
            "manifest_id": manifest_id,
            "trained_at": now,
        }
        model = {
            "id": model_id,
            "status": "草稿",
            "pending": True,
            "abnormal": window.get("时区状态") == "待确认",
            "模型编号": f"MODEL-{model_id:04d}",
            "模型名称": f"model_{(pipeline or {}).get('流水线名称', 'unknown')}_v{model_id}",
            "算法类型": (pipeline or {}).get("算法类型", "xgboost"),
            "训练特征": (pipeline or {}).get("特征集", ""),
            "模型版本": (pipeline or {}).get("流水线版本", "v1.0.0"),
            "模型文件": f"models/pipeline_{pipeline_id}_run_{run_id}.pkl",
            "评估分数": "0.892",
            "部署状态": "未部署",
            "关联流水线": str(pipeline_id or ""),
            "运行编号": run.get("运行编号"),
            "调度编号": str(schedule_id or ""),
            "窗口编号": window.get("窗口编号"),
            "周期版本": window.get("周期版本"),
            "周期来源": window.get("周期来源"),
            "原表达式": window.get("原表达式"),
            "时区": window.get("时区"),
            "训练分区": list(partitions),
            "分区范围": snapshot["partition_range"],
            "起止日期": f"{window.get('开始日期')} ~ {window.get('结束日期')}",
            "训练开始": run.get("受理时间") or now,
            "训练完成": now,
            "训练输入快照": snapshot,
        }
        rows.append(model)
        run["模型编号"] = str(model_id)
        run["模型快照"] = snapshot
        return model

    # ── 回放 ──
    def replay_model(self, model_id: int) -> dict[str, Any]:
        """按模型产物里冻结的快照回放原分区和原时间范围，只读、不占用分区。"""
        model = store.find("model_registry", model_id)
        if model is None:
            return {"ok": False, "message": f"模型 {model_id} 不存在"}
        snapshot = model.get("训练输入快照")
        if not isinstance(snapshot, dict) or not snapshot.get("partitions"):
            run_code = model.get("运行编号")
            run = next((row for row in store.rows(RUN_TABLE)
                        if str(row.get("运行编号")) == str(run_code)), None)
            if run is not None:
                snapshot = {
                    "run_id": run.get("id"),
                    "run_code": run.get("运行编号"),
                    "schedule_id": int(float(str(run.get("关联调度") or 0))),
                    "pipeline_id": int(float(str(run.get("关联流水线") or 0))),
                    "window_id": run.get("窗口ID"),
                    "cycle_version": run.get("周期版本"),
                    "cycle_source": run.get("周期来源"),
                    "raw_expression": run.get("原表达式"),
                    "timezone": run.get("时区"),
                    "timezone_status": run.get("时区状态"),
                    "window_start": run.get("窗口开始"),
                    "window_end": run.get("窗口结束"),
                    "partitions": run.get("已消费分区") or run.get("下发分区") or [],
                }
        if not isinstance(snapshot, dict) or not snapshot.get("partitions"):
            return {
                "ok": False,
                "message": f"模型 {model_id} 没有可回放的训练分区快照",
                "model_id": model_id,
            }
        schedule_id = snapshot.get("schedule_id")
        sched = store.find("pipeline_schedule", int(schedule_id)) if schedule_id else None
        run_id = snapshot.get("run_id")
        run = store.find(RUN_TABLE, int(run_id)) if run_id else None
        partitions = list(snapshot.get("partitions") or [])
        return {
            "ok": True,
            "mode": "read_only",
            "replay": {
                "read_only": True,
                "reuses_partition_claims": False,
                "replayed_at": datetime.now().isoformat(timespec="seconds"),
            },
            "model_id": model_id,
            "model_code": model.get("模型编号"),
            "run_id": run_id,
            "run_code": snapshot.get("run_code") or model.get("运行编号"),
            "run_status": (run or {}).get("status"),
            "pipeline_id": snapshot.get("pipeline_id"),
            "schedule_id": schedule_id,
            "window_id": snapshot.get("window_id"),
            "window_number": snapshot.get("window_number"),
            "cycle_version": snapshot.get("cycle_version"),
            "cycle_source": snapshot.get("cycle_source"),
            "raw_expression": snapshot.get("raw_expression"),
            "source_expression": snapshot.get("source_expression"),
            "timezone": snapshot.get("timezone"),
            "timezone_status": snapshot.get("timezone_status"),
            "start_date": snapshot.get("window_start"),
            "end_date": snapshot.get("window_end"),
            "partitions": partitions,
            "partition_range": snapshot.get("partition_range") or _partition_range(partitions),
            "manifest_id": snapshot.get("manifest_id"),
            "trained_at": snapshot.get("trained_at"),
            "schedule_link": {
                "schedule_id": schedule_id,
                "cycle_version": snapshot.get("cycle_version"),
                "window_id": snapshot.get("window_id"),
                "current_cycle_version": (sched or {}).get("周期版本"),
                "current_window_id": (sched or {}).get("窗口编号"),
                "cycle_changed": (
                    (sched or {}).get("周期版本") is not None
                    and str((sched or {}).get("周期版本")) != str(snapshot.get("cycle_version"))
                ),
            },
        }

    def replay_run(self, run_id: int) -> dict[str, Any]:
        run = store.find(RUN_TABLE, run_id)
        if run is None:
            return {"ok": False, "message": f"跑次 {run_id} 不存在"}
        schedule_id = int(float(str(run.get("关联调度") or 0) or 0))
        sched = store.find("pipeline_schedule", schedule_id) if schedule_id else None
        # 已消费分区优先；一个分区都没消费时（刚受理就被换代）回放原计划分区。
        consumed = list(run.get("已消费分区") or [])
        partitions = consumed or list(
            run.get("原计划分区") or run.get("下发分区") or [])
        return {
            "ok": True,
            "mode": "read_only",
            "replay": {"read_only": True, "reuses_partition_claims": False},
            "run_id": run_id,
            "run_code": run.get("运行编号"),
            "run_status": run.get("status"),
            "pipeline_id": run.get("关联流水线"),
            "schedule_id": schedule_id,
            "window_id": run.get("窗口ID"),
            "cycle_version": run.get("周期版本"),
            "cycle_source": run.get("周期来源"),
            "raw_expression": run.get("原表达式"),
            "timezone": run.get("时区"),
            "start_date": run.get("窗口开始"),
            "end_date": run.get("窗口结束"),
            "partitions": partitions,
            "consumed_partitions": consumed,
            "planned_partitions": run.get("原计划分区") or [],
            "partition_range": _partition_range(partitions),
            "partition_scope": (
                "已消费分区" if consumed else "原计划分区（未消费）"
            ),
            "schedule_link": {
                "schedule_id": schedule_id,
                "cycle_version": run.get("周期版本"),
                "current_cycle_version": (sched or {}).get("周期版本"),
            },
        }

    # ── 内部辅助 ──
    def _pick_window(self, schedule_id: int | None, pipeline_id: int | None,
                     version: str | None) -> dict[str, Any] | None:
        if version:
            return self.find_window_version(
                version, schedule_id=schedule_id, pipeline_id=pipeline_id)
        window = self.current_window(schedule_id=schedule_id, pipeline_id=pipeline_id)
        if window is not None:
            return window
        if pipeline_id is not None:
            sched = self._schedule_for_pipeline(pipeline_id)
            if sched is not None:
                return self.current_window(schedule_id=int(sched["id"]))
        return None

    @staticmethod
    def _version_number(value: Any) -> int:
        match = re.fullmatch(r"v(\d+)", str(value or "").strip())
        return int(match.group(1)) if match else 0

    def _pipeline_id(self, sched: dict[str, Any]) -> int | None:
        try:
            return int(float(str(sched.get("关联流水线") or 0)))
        except (TypeError, ValueError):
            return None

    def _schedule_for_pipeline(self, pipeline_id: int) -> dict[str, Any] | None:
        matches = [
            row for row in store.rows("pipeline_schedule")
            if str(row.get("关联流水线")) == str(pipeline_id)
        ]
        return matches[0] if matches else None

    def _create_legacy_schedule(self, pipeline: dict[str, Any]) -> dict[str, Any]:
        rows = store.rows("pipeline_schedule")
        next_id = _next_id(rows)
        row = {
            "id": next_id,
            "status": "草稿",
            "pending": True,
            "abnormal": False,
            "调度编号": f"SCH-{next_id:04d}",
            "关联流水线": str(pipeline.get("id")),
            "Cron表达式": str(pipeline.get("训练周期") or ""),
            "依赖任务": "",
            "失败策略": "retry3",
            "通知方式": "email",
            "最大重试数": "3",
            "调度状态": "草稿",
        }
        rows.append(row)
        return row

    def _active_runs(self, *, schedule_id: int | None = None,
                     pipeline_id: int | None = None) -> list[dict[str, Any]]:
        runs = self.list_runs(schedule_id=schedule_id, pipeline_id=pipeline_id)
        return [run for run in runs if run.get("status") not in ("已作废", "已回放")]

    def _reserve(self, run: dict[str, Any], partitions: Iterable[str],
                 window: dict[str, Any]) -> tuple[list[str], list[dict[str, Any]]]:
        reserved: list[str] = []
        skipped: list[dict[str, Any]] = []
        claims = self._claim_map(int(window.get("关联流水线") or 0) or None)
        for partition in partitions:
            claim = claims.get(partition)
            if claim is not None:
                skipped.append({
                    "partition": partition,
                    "date": partition.split("=", 1)[-1],
                    "reason": "分区已被其他跑次占用",
                    "run_id": claim.get("跑次ID"),
                    "cycle_version": claim.get("周期版本"),
                })
                continue
            claim = self._add_claim(run, window, partition)
            claims[partition] = claim
            reserved.append(partition)
        return reserved, skipped

    def _add_claim(self, run: dict[str, Any], window: dict[str, Any],
                   partition: str) -> dict[str, Any]:
        rows = store.rows(CLAIM_TABLE)
        claim = {
            "id": _next_id(rows),
            "status": "reserved",
            "pending": True,
            "abnormal": False,
            "分区": partition,
            "关联流水线": str(window.get("关联流水线") or ""),
            "关联调度": str(window.get("关联调度") or ""),
            "跑次ID": run.get("id"),
            "运行编号": run.get("运行编号"),
            "窗口ID": window.get("id"),
            "窗口编号": window.get("窗口编号"),
            "周期版本": window.get("周期版本"),
            "state": "reserved",
            "占用时间": datetime.now().isoformat(timespec="seconds"),
        }
        rows.append(claim)
        return claim

    def _create_run(self, window: dict[str, Any], *, status: str,
                    accepted_at: datetime | None, source: str) -> dict[str, Any]:
        rows = store.rows(RUN_TABLE)
        run_id = _next_id(rows)
        now = (accepted_at or datetime.now()).isoformat()
        run = {
            "id": run_id,
            "status": status,
            "pending": True,
            "abnormal": False,
            "运行编号": f"RUN-{run_id:04d}",
            "关联流水线": window.get("关联流水线"),
            "关联调度": window.get("关联调度"),
            "窗口ID": window.get("id"),
            "窗口编号": window.get("窗口编号"),
            "周期版本": window.get("周期版本"),
            "周期来源": window.get("周期来源"),
            "来源表达式": window.get("来源表达式"),
            "原表达式": window.get("原表达式"),
            "时区": window.get("时区"),
            "时区状态": window.get("时区状态"),
            "窗口开始": window.get("开始日期"),
            "窗口结束": window.get("结束日期"),
            "受理时间": now,
            "受理方式": source,
            "预留分区": [],
            "下发分区": [],
            "已消费分区": [],
            "原计划分区": list((window.get("分区范围") or {}).get("partitions") or []),
            "模型编号": "",
            "待核项": window.get("待核项") or [],
        }
        rows.append(run)
        return run

    # 兼容常见的调用命名。
    migrate_training_windows = migrate_schedule
    generate_training_input_manifest = generate_manifest
    dispatch_training_inputs = dispatch_manifest
    start_training_run = start_training
    replay_model_training_window = replay_model


training_windows = TrainingWindowService()
