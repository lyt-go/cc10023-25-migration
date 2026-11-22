"""内存数据仓库：给每个业务模块准备一份可筛选、可流转的示例数据。

真实项目里这里会换成数据库访问层；当前实现只依赖标准库，保证克隆下来就能起。
"""
from __future__ import annotations

import copy
from typing import Any

from app.seed import SEED_ROWS


# 训练窗口的明细表由领域服务按需写入，不参与运营概览的业务模块计数。
INTERNAL_TABLES = {
    "training_window",
    "training_window_period",
    "training_run",
    "partition_claim",
    "training_manifest",
}


class Store:
    def __init__(self) -> None:
        self.reset()

    def module_names(self) -> list[str]:
        return sorted(self._tables)

    def business_module_names(self) -> list[str]:
        """运营概览只统计业务模块，内部明细表单独查询。"""
        return [name for name in self.module_names() if name not in INTERNAL_TABLES]

    def rows(self, module: str) -> list[dict[str, Any]]:
        return self._tables.setdefault(module, [])

    def reset(self) -> None:
        """恢复示例数据；集成测试用它隔离各条链路对内存状态的修改。"""
        self._tables = copy.deepcopy(SEED_ROWS)

    def find(self, module: str, entry_id: int) -> dict[str, Any] | None:
        for row in self.rows(module):
            if int(row.get("id", 0)) == entry_id:
                return row
        return None

    def overview(self) -> dict[str, object]:
        modules: list[dict[str, object]] = []
        for name in self.business_module_names():
            rows = self.rows(name)
            modules.append({
                "name": name,
                "created": len(rows),
                "pending": sum(1 for row in rows if row.get("pending")),
                "abnormal": sum(1 for row in rows if row.get("abnormal")),
            })
        cards = [
            {"label": "业务模块", "value": len(modules)},
            {"label": "今日新增", "value": sum(int(item["created"]) for item in modules)},
            {"label": "待处理", "value": sum(int(item["pending"]) for item in modules)},
            {"label": "异常量", "value": sum(int(item["abnormal"]) for item in modules)},
        ]
        return {"cards": cards, "modules": modules}


store = Store()
