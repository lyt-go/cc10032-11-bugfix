"""late_data 领域服务：迟到数据处理策略。

业务逻辑：
1. 迟到数据判定：基于watermark的当前水位和允许延迟阈值
2. 迟到处理链路：watermark → late_data → 侧输出/重定向主题
3. 失败场景：丢弃阈值超限、重定向主题不存在、侧输出路径不可写
4. 与上下游流水线：消费 watermark 水位；输出被判定为迟到的数据行

迟到处理按组核对（本次修复）：
- 判定入口按 (关联源, 消费组) 读取本组确认位置，不再读到其他组的分区进度；
- 判定结论写入本组迟到判定清单，绑定来源、分区和消费组；
- 未指定消费组时回退历史无组标识水位（兼容旧数据），结论标记为未归组。

跨模块数据流：
  watermark → late_data（水位决定允许延迟窗口）
  late_data → 侧输出/重定向（迟到数据路由到专门主题）
"""
from __future__ import annotations

import time
from typing import Any

from app.services.base import BaseModuleService
from app.store import store


class LateDataService(BaseModuleService):
    MODULE = "late_data"
    ENTITY = "迟到数据策略"
    OPERATION_TERM = "关联"
    LIST_FIELDS = ["策略编号", "关联源", "允许延迟秒数", "处理方式", "侧输出路径", "重定向主题", "丢弃阈值", "最后更新"]
    REQUIRED_FIELDS = ["策略编号", "关联源", "允许延迟秒数"]
    STATUS_ORDER = ["草稿", "待验证", "已生效", "已停用"]
    ACTION_RULES = {"提交验证": "待验证", "标记生效": "已生效", "停用策略": "已停用"}
    NEGATIVE_ACTIONS = ["停用策略"]

    PIPELINE = "late_data_handler"
    UPSTREAM_MODULES = ["watermark"]
    DOWNSTREAM_MODULES = []
    PRODUCES_ARTIFACT = "迟到数据记录"

    def classify_late_events(
        self, entry_id: int, events: list[dict[str, Any]], group: str | None = None
    ) -> dict[str, Any]:
        """判定事件是否迟到：按消费组读取本组确认位置，基于允许延迟窗口判定。

        失败场景：
        - 超出丢弃阈值 → 直接移除
        - 侧输出路径不存在 → 写入失败
        - 重定向主题未配置 → 无法路由
        - 未指定消费组 → 兼容历史无组标识水位，结论标记为未归组
        """
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return {"ok": False, "reason": f"策略 {entry_id} 不存在"}
        if entry.get("status") != "已生效":
            return {"ok": False, "reason": f"策略未生效，当前状态: {entry.get('status')}"}

        allowed_lag = float(str(entry.get("允许延迟秒数", "30")))
        discard_threshold = float(str(entry.get("丢弃阈值", "3600")))
        source_id = str(entry.get("关联源", ""))
        group_name = str(group).strip() if group else ""

        # 消费上游watermark：按组核对本组确认位置，避免读到其他组进度
        if group_name:
            wm_info = store.get_watermark(source_id, group_name) or {}
        else:
            wm_info = store.get_watermark(source_id) or {}
        raw_offset = wm_info.get("offset")
        offset_known = isinstance(raw_offset, (int, float))
        current_offset = raw_offset if offset_known else time.time()

        on_time = []
        late = []
        discarded = []

        for event in events:
            event_pos = event.get("position", event.get("ts", time.time()))
            # 有符号延迟：事件落后于确认位置为正（迟到）；尚未到期为负（准时）
            lag = current_offset - event_pos
            if lag <= allowed_lag:
                on_time.append(event)
            elif lag <= discard_threshold:
                late.append({"event": event, "lag": round(lag, 1), "redirect": entry.get("重定向主题", "")})
            else:
                discarded.append({"event": event, "lag": round(lag, 1), "reason": f"超出丢弃阈值 {discard_threshold}s"})

        # 写入到流水线
        batch_id = f"LATE_{entry_id}_{int(time.time())}"
        if late:
            self.produce_artifact(late, batch_id)

        # 判定结论同步到本组迟到判定清单（绑定来源、分区、消费组）
        if group_name:
            store.append_late_determination({
                "kind": "迟到判定",
                "来源": source_id,
                "分区标识": source_id,
                "消费组": group_name,
                "确认位点": current_offset if offset_known else None,
                "依据": str(wm_info.get("basis") or ("消费提交" if offset_known else "无本组水位记录")),
                "详情": f"准时 {len(on_time)} / 迟到 {len(late)} / 丢弃 {len(discarded)}",
            })

        entry["最后更新"] = time.strftime("%Y-%m-%d %H:%M:%S")

        return {
            "ok": True,
            "entry_id": entry_id,
            "source": source_id,
            "消费组": group_name or None,
            "attributed": bool(group_name and offset_known),
            "allowed_lag": allowed_lag,
            "discard_threshold": discard_threshold,
            "on_time": len(on_time),
            "late": len(late),
            "discarded": len(discarded),
            "late_samples": late[:3],
            "discarded_samples": discarded[:3],
        }

    def group_determinations(self, group: str, partition: str | None = None) -> dict[str, Any]:
        """迟到判定清单按组核对：只返回本组的提交同步与判定记录。"""
        group_name = str(group).strip()
        records = store.late_determinations_for(group_name, partition)
        return {"ok": True, "消费组": group_name, "count": len(records), "清单": records}

    def domain_snapshot(self) -> dict[str, Any]:
        base = super().domain_snapshot()
        base["active_policies"] = sum(1 for r in store.rows(self.MODULE) if r.get("status") == "已生效")
        base["determinations_tracked"] = len(store._late_determinations)
        return base
