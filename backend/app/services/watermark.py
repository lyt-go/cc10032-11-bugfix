"""watermark 领域服务：消费水位管理。

业务逻辑：
1. 消费水位追踪：分区偏移量、延迟秒数、积压消息数
2. 水位管理链路：watermark → late_data（水位决定延迟阈值）
3. 失败场景：水位重置冲突、heartbeat丢失检测、延迟告警
4. 与上下游流水线：消费 stream_source 的分区信息；为 late_data 提供水位参数

消费位置归组（本次修复）：
- 共享水位按 (分区标识, 消费组) 归属：一组提交不再覆盖另一组进度，
  迟到处理入口也不会再读到其他组的分区进度；
- 进度列表与迟到判定按组核对：各组只能查看/使用自己的确认位置；
- 历史无组标识记录保留兼容读取，但不构成任何组的推进依据；
  须值班员发起认领申请、凭独立确证归属后才允许推进，
  共享分区存在多个合理认领方时按权限保留待确认原因。

跨模块数据流：
  stream_source → watermark（获取分区和消费进度）
  watermark → late_data（水位决定延迟数据的判定阈值）
"""
from __future__ import annotations

import time
from typing import Any

from app.services.base import BaseModuleService
from app.store import store


class WatermarkService(BaseModuleService):
    MODULE = "watermark"
    ENTITY = "消费水位"
    OPERATION_TERM = "水位"
    LIST_FIELDS = ["分区标识", "消费组", "当前位点", "提交时间", "最大事件时间", "延迟秒数", "积压消息数", "上次心跳"]
    REQUIRED_FIELDS = ["分区标识", "消费组", "当前位点"]
    STATUS_ORDER = ["正常", "延迟", "积压", "已重置"]
    ACTION_RULES = {"标记延迟": "延迟", "标记积压": "积压", "重置水位": "已重置"}
    NEGATIVE_ACTIONS = []

    PIPELINE = "watermark_management"
    UPSTREAM_MODULES = ["stream_source"]
    DOWNSTREAM_MODULES = ["late_data"]
    PRODUCES_ARTIFACT = "水位偏移快照"

    # 可直接确证归属的回执类型；其余（如间接线索）只记录为待确认原因
    DIRECT_PROOFS = {"采样确认", "独立确证", "消费提交"}

    def compute_watermark(self, entry_id: int) -> dict[str, Any]:
        """计算当前水位：按 (分区, 消费组) 归组取数，结合stream_source数据和分区进度。

        失败场景：
        - 上游断流 → watermark未推进
        - heartbeat超时 → 延迟告警
        - 水位重置不合法 → 冲突检测
        """
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return {"ok": False, "reason": f"水位 {entry_id} 不存在"}

        partition = str(entry.get("分区标识", ""))
        group = str(entry.get("消费组", "")).strip()
        # 按组读取本组确认位置：不读取其他组进度，也不把无组标识记录当作本组推进
        if group:
            wm_data = store.get_watermark(partition, group)
        else:
            # 兼容无消费组的旧条目：沿用无组读取，但只接受有具体位点的历史记录
            legacy_view = store.get_watermark(partition)
            wm_data = (
                legacy_view
                if legacy_view and isinstance(legacy_view.get("offset"), (int, float))
                else None
            )
        if wm_data is not None:
            current_offset = float(wm_data["offset"])
            position_source = "本组确认位置" if group else "历史无组标识记录"
        else:
            current_offset = float(entry.get("当前位点", "0"))
            position_source = "条目登记位点"
        last_heartbeat = wm_data["ts"] if wm_data else time.time()
        delay = time.time() - last_heartbeat

        # 更新水位
        entry["当前位点"] = str(current_offset)
        entry["提交时间"] = time.strftime("%Y-%m-%d %H:%M:%S")
        entry["延迟秒数"] = str(round(max(delay, 0), 1))
        entry["上次心跳"] = time.strftime("%Y-%m-%d %H:%M:%S")

        status = "正常"
        lag = float(str(entry.get("延迟秒数", 0)))
        if lag > 300:
            status = "积压"
        elif lag > 60:
            status = "延迟"
        entry["status"] = status
        entry["pending"] = status in ("延迟", "积压")
        entry["abnormal"] = status == "积压"

        # 归组提交：绑定来源、分区、消费组，并同步本组迟到判定清单
        store.update_watermark(partition, {
            "offset": current_offset + 50,
            "ts": time.time(),
            "lag_seconds": lag,
            "source": "watermark.compute",
            "basis": "消费提交",
        }, group=group or None)

        # 提示仍存在待认领的无组标识旧位点
        legacy = store.legacy_watermark(partition)
        pending_legacy = None
        if legacy is not None and not legacy.get("claimed_by"):
            pending_legacy = {
                "位点": legacy.get("offset"),
                "提示": "无组标识旧位点待认领，需值班员发起认领申请",
            }

        return {
            "ok": True,
            "entry_id": entry_id,
            "partition": partition,
            "消费组": group or None,
            "position_source": position_source,
            "status": status,
            "lag_seconds": round(lag, 1),
            "current_offset": current_offset + 50,
            "待认领旧位点": pending_legacy,
        }

    # ---------- 消费位置归组 ----------
    def commit_offset(
        self,
        partition: str,
        group: str,
        offset: Any,
        source: str = "watermark.commit",
        basis: str = "消费提交",
    ) -> dict[str, Any]:
        """消费组提交自己的确认位置：只写本组进度，并同步本组迟到判定清单。

        旧采样等无组标识的并发写入由 store 隔离在历史记录中，
        不会推进任何组的确认位置。
        """
        group_name = str(group).strip()
        if not group_name:
            return {"ok": False, "reason": "缺少消费组，拒绝写入无组标识进度"}
        try:
            offset_val = float(offset)
        except (TypeError, ValueError):
            return {"ok": False, "reason": f"位点 {offset} 不是有效数值"}
        store.update_watermark(partition, {
            "offset": offset_val,
            "ts": time.time(),
            "source": source,
            "basis": basis,
        }, group=group_name)
        record = store.get_watermark(partition, group_name)
        return {
            "ok": True,
            "partition": str(partition),
            "消费组": group_name,
            "确认位点": record["offset"],
            "依据": record.get("basis", basis),
        }

    def group_progress(self, group: str) -> dict[str, Any]:
        """进度列表按组核对：只返回本组确认位置，其他组进度不泄露位点。"""
        group_name = str(group).strip()
        own = store.watermark_records_for_group(group_name)
        claims = [c for c in store.watermark_claims() if c.get("group") == group_name]
        return {
            "ok": True,
            "消费组": group_name,
            "确认位置": [
                {
                    "分区标识": rec.get("partition"),
                    "确认位点": rec.get("offset"),
                    "依据": rec.get("basis", "消费提交"),
                    "来源": rec.get("source", ""),
                    "提交时间": rec.get("ts"),
                }
                for rec in sorted(own, key=lambda r: str(r.get("partition")))
            ],
            "其他组进度条数": store.count_other_group_records(group_name),
            "待认领旧位点": [
                {"分区标识": r.get("partition"), "位点": r.get("offset")}
                for r in store.unattributed_watermarks()
            ],
            "认领申请": [self._claim_view(c) for c in claims],
            "迟到判定清单": store.late_determinations_for(group_name),
        }

    # ---------- 旧位点认领 ----------
    def open_watermark_claim(
        self, partition: str, group: str, offset: Any, operator: str, note: str = ""
    ) -> dict[str, Any]:
        """值班员针对无组标识旧位点打开认领申请。

        共享分区存在多个合理认领方时，申请保持待确认并保留各方待确认原因，
        直到本组独立确证到达；同一旧位点已被认领的，拒绝重复认领。
        """
        partition = str(partition)
        group_name = str(group).strip()
        if not group_name:
            return {"ok": False, "reason": "认领申请必须指定消费组"}
        try:
            offset_val = float(offset)
        except (TypeError, ValueError):
            return {"ok": False, "reason": f"位点 {offset} 不是有效数值"}
        legacy = store.legacy_watermark(partition)
        if legacy is None:
            return {"ok": False, "reason": f"分区 {partition} 没有无组标识的旧位点可认领"}
        claimed = store.is_legacy_offset_claimed(partition, offset_val)
        owner = (claimed or {}).get("group") or legacy.get("claimed_by")
        if owner:
            return {"ok": False, "reason": f"旧位点 {offset_val} 已被 {owner} 认领，不可重复认领"}
        # 幂等：同组同分区已有待确认申请时复用
        for c in store.watermark_claims(partition):
            if c.get("group") == group_name and c.get("status") == "待确认":
                return {"ok": True, "claim": self._claim_view(c), "message": "已有待确认申请，复用"}
        claim = {
            "claim_id": f"CLAIM-{len(store.watermark_claims()) + 1:04d}",
            "partition": partition,
            "group": group_name,
            "offset": offset_val,
            "operator": operator,
            "note": note,
            "status": "待确认",
            "receipts": [],
            "pending_reasons": [],
            "basis": None,
            "opened_ts": time.time(),
            "resolved_ts": None,
        }
        legacy_offset = legacy.get("offset")
        if isinstance(legacy_offset, (int, float)) and float(legacy_offset) != offset_val:
            claim["pending_reasons"].append(
                f"申请位点 {offset_val} 与旧记录位点 {legacy_offset} 不一致，裁决以旧记录为准"
            )
        store.add_watermark_claim(claim)
        # 打开申请时一并核对已暂存的回执
        self._adjudicate_claims(partition)
        return {"ok": True, "claim": self._claim_view(claim)}

    def submit_claim_receipt(
        self, partition: str, group: str, offset: Any, source: str, proof: str
    ) -> dict[str, Any]:
        """回执补到：独立确证（采样确认等）可确证本组申请并推进；
        间接线索只记录为待确认原因，不构成任何组的推进依据。"""
        partition = str(partition)
        group_name = str(group).strip()
        try:
            offset_val = float(offset)
        except (TypeError, ValueError):
            return {"ok": False, "reason": f"位点 {offset} 不是有效数值"}
        proof_name = str(proof).strip()
        receipt = {
            "receipt_id": f"RCPT-{len(store.watermark_receipts()) + 1:04d}",
            "partition": partition,
            "group": group_name,
            "offset": offset_val,
            "source": str(source),
            "proof": proof_name,
            "direct": proof_name in self.DIRECT_PROOFS,
            "ts": time.time(),
        }
        store.add_watermark_receipt(receipt)
        for claim in store.watermark_claims(partition):
            if claim.get("status") != "待确认":
                continue
            if receipt["direct"] and claim.get("group") == group_name:
                claim["receipts"].append(receipt["receipt_id"])
            elif not receipt["direct"]:
                # 间接线索：共享分区存在多个合理认领方，按权限保留待确认原因
                claim["pending_reasons"].append(
                    f"{group_name} 提供间接线索（来源 {source}，位点 {offset_val}），归属待确认"
                )
        resolved = self._adjudicate_claims(partition)
        return {
            "ok": True,
            "receipt": receipt,
            "本轮确证": [self._claim_view(c) for c in resolved],
            "claims": [self._claim_view(c) for c in store.watermark_claims(partition)],
        }

    def review_watermark_claims(self, partition: str | None = None) -> dict[str, Any]:
        """本轮核对：一并裁决旧申请、汇总各组实际推进、列出下一轮可认领范围。"""
        newly = self._adjudicate_claims(partition)
        claims = store.watermark_claims(partition)
        grouped = store.grouped_watermarks()
        if partition is not None:
            grouped = {
                g: {p: r for p, r in parts.items() if p == str(partition)}
                for g, parts in grouped.items()
            }
            grouped = {g: parts for g, parts in grouped.items() if parts}
        claimable = store.unattributed_watermarks()
        claimed = store.claimed_legacy_offsets()
        if partition is not None:
            claimable = [r for r in claimable if r.get("partition") == str(partition)]
            claimed = [r for r in claimed if r.get("partition") == str(partition)]
        return {
            "ok": True,
            "本轮新裁决": [self._claim_view(c) for c in newly],
            "裁决结果": [self._claim_view(c) for c in claims],
            "各组确认位置": {
                g: {p: r.get("offset") for p, r in parts.items()}
                for g, parts in grouped.items()
            },
            "下一轮可认领范围": [
                {"分区标识": r.get("partition"), "位点": r.get("offset"), "来源": r.get("source", "")}
                for r in claimable
            ],
            "已认领旧记录": claimed,
        }

    def _adjudicate_claims(self, partition: str | None = None) -> list[dict[str, Any]]:
        """裁决待确认申请：只认本组直接确证；确认后归属旧位点并同步本组迟到判定清单。"""
        resolved = []
        for claim in store.watermark_claims(partition):
            if claim.get("status") != "待确认":
                continue
            direct = [
                r for r in store.watermark_receipts(claim["partition"])
                if r.get("direct") and r.get("group") == claim["group"]
                and float(r.get("offset", "nan")) == float(claim["offset"])
            ]
            if not direct:
                continue
            basis_receipt = direct[-1]  # 最新一份直接确证作为采用依据
            legacy = store.attribute_legacy_watermark(
                claim["partition"],
                claim["group"],
                source=basis_receipt.get("source", ""),
                basis=basis_receipt.get("proof", ""),
                receipt_id=basis_receipt.get("receipt_id", ""),
            )
            claim["resolved_ts"] = time.time()
            if legacy is None:
                claim["status"] = "已驳回"
                claim["pending_reasons"].append("旧位点已被其他组认领或不存在，本申请驳回")
            else:
                claim["status"] = "已确证"
                claim["basis"] = {
                    "receipt_id": basis_receipt.get("receipt_id"),
                    "proof": basis_receipt.get("proof"),
                    "source": basis_receipt.get("source"),
                }
            resolved.append(claim)
        return resolved

    @staticmethod
    def _claim_view(claim: dict[str, Any]) -> dict[str, Any]:
        return {
            "claim_id": claim.get("claim_id"),
            "partition": claim.get("partition"),
            "group": claim.get("group"),
            "offset": claim.get("offset"),
            "status": claim.get("status"),
            "依据": claim.get("basis"),
            "pending_reasons": list(claim.get("pending_reasons", [])),
        }

    def emit_watermark_for_late_data(self, source_id: str, group: str | None = None) -> dict[str, Any]:
        """向 late_data 暴露当前水位信息，用于判定迟到数据。

        指定消费组时只暴露该组确认位置；无组读取回退到历史兼容视图，
        分区已归组时不泄露任何组的具体位点。
        """
        group_name = str(group).strip() if group else ""
        wm_data = store.get_watermark(source_id, group_name) if group_name else store.get_watermark(source_id)
        wm_data = wm_data or {}
        result = {
            "ok": True,
            "source": source_id,
            "消费组": group_name or None,
            "current_offset": wm_data.get("offset") or 0,
            "lag_seconds": wm_data.get("lag_seconds", 0),
            "last_update": wm_data.get("ts", time.time()),
            "downstream_consumer": "late_data",
        }
        if wm_data.get("ambiguous"):
            result["ambiguous"] = True
            result["claimants"] = wm_data.get("claimants", [])
            result["current_offset"] = 0  # 歧义视图不泄露任何组的确认位点
        return result

    def domain_snapshot(self) -> dict[str, Any]:
        base = super().domain_snapshot()
        base["watermark_offsets_tracked"] = len(store._watermark_offsets)
        base["pending_legacy_records"] = len(store.unattributed_watermarks())
        base["open_claims"] = sum(1 for c in store.watermark_claims() if c.get("status") == "待确认")
        return base
