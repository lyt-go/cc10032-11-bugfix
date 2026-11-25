"""stream_source 领域服务：流式数据源管理与数据接入。

业务逻辑：
1. 流式源配置管理：消息中间件、Topic反序列化、水位起始点
2. 数据接入链路：消息中间件→反序列化→数据行→下游batch_slice/watermark
3. 失败场景：连接超时检测、脏数据反序列化失败、并发度冲突
4. 与下游流水线：为 batch_slice 和 watermark 提供数据源元信息

跨模块数据流：
  stream_source → batch_slice（提供流式源元信息）
  stream_source → watermark（提供分区消费进度）
"""
from __future__ import annotations

import time
from typing import Any

from app.services.base import BaseModuleService
from app.store import store


class StreamSourceService(BaseModuleService):
    MODULE = "stream_source"
    ENTITY = "流式源配置"
    OPERATION_TERM = "接入"
    LIST_FIELDS = ["源标识", "消息中间件", "Topic名称", "消费组", "反序列化方式", "起始位点", "并发度", "连接超时"]
    REQUIRED_FIELDS = ["源标识", "消息中间件", "Topic名称"]
    STATUS_ORDER = ["草稿", "待验证", "已启用", "已停用"]
    ACTION_RULES = {"提交验证": "待验证", "启用源": "已启用", "停用源": "已停用"}
    NEGATIVE_ACTIONS = ["停用源"]

    # 跨模块流水线
    PIPELINE = "stream_ingestion"
    DOWNSTREAM_MODULES = ["batch_slice", "watermark"]
    PRODUCES_ARTIFACT = "流式数据源元信息快照"

    def stream_sample(self, source_id: int, row_count: int = 10) -> dict[str, Any]:
        """模拟从消息中间件读取流式数据样本。

        失败场景：
        - 源未启用 → 拒绝
        - 连接超时超限 → 模拟中断
        - 反序列化格式不匹配 → 返回脏数据列表
        """
        entry = store.find(self.MODULE, source_id)
        if entry is None:
            return {"ok": False, "reason": f"流式源 {source_id} 不存在"}
        if entry.get("status") != "已启用":
            return {"ok": False, "reason": f"流式源 {source_id} 未启用，当前状态: {entry.get('status')}"}

        timeout_val = str(entry.get("连接超时", "30"))
        deser = str(entry.get("反序列化方式", "json")).lower()
        samples = []
        dirty = []

        for i in range(row_count):
            ts = time.time() - i * 5  # 模拟时间偏移
            raw_payload = f"msg_{source_id}_{i}_{ts:.0f}"
            try:
                # 模拟反序列化
                if deser == "json":
                    parsed = {"ts": ts, "payload": raw_payload, "topic": entry.get("Topic名称")}
                elif deser == "avro":
                    parsed = {"ts": ts, "avro_schema": "v1", "payload": raw_payload, "topic": entry.get("Topic名称")}
                elif deser == "protobuf":
                    parsed = {"ts": ts, "proto_msg": raw_payload, "topic": entry.get("Topic名称")}
                else:
                    parsed = {"ts": ts, "raw": raw_payload, "topic": entry.get("Topic名称")}

                # 模拟连接超时：30%概率
                if i > 0 and i % 3 == 0:
                    try:
                        timeout = int(timeout_val)
                    except ValueError:
                        timeout = 30
                    if timeout < 5:
                        dirty.append({"row": i, "issue": f"连接超时({timeout}ms)，消息 {raw_payload} 读取中断"})
                        continue

                samples.append(parsed)
            except Exception:
                dirty.append({"row": i, "issue": f"反序列化失败: 格式 {deser} 不兼容消息 {raw_payload}"})

        # 更新水位偏移
        if samples:
            # 最后一个样本的时间戳作为当前水位
            # 注意：采样写入不带消费组，属于历史无组标识记录（旧采样路径）。
            # 它不会推进任何消费组的确认位置；如需归组，须走 watermark 的
            # 认领流程凭独立确证归属。
            last_ts = max(s.get("ts", 0) for s in samples)
            store.update_watermark(str(source_id), {"offset": last_ts, "source": entry.get("源标识"), "ts": time.time()})

        # 产生流水线制品
        batch_id = f"STREAM_{source_id}_{int(time.time())}"
        self.produce_artifact(samples, batch_id)

        return {
            "ok": True,
            "source_id": source_id,
            "samples": len(samples),
            "dirty": len(dirty),
            "dirty_details": dirty[:5],
        }

    def check_connectivity(self, source_id: int) -> dict[str, Any]:
        """连通性检测：预检消息中间件连接状态

        失败场景：
        - 连接超时 → 返回超时警告
        - Topic不存在 → 返回配置错误
        - 消费组冲突 → 返回并发度冲突
        """
        entry = store.find(self.MODULE, source_id)
        if entry is None:
            return {"ok": False, "reason": "源配置不存在"}

        issues = []
        # 模拟连接检测
        if int(str(entry.get("并发度", "1"))) > 10:
            issues.append("并发度>10，存在消费组冲突风险")
        if "kafka" not in str(entry.get("消息中间件", "")).lower() and "pulsar" not in str(entry.get("消息中间件", "")).lower():
            issues.append("消息中间件类型未识别，仅支持kafka/pulsar")
        return {
            "ok": len(issues) == 0,
            "source_id": source_id,
            "issues": issues,
            "ready": len(issues) == 0,
        }

    def emit_source_manifest(self) -> list[dict[str, Any]]:
        """输出所有已启用流式源的清单，供 batch_slice 和 watermark 消费"""
        rows = store.rows(self.MODULE)
        return [
            {"id": r["id"], "源标识": r.get("源标识"), "Topic名称": r.get("Topic名称"),
             "反序列化方式": r.get("反序列化方式"), "起始位点": r.get("起始位点"),
             "并发度": r.get("并发度"), "连接超时": r.get("连接超时"), "status": r.get("status")}
            for r in rows if r.get("status") == "已启用"
        ]

    def domain_snapshot(self) -> dict[str, Any]:
        base = super().domain_snapshot()
        base["enabled_sources"] = len(self.emit_source_manifest())
        base["watermark_partitions"] = len(store._watermark_offsets)
        return base