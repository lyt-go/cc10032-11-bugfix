"""内存数据仓库：给每个业务模块准备一份可筛选、可流转的示例数据，并支持跨模块数据流水线。

真实项目里这里会换成数据库访问层；当前实现只依赖标准库，保证克隆下来就能起。
"""
from __future__ import annotations

import hashlib
import time
from typing import Any

from app.seed import SEED_ROWS


class DataPipeline:
    """跨模块数据流水线引擎：支持 输入→转换→版本化产物→下游消费 的完整链路。

    每条流水线定义了源模块、目标模块、转换函数和验证规则。
    产物存储为可版本化的数据快照，支持脏行检测、重复批次拒绝、冲突版本检测、中断恢复。
    """

    def __init__(self, name: str, sources: list[str], target_module: str) -> None:
        self.name = name
        self.sources = sources
        self.target_module = target_module
        self._artifacts: list[dict[str, Any]] = []
        self._versions: list[str] = []  # 版本号列表
        self._dirty_log: list[dict[str, Any]] = []
        self._batch_registry: set[str] = set()  # 防重复批次
        self._last_checkpoint: dict[str, Any] = {}  # 中断恢复点
        self._interrupted = False

    def ingest(self, rows: list[dict[str, Any]], batch_id: str) -> tuple[int, list[str]]:
        """摄入数据行，返回 (成功行数, 脏行详情列表)"""
        if batch_id in self._batch_registry:
            return 0, [f"重复批次 {batch_id} 已存在，拒绝摄入"]
        self._batch_registry.add(batch_id)
        clean_rows = []
        dirty_details = []
        for i, row in enumerate(rows):
            issues = self._validate_row(row, i)
            if issues:
                dirty_details.append(f"行 {i}: {'; '.join(issues)}")
                self._dirty_log.append({"batch": batch_id, "row_index": i, "issues": issues, "row": row, "ts": time.time()})
            else:
                clean_rows.append(row)
        self._artifacts.extend(clean_rows)
        return len(clean_rows), dirty_details

    def transform(self, transform_fn, *args, **kwargs) -> list[dict[str, Any]]:
        """对当前产物执行转换函数"""
        self._save_checkpoint("transform_enter", len(self._artifacts))
        try:
            result = transform_fn(self._artifacts, *args, **kwargs)
            version = self._make_version("transform")
            self._versions.append(version)
            self._save_checkpoint("transform_exit", len(result))
            return result
        except Exception:
            self._interrupted = True
            raise

    def versioned_snapshot(self) -> dict[str, Any]:
        """生成版本化产物快照"""
        version = self._versions[-1] if self._versions else self._make_version("snapshot")
        return {
            "pipeline": self.name,
            "version": version,
            "artifact_count": len(self._artifacts),
            "artifact_sample": self._artifacts[:5],
            "dirty_count": len(self._dirty_log),
            "batch_count": len(self._batch_registry),
            "interrupted": self._interrupted,
            "checkpoint": self._last_checkpoint,
        }

    def health_report(self) -> dict[str, Any]:
        """流水线健康报告：用于外部模块消费和容量材料生成"""
        return {
            "pipeline": self.name,
            "source_modules": self.sources,
            "target_module": self.target_module,
            "total_ingested": sum(1 for a in self._artifacts),
            "dirty_rows": len(self._dirty_log),
            "versions": len(self._versions),
            "batches_processed": len(self._batch_registry),
            "interrupted": self._interrupted,
            "last_artifact_ts": max((a.get("ts", 0) for a in self._artifacts), default=0) if self._artifacts else 0,
        }

    def _validate_row(self, row: dict[str, Any], index: int) -> list[str]:
        issues = []
        for key, val in row.items():
            if val is None or (isinstance(val, str) and not val.strip()):
                issues.append(f"空值字段 '{key}'")
        if any(row.get(k) is None for k in ["id"]):
            issues.append("缺少主键 id")
        return issues

    def _make_version(self, tag: str) -> str:
        ts = str(int(time.time() * 1000))
        raw = f"{self.name}:{tag}:{ts}"
        return hashlib.md5(raw.encode()).hexdigest()[:12]

    def _save_checkpoint(self, stage: str, count: int) -> None:
        self._last_checkpoint = {"stage": stage, "count": count, "ts": time.time()}

    def recover_checkpoint(self) -> dict[str, Any]:
        """从最近检查点恢复"""
        return {
            "pipeline": self.name,
            "interrupted": self._interrupted,
            "last_checkpoint": self._last_checkpoint,
            "artifacts_before_interrupt": len(self._artifacts),
        }


class Store:
    def __init__(self) -> None:
        self._tables: dict[str, list[dict[str, Any]]] = {
            name: [dict(row) for row in rows] for name, rows in SEED_ROWS.items()
        }
        # 跨模块数据流水线
        self._pipelines: dict[str, DataPipeline] = {}
        # 全局变更日志（用于 changelog 跨模块追溯）
        self._global_changelog: list[dict[str, Any]] = []
        # Schema 注册表（cross-module schema registry）
        self._schema_registry: dict[str, list[dict[str, Any]]] = {}
        # 水位偏移记录（watermark → late_data 共享）
        # 键为 (分区标识, 消费组)：同分区的不同消费组各自持有确认位置，互不覆盖；
        # 消费组为 None 的键承载历史无组标识记录（兼容旧数据，待认领）。
        self._watermark_offsets: dict[tuple[str, str | None], dict[str, Any]] = {}
        # 已认领的无组标识旧位点登记：(分区标识, 位点) → 认领信息（防止同一份记录被重复认领）
        self._claimed_legacy_offsets: dict[tuple[str, float], dict[str, Any]] = {}
        # 旧位点认领申请（值班员发起）
        self._watermark_claims: list[dict[str, Any]] = []
        # 认领回执：独立确证 / 间接线索
        self._watermark_receipts: list[dict[str, Any]] = []
        # 迟到判定清单：提交状态与判定结论按 (来源, 分区, 消费组) 绑定
        self._late_determinations: list[dict[str, Any]] = []
        # 快照存储表（snapshot_mgr → backfill_job 共享）
        self._snapshot_store: dict[str, dict[str, Any]] = {}
        # 血缘图（lineage → table_format 共享）
        self._lineage_graph: dict[str, list[dict[str, Any]]] = {}
        # 归档清单（cold_archive → retention 共享）
        self._archive_manifest: list[dict[str, Any]] = []

    # ---------- 基础 CRUD ----------
    def module_names(self) -> list[str]:
        return sorted(self._tables)

    def rows(self, module: str) -> list[dict[str, Any]]:
        return self._tables.setdefault(module, [])

    def find(self, module: str, entry_id: int) -> dict[str, Any] | None:
        for row in self.rows(module):
            if int(row.get("id", 0)) == entry_id:
                return row
        return None

    def overview(self) -> dict[str, object]:
        modules: list[dict[str, object]] = []
        for name in self.module_names():
            rows = self.rows(name)
            modules.append({
                "name": name,
                "created": len(rows),
                "pending": sum(1 for row in rows if row.get("pending")),
                "abnormal": sum(1 for row in rows if row.get("abnormal")),
            })
        # 跨模块流水线状态
        pipeline_stats = []
        for pname, pl in self._pipelines.items():
            pipeline_stats.append(pl.health_report())
        cards = [
            {"label": "业务模块", "value": len(modules)},
            {"label": "今日新增", "value": sum(int(item["created"]) for item in modules)},
            {"label": "待处理", "value": sum(int(item["pending"]) for item in modules)},
            {"label": "异常量", "value": sum(int(item["abnormal"]) for item in modules)},
            {"label": "流水线", "value": len(pipeline_stats)},
        ]
        return {"cards": cards, "modules": modules, "pipelines": pipeline_stats}

    # ---------- 跨模块流水线 ----------
    def create_pipeline(self, name: str, sources: list[str], target_module: str) -> DataPipeline:
        pl = DataPipeline(name, sources, target_module)
        self._pipelines[name] = pl
        return pl

    def pipeline(self, name: str) -> DataPipeline | None:
        return self._pipelines.get(name)

    def all_pipelines(self) -> dict[str, DataPipeline]:
        return dict(self._pipelines)

    # ---------- Schema 注册表 ----------
    def register_schema(self, topic: str, schema_def: dict[str, Any], version: str) -> dict[str, Any]:
        versions = self._schema_registry.setdefault(topic, [])
        entry = {"topic": topic, "definition": schema_def, "version": version, "ts": time.time()}
        versions.append(entry)
        return entry

    def schema_versions(self, topic: str) -> list[dict[str, Any]]:
        return self._schema_registry.get(topic, [])

    def check_schema_compatibility(self, topic: str, new_fields: list[str]) -> dict[str, Any]:
        versions = self.schema_versions(topic)
        if not versions:
            return {"compatible": True, "reason": "新主题，无兼容性问题"}
        latest = versions[-1]
        old_fields = set(latest.get("definition", {}).get("fields", []))
        missing_required = [f for f in old_fields if f not in new_fields]
        if missing_required:
            return {"compatible": False, "reason": f"缺少已有字段: {missing_required}"}
        return {"compatible": True, "reason": "向后兼容"}

    # ---------- 水位记录 ----------
    @staticmethod
    def _watermark_key(partition: str, group: str | None) -> tuple[str, str | None]:
        g = str(group).strip() if group else ""
        return (str(partition), g or None)

    def update_watermark(self, partition: str, offset: dict[str, Any], group: str | None = None) -> None:
        """写入水位进度。

        - 指定消费组：写入该组自己的确认位置，并把提交状态同步到本组迟到判定清单；
        - 不指定消费组：写入历史无组标识记录（兼容旧采样/旧数据），
          不触碰任何消费组的归组进度。
        """
        key = self._watermark_key(partition, group)
        record = dict(offset)
        record["partition"] = key[0]
        record["group"] = key[1]
        record["attributed"] = key[1] is not None
        record.setdefault("ts", time.time())
        if key[1] is None:
            # 无组标识记录：若该 (分区, 位点) 已被认领，保留认领标记，防止重复认领
            try:
                claim_info = self._claimed_legacy_offsets.get((key[0], float(record.get("offset"))))
            except (TypeError, ValueError):
                claim_info = None
            if claim_info:
                record["claimed_by"] = claim_info["group"]
                record["claim_basis"] = claim_info["basis"]
                record["claim_receipt_id"] = claim_info["receipt_id"]
            self._watermark_offsets[key] = record
            return
        self._watermark_offsets[key] = record
        # 提交状态同步到本组迟到判定清单（绑定来源、分区、消费组）
        self.append_late_determination({
            "kind": "提交同步",
            "来源": str(record.get("source") or "watermark"),
            "分区标识": key[0],
            "消费组": key[1],
            "确认位点": record.get("offset"),
            "依据": str(record.get("basis") or "消费提交"),
        })

    def get_watermark(self, partition: str, group: str | None = None) -> dict[str, Any] | None:
        """按组读取确认位置。

        - 指定消费组：只返回该组自己的确认位置；没有则返回 None，
          不回退到无组标识记录（各组只能使用可证明属于该组的推进）；
        - 不指定消费组：返回历史无组标识记录；若分区只有归组记录，
          返回歧义视图（不泄露任何组的确认位点，仅列出合理认领方）。
        """
        partition = str(partition)
        if group:
            rec = self._watermark_offsets.get((partition, str(group)))
            return dict(rec) if rec is not None else None
        legacy = self._watermark_offsets.get((partition, None))
        if legacy is not None:
            result = dict(legacy)
            result["legacy"] = True
            return result
        claimants = sorted({g for (p, g) in self._watermark_offsets if p == partition and g})
        if claimants:
            return {
                "partition": partition,
                "group": None,
                "offset": None,
                "attributed": False,
                "ambiguous": True,
                "claimants": claimants,
                "reason": "分区进度已按消费组归属，无组读取不返回具体位点",
            }
        return None

    def watermark_records_for_group(self, group: str) -> list[dict[str, Any]]:
        """指定消费组的全部确认位置（进度列表按组核对）。"""
        g = str(group)
        return [dict(rec) for (_p, gg), rec in self._watermark_offsets.items() if gg == g]

    def count_other_group_records(self, group: str) -> int:
        """其他消费组的归组记录条数（只暴露条数，不泄露位点）。"""
        g = str(group)
        return sum(1 for (_p, gg) in self._watermark_offsets if gg is not None and gg != g)

    def grouped_watermarks(self) -> dict[str, dict[str, dict[str, Any]]]:
        """全部已归组确认位置：{消费组: {分区标识: 记录}}"""
        result: dict[str, dict[str, dict[str, Any]]] = {}
        for (p, g), rec in self._watermark_offsets.items():
            if g is None:
                continue
            result.setdefault(g, {})[p] = dict(rec)
        return result

    def legacy_watermark(self, partition: str) -> dict[str, Any] | None:
        """分区的无组标识历史记录（不含归组记录）。"""
        rec = self._watermark_offsets.get((str(partition), None))
        return dict(rec) if rec is not None else None

    def unattributed_watermarks(self) -> list[dict[str, Any]]:
        """仍未完成归属的无组标识旧记录（下一轮可认领范围）。"""
        return [
            dict(rec) for (_p, g), rec in self._watermark_offsets.items()
            if g is None and not rec.get("claimed_by")
        ]

    def claimed_legacy_offsets(self) -> list[dict[str, Any]]:
        """已认领的无组标识旧位点及认领依据。"""
        return [
            {"partition": p, "offset": o, **info}
            for (p, o), info in self._claimed_legacy_offsets.items()
        ]

    def is_legacy_offset_claimed(self, partition: str, offset: Any) -> dict[str, Any] | None:
        try:
            key = (str(partition), float(offset))
        except (TypeError, ValueError):
            return None
        return self._claimed_legacy_offsets.get(key)

    def attribute_legacy_watermark(
        self, partition: str, group: str, *, source: str, basis: str, receipt_id: str
    ) -> dict[str, Any] | None:
        """把无组标识旧位点归属到指定消费组：登记认领并生成该组自己的确认位置。

        归属时记录采用的依据（来源与回执），已被认领的记录拒绝重复归属。
        """
        key = (str(partition), None)
        legacy = self._watermark_offsets.get(key)
        if legacy is None or legacy.get("claimed_by"):
            return None
        legacy["claimed_by"] = str(group)
        legacy["claim_basis"] = basis
        legacy["claim_receipt_id"] = receipt_id
        legacy["claimed_ts"] = time.time()
        try:
            self._claimed_legacy_offsets[(key[0], float(legacy.get("offset")))] = {
                "group": str(group), "basis": basis, "receipt_id": receipt_id,
            }
        except (TypeError, ValueError):
            pass
        self.update_watermark(key[0], {
            "offset": legacy.get("offset"),
            "source": source,
            "basis": basis,
            "claimed_from": "无组标识旧位点",
            "ts": time.time(),
        }, group=group)
        return dict(legacy)

    # ---------- 旧位点认领 ----------
    def add_watermark_claim(self, claim: dict[str, Any]) -> dict[str, Any]:
        self._watermark_claims.append(claim)
        return claim

    def watermark_claims(self, partition: str | None = None) -> list[dict[str, Any]]:
        if partition is None:
            return list(self._watermark_claims)
        return [c for c in self._watermark_claims if c.get("partition") == str(partition)]

    def add_watermark_receipt(self, receipt: dict[str, Any]) -> dict[str, Any]:
        self._watermark_receipts.append(receipt)
        return receipt

    def watermark_receipts(self, partition: str | None = None) -> list[dict[str, Any]]:
        if partition is None:
            return list(self._watermark_receipts)
        return [r for r in self._watermark_receipts if r.get("partition") == str(partition)]

    # ---------- 迟到判定清单 ----------
    def append_late_determination(self, record: dict[str, Any]) -> dict[str, Any]:
        entry = dict(record)
        entry.setdefault("ts", time.time())
        self._late_determinations.append(entry)
        return entry

    def late_determinations_for(self, group: str, partition: str | None = None) -> list[dict[str, Any]]:
        """指定消费组的迟到判定清单（按组核对，不返回其他组记录）。"""
        g = str(group)
        return [
            r for r in self._late_determinations
            if r.get("消费组") == g and (partition is None or r.get("分区标识") == str(partition))
        ]

    # ---------- 快照存储 ----------
    def save_snapshot(self, snap_id: str, data: dict[str, Any]) -> None:
        self._snapshot_store[snap_id] = data

    def get_snapshot(self, snap_id: str) -> dict[str, Any] | None:
        return self._snapshot_store.get(snap_id)

    # ---------- 变更日志 ----------
    def append_changelog(self, entry: dict[str, Any]) -> None:
        entry["_ts"] = time.time()
        self._global_changelog.append(entry)

    def changelog_for_table(self, table_name: str) -> list[dict[str, Any]]:
        return [e for e in self._global_changelog if e.get("目标表") == table_name]

    # ---------- 血缘图 ----------
    def add_lineage_edge(self, upstream: str, downstream: str, edge_data: dict[str, Any]) -> None:
        edges = self._lineage_graph.setdefault(upstream, [])
        edges.append({"downstream": downstream, **edge_data, "ts": time.time()})

    def lineage_downstream(self, table_name: str) -> list[dict[str, Any]]:
        return self._lineage_graph.get(table_name, [])

    # ---------- 归档清单 ----------
    def add_archive_record(self, record: dict[str, Any]) -> None:
        self._archive_manifest.append(record)

    def archive_manifest_for_table(self, table_name: str) -> list[dict[str, Any]]:
        return [r for r in self._archive_manifest if r.get("源表名称") == table_name]


store = Store()