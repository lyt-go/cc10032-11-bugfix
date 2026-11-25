"""消费位置归组与旧位点认领验收测试。

验收场景：
第一轮 —— 甲组/乙组共用分区标识：分别确认到 120 / 75 且互不覆盖；
          甲组推进到 130 并回放位置 80，同一事件在两组得到不同的迟到结论；
          提交状态同步到本组迟到判定清单（绑定来源、分区、消费组）。
第二轮 —— 无组标识旧位点 140：值班员打开甲组认领申请后，
          乙组间接线索先到、甲组独立采样确认后到；
          已确证的来源允许推进，未完成归属的旧记录保留待认领；
          核对本轮裁决、两组实际推进与下一轮可认领范围。
兼容 —— 旧采样（无组标识）与正常消费并发提交时相互隔离；
        无组读取不泄露任何组的确认位点。
"""
from __future__ import annotations

import time

from app.services import service_for
from app.store import store


def _wm():
    return service_for("watermark")


def _ld():
    return service_for("late_data")


def _make_policy(partition: str, allowed: str = "30", discard: str = "3600") -> int:
    """登记一条已生效的迟到策略，关联源指向指定分区。"""
    ld = _ld()
    entry, missing = ld.create_entry({
        "策略编号": f"LATE-ACC-{partition}",
        "关联源": partition,
        "允许延迟秒数": allowed,
        "处理方式": "side_output",
        "侧输出路径": "s3://late-output/accept/",
        "重定向主题": "late_accept",
        "丢弃阈值": discard,
        "最后更新": "",
    })
    assert not missing
    entry["status"] = "已生效"
    return int(entry["id"])


# ============================================================
# 第一轮：共用分区标识的两组各自确认，互不覆盖
# ============================================================
class TestRound1GroupScopedProgress:
    PARTITION = "partition-acc-r1"

    def test_commits_do_not_overwrite_across_groups(self):
        """甲组确认 120、乙组确认 75：同分区提交互不覆盖"""
        wm = _wm()
        r1 = wm.commit_offset(self.PARTITION, "甲组", 120, source="normal_consume")
        r2 = wm.commit_offset(self.PARTITION, "乙组", 75, source="normal_consume")
        assert r1["ok"] and r2["ok"]
        assert store.get_watermark(self.PARTITION, "甲组")["offset"] == 120.0
        assert store.get_watermark(self.PARTITION, "乙组")["offset"] == 75.0

    def test_advance_replay_and_per_group_late_conclusion(self):
        """甲组推进到 130 并回放位置 80：两组位置独立，同一事件迟到结论不同"""
        wm = _wm()
        wm.commit_offset(self.PARTITION, "甲组", 120, source="normal_consume")
        wm.commit_offset(self.PARTITION, "乙组", 75, source="normal_consume")
        wm.commit_offset(self.PARTITION, "甲组", 130, source="normal_consume")

        # 两组位置：甲组推进不影响乙组
        assert store.get_watermark(self.PARTITION, "甲组")["offset"] == 130.0
        assert store.get_watermark(self.PARTITION, "乙组")["offset"] == 75.0

        # 进度列表按组核对：各组只能看到自己的确认位置
        prog_a = wm.group_progress("甲组")
        mine_a = {p["分区标识"]: p["确认位点"] for p in prog_a["确认位置"]}
        assert mine_a[self.PARTITION] == 130.0
        assert prog_a["其他组进度条数"] >= 1  # 乙组进度仅以条数存在，不泄露位点
        assert all(
            not (p["分区标识"] == self.PARTITION and p["确认位点"] == 75.0)
            for p in prog_a["确认位置"]
        )
        prog_b = wm.group_progress("乙组")
        mine_b = {p["分区标识"]: p["确认位点"] for p in prog_b["确认位置"]}
        assert mine_b[self.PARTITION] == 75.0

        # 回放位置 80：同一事件在两组的迟到结论不同
        policy_id = _make_policy(self.PARTITION, allowed="30", discard="3600")
        ld = _ld()
        replay = [{"position": 80, "payload": "replay-80"}]
        res_a = ld.classify_late_events(policy_id, replay, group="甲组")
        res_b = ld.classify_late_events(policy_id, replay, group="乙组")
        assert res_a["ok"] and res_b["ok"]
        assert res_a["attributed"] and res_b["attributed"]
        # 甲组确认到 130：位置 80 落后 50，超出允许延迟 30 → 迟到
        assert res_a["late"] == 1 and res_a["on_time"] == 0
        # 乙组确认到 75：位置 80 尚未到期 → 不判迟到
        assert res_b["late"] == 0 and res_b["on_time"] == 1

    def test_commit_status_synced_to_group_determination_list(self):
        """提交状态同步到本组迟到判定清单，绑定来源、分区和消费组"""
        wm = _wm()
        wm.commit_offset(self.PARTITION, "甲组", 120, source="normal_consume")
        wm.commit_offset(self.PARTITION, "甲组", 130, source="normal_consume")
        wm.commit_offset(self.PARTITION, "乙组", 75, source="normal_consume")

        dets_a = store.late_determinations_for("甲组", self.PARTITION)
        syncs = [d for d in dets_a if d["kind"] == "提交同步"]
        assert any(d["确认位点"] == 130.0 for d in syncs)
        for d in dets_a:
            assert d["消费组"] == "甲组"
            assert d["分区标识"] == self.PARTITION
            assert d["来源"]  # 来源非空

        # 清单按组隔离：乙组清单里不出现甲组的提交
        dets_b = store.late_determinations_for("乙组", self.PARTITION)
        assert dets_b
        assert all(d["消费组"] == "乙组" for d in dets_b)
        assert all(d["确认位点"] != 130.0 for d in dets_b)


# ============================================================
# 第二轮：无组标识旧位点的认领与裁决
# ============================================================
class TestRound2LegacyClaim:
    PARTITION = "partition-acc-r2"
    SPARE = "partition-acc-r2-spare"

    def test_legacy_claim_workflow(self):
        """旧位点 140：值班员开申请 → 乙组间接线索 → 甲组采样确认 → 裁决归属"""
        wm = _wm()
        # 历史无组标识旧位点（旧采样写入，不带消费组）
        store.update_watermark(self.PARTITION, {"offset": 140, "source": "旧采样", "ts": time.time() - 86400})
        # 另留一个无组标识旧位点，用于核对下一轮可认领范围
        store.update_watermark(self.SPARE, {"offset": 55, "source": "旧采样", "ts": time.time() - 86400})

        # 值班员打开甲组认领申请
        opened = wm.open_watermark_claim(self.PARTITION, "甲组", 140, operator="值班员")
        assert opened["ok"] is True
        claim_id = opened["claim"]["claim_id"]
        assert opened["claim"]["status"] == "待确认"

        # 确认前先收到乙组对旧记录的间接线索：只保留待确认原因，不构成推进
        clue = wm.submit_claim_receipt(self.PARTITION, "乙组", 140, source="对账线索", proof="间接线索")
        assert clue["ok"] is True
        claim = next(c for c in store.watermark_claims(self.PARTITION) if c["claim_id"] == claim_id)
        assert claim["status"] == "待确认"
        assert any("乙组" in reason for reason in claim["pending_reasons"])
        assert store.get_watermark(self.PARTITION, "甲组") is None
        assert store.get_watermark(self.PARTITION, "乙组") is None

        # 再收到甲组独立的采样确认 → 确证归属
        confirm = wm.submit_claim_receipt(self.PARTITION, "甲组", 140, source="stream_sample", proof="采样确认")
        assert confirm["ok"] is True
        claim = next(c for c in store.watermark_claims(self.PARTITION) if c["claim_id"] == claim_id)
        assert claim["status"] == "已确证"
        assert claim["basis"]["proof"] == "采样确认"

        # 旧歧义记录区分采用的依据：甲组确认位置来自采样确认，乙组不因此推进
        rec_a = store.get_watermark(self.PARTITION, "甲组")
        assert rec_a["offset"] == 140.0
        assert rec_a["basis"] == "采样确认"
        assert rec_a["source"] == "stream_sample"
        assert store.get_watermark(self.PARTITION, "乙组") is None
        legacy = store.get_watermark(self.PARTITION)
        assert legacy["claimed_by"] == "甲组"
        assert legacy["claim_basis"] == "采样确认"

        # 同一份记录不可重复认领
        dup = wm.open_watermark_claim(self.PARTITION, "乙组", 140, operator="值班员")
        assert dup["ok"] is False
        assert "重复认领" in dup["reason"]

        # 已确证的来源允许推进；乙组只能用自己的确证推进
        adv = wm.commit_offset(self.PARTITION, "甲组", 150, source="normal_consume")
        assert adv["ok"] is True
        own_b = wm.commit_offset(self.PARTITION, "乙组", 60, source="normal_consume")
        assert own_b["ok"] is True
        assert store.get_watermark(self.PARTITION, "甲组")["offset"] == 150.0
        assert store.get_watermark(self.PARTITION, "乙组")["offset"] == 60.0

        # 本轮核对：旧申请裁决、两组实际推进、下一轮可认领范围
        review = wm.review_watermark_claims()
        adjudicated = {c["claim_id"]: c for c in review["裁决结果"]}
        assert adjudicated[claim_id]["status"] == "已确证"
        assert adjudicated[claim_id]["依据"]["proof"] == "采样确认"
        assert review["各组确认位置"]["甲组"][self.PARTITION] == 150.0
        assert review["各组确认位置"]["乙组"][self.PARTITION] == 60.0
        claimable = {(c["分区标识"], c["位点"]) for c in review["下一轮可认领范围"]}
        assert (self.SPARE, 55.0) in claimable           # 未完成归属的旧记录保留待认领
        assert (self.PARTITION, 140.0) not in claimable  # 已认领记录退出可认领范围
        claimed = {(c["partition"], c["offset"]): c for c in review["已认领旧记录"]}
        assert claimed[(self.PARTITION, 140.0)]["group"] == "甲组"

    def test_competing_claims_single_winner(self):
        """共享分区多个合理认领方：先确证者归属，其余申请驳回并保留原因"""
        wm = _wm()
        partition = "partition-acc-r2-race"
        store.update_watermark(partition, {"offset": 90, "source": "旧采样", "ts": time.time() - 3600})

        assert wm.open_watermark_claim(partition, "甲组", 90, operator="值班员")["ok"]
        assert wm.open_watermark_claim(partition, "乙组", 90, operator="值班员")["ok"]
        # 乙组间接线索先到：两份申请都保留待确认原因
        wm.submit_claim_receipt(partition, "乙组", 90, source="对账线索", proof="间接线索")
        # 甲组独立确证先到 → 甲组归属
        wm.submit_claim_receipt(partition, "甲组", 90, source="stream_sample", proof="采样确认")
        # 乙组的确证后补到 → 旧位点已被认领，申请驳回
        wm.submit_claim_receipt(partition, "乙组", 90, source="stream_sample", proof="采样确认")

        claims = {c["group"]: c for c in store.watermark_claims(partition)}
        assert claims["甲组"]["status"] == "已确证"
        assert claims["乙组"]["status"] == "已驳回"
        assert any("已被其他组认领" in r for r in claims["乙组"]["pending_reasons"])
        assert store.get_watermark(partition, "甲组")["offset"] == 90.0
        assert store.get_watermark(partition, "乙组") is None


# ============================================================
# 兼容与隔离：旧采样并发、无组读取、归组取数
# ============================================================
class TestLegacyCompatibilityAndIsolation:
    def test_legacy_sampling_does_not_advance_group(self):
        """旧采样与正常消费并发提交：各组只用可证明属于该组的推进"""
        wm = _wm()
        partition = "partition-acc-r3"
        wm.commit_offset(partition, "甲组", 200, source="normal_consume")
        # 旧采样并发写入（无组标识）：不得推进任何组的确认位置
        store.update_watermark(partition, {"offset": 180, "source": "旧采样", "ts": time.time()})
        assert store.get_watermark(partition, "甲组")["offset"] == 200.0
        legacy = store.get_watermark(partition)
        assert legacy["legacy"] is True
        assert legacy["offset"] == 180
        # 无组标识记录进入待认领范围，认领前不构成推进依据
        claimable = [r for r in store.unattributed_watermarks() if r["partition"] == partition]
        assert len(claimable) == 1

    def test_group_less_read_does_not_leak_group_offsets(self):
        """无组读取只给歧义视图：不泄露任何组的确认位点"""
        wm = _wm()
        partition = "partition-acc-r4"
        wm.commit_offset(partition, "甲组", 300, source="normal_consume")
        wm.commit_offset(partition, "乙组", 100, source="normal_consume")
        view = store.get_watermark(partition)
        assert view is not None
        assert view["ambiguous"] is True
        assert view["offset"] is None
        assert set(view["claimants"]) == {"甲组", "乙组"}

    def test_compute_watermark_uses_own_group(self):
        """compute_watermark 按条目消费组取数：同分区两组互不串位"""
        wm = _wm()
        e1, m1 = wm.create_entry({"分区标识": "partition-acc-r5", "消费组": "甲组", "当前位点": "500"})
        e2, m2 = wm.create_entry({"分区标识": "partition-acc-r5", "消费组": "乙组", "当前位点": "700"})
        assert not m1 and not m2
        r1 = wm.compute_watermark(int(e1["id"]))
        r2 = wm.compute_watermark(int(e2["id"]))
        assert r1["ok"] and r2["ok"]
        assert r1["position_source"] == "条目登记位点"   # 首次：本组无记录，用条目登记位点
        # 甲组推进到 550 后，乙组仍从自己的 700 出发，而不是读到甲组的 550
        assert store.get_watermark("partition-acc-r5", "甲组")["offset"] == 550.0
        assert store.get_watermark("partition-acc-r5", "乙组")["offset"] == 750.0
        # 再次计算：各自从本组确认位置继续推进
        r1b = wm.compute_watermark(int(e1["id"]))
        assert r1b["position_source"] == "本组确认位置"
        assert store.get_watermark("partition-acc-r5", "甲组")["offset"] == 600.0
        assert store.get_watermark("partition-acc-r5", "乙组")["offset"] == 750.0

    def test_emit_watermark_scoped_by_group(self):
        """向 late_data 暴露的水位按组隔离"""
        wm = _wm()
        partition = "partition-acc-r6"
        wm.commit_offset(partition, "甲组", 400, source="normal_consume")
        emit_a = wm.emit_watermark_for_late_data(partition, group="甲组")
        emit_b = wm.emit_watermark_for_late_data(partition, group="乙组")
        assert emit_a["current_offset"] == 400.0
        assert emit_b["current_offset"] == 0  # 乙组无确认位置，看不到甲组位点
        emit_legacy = wm.emit_watermark_for_late_data(partition)
        assert emit_legacy["ambiguous"] is True
        assert emit_legacy["current_offset"] == 0
