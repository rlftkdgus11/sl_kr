#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import csv
import datetime as dt
import glob
import json
import os
import re
from collections import Counter, defaultdict
from difflib import SequenceMatcher


ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def latest(pattern: str) -> str:
    paths = glob.glob(os.path.join(ROOT, pattern))
    if not paths:
        raise FileNotFoundError(f"missing required artifact: {pattern}")
    return max(paths, key=os.path.getmtime)


def read_tsv(path: str) -> list[dict[str, str]]:
    with open(path, "r", encoding="utf-8-sig", errors="replace", newline="") as f:
        return list(csv.DictReader(f, delimiter="\t"))


def read_csv(path: str) -> list[dict[str, str]]:
    with open(path, "r", encoding="utf-8-sig", errors="replace", newline="") as f:
        return list(csv.DictReader(f))


def to_int(v: str, default: int = 0) -> int:
    try:
        return int(str(v).strip().strip('"'))
    except Exception:
        return default


def to_float(v: str, default: float = 0.0) -> float:
    try:
        return float(str(v).strip().strip('"'))
    except Exception:
        return default


def one_line(text: str) -> str:
    text = (text or "").replace("\r", " ").replace("\n", " | ").replace("\t", " ")
    text = re.sub(r"\s+", " ", text).strip()
    return text


def norm_name(text: str) -> str:
    out: list[str] = []
    last_space = False
    for ch in text:
        c = " " if ch == "_" else ch
        if c in ("\\", "/"):
            out.clear()
            last_space = False
            continue
        if c == ".":
            break
        c = c.lower()
        if c in (" ", "\t"):
            if last_space:
                continue
            out.append(" ")
            last_space = True
            continue
        last_space = False
        out.append(c)
    return "".join(out).strip()


def norm_text(text: str) -> str:
    text = one_line(text).lower()
    text = re.sub(r"[|,:;.!?\"'`~()\[\]{}<>-]", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def sim_ratio(a: str, b: str) -> str:
    na = norm_text(a)
    nb = norm_text(b)
    if not na or not nb:
        return ""
    return f"{SequenceMatcher(None, na, nb).ratio():.3f}"


def split_pipes(text: str) -> list[str]:
    if not text:
        return []
    return [x.strip() for x in text.split("|") if x.strip()]


def load_sound_names(path: str) -> dict[int, str]:
    out: dict[int, str] = {}
    with open(path, "r", encoding="utf-8-sig", errors="replace", newline="") as f:
        r = csv.reader(f)
        next(r, None)
        for row in r:
            if not row:
                continue
            h = to_int(row[0], -1)
            if h < 0:
                continue
            name = (row[1] if len(row) > 1 else "").strip()
            if name:
                out[h] = name
    return out


def load_subs_by_handle(path: str) -> dict[int, dict[str, str]]:
    out: dict[int, dict[str, str]] = {}
    with open(path, "r", encoding="utf-8-sig", errors="replace") as f:
        for line in f:
            line = line.rstrip("\n")
            if not line:
                continue
            cols = line.split("\t")
            if len(cols) < 4:
                continue
            h = to_int(cols[0], -1)
            if h < 0:
                try:
                    h = int((cols[0].strip().lstrip("0") or "0"), 10)
                except Exception:
                    continue
            out[h] = {
                "color": cols[1].strip(),
                "duration": cols[2].strip(),
                "text": one_line(cols[3].strip()),
            }
    return out


def load_subs_by_name(path: str) -> tuple[dict[str, dict[str, str]], dict[str, dict[str, str]]]:
    exact: dict[str, dict[str, str]] = {}
    norm_first: dict[str, dict[str, str]] = {}
    with open(path, "r", encoding="utf-8-sig", errors="replace") as f:
        for line in f:
            line = line.rstrip("\n")
            if not line:
                continue
            cols = line.split("\t")
            if len(cols) < 4:
                continue
            name = cols[0].strip()
            if not name:
                continue
            rec = {
                "name": name,
                "color": cols[1].strip(),
                "duration": cols[2].strip(),
                "text": one_line(cols[3].strip()),
            }
            exact[name] = rec
            n = norm_name(name)
            if n and n not in norm_first:
                norm_first[n] = rec
    return exact, norm_first


def load_mobile(path: str) -> dict[int, dict[str, str]]:
    out: dict[int, dict[str, str]] = {}
    with open(path, "r", encoding="utf-8-sig", errors="replace") as f:
        data = json.load(f)
    if not isinstance(data, list):
        return out
    for entry in data:
        if not isinstance(entry, dict):
            continue
        h = to_int(entry.get("source_index", ""), -1)
        if h < 0:
            continue
        sents = entry.get("sentences", [])
        lines: list[str] = []
        if isinstance(sents, list):
            for s in sents:
                if isinstance(s, dict):
                    t = one_line(str(s.get("text", "")))
                else:
                    t = one_line(str(s))
                if t:
                    lines.append(t)
        out[h] = {
            "speaker": one_line(str(entry.get("speaker", ""))),
            "color": one_line(str(entry.get("color", ""))),
            "sentence_count": str(len(lines)),
            "text": one_line(" | ".join(lines)),
        }
    return out


def load_duration_map(path: str) -> dict[int, float]:
    out: dict[int, float] = {}
    for row in read_csv(path):
        h = to_int(row.get("handle", ""), -1)
        if h < 0:
            continue
        out[h] = to_float(row.get("seconds", "0"), 0.0)
    return out


def load_hash_collisions(path: str) -> tuple[dict[int, list[tuple[str, int, int, int, int]]], list[dict[str, str]]]:
    key_to_handles: dict[tuple[str, int, int, int, int], set[int]] = defaultdict(set)
    with open(path, "r", encoding="utf-8-sig", errors="replace") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            cols = line.split("\t")
            if len(cols) < 6:
                continue
            try:
                key = (cols[0], int(cols[1]), int(cols[2]), int(cols[3]), int(cols[4]))
                handle = int(cols[5])
            except Exception:
                continue
            key_to_handles[key].add(handle)

    handle_to_keys: dict[int, list[tuple[str, int, int, int, int]]] = defaultdict(list)
    key_rows: list[dict[str, str]] = []
    for key, hs_set in key_to_handles.items():
        if len(hs_set) <= 1:
            continue
        hs = sorted(hs_set)
        for h in hs:
            handle_to_keys[h].append(key)
        key_rows.append(
            {
                "hash": key[0],
                "nbytes": str(key[1]),
                "rate": str(key[2]),
                "ch": str(key[3]),
                "bits": str(key[4]),
                "handles": "|".join(str(x) for x in hs),
                "handle_count": str(len(hs)),
            }
        )
    key_rows.sort(key=lambda r: (-to_int(r["handle_count"]), r["hash"]))
    return dict(handle_to_keys), key_rows


def main() -> int:
    date_tag = dt.datetime.now().strftime("%Y%m%d")

    review_path = latest("static_kr_patch_review_surface_*.tsv")
    master_path = latest("static_kr_patch_handle_master_*.tsv")
    uncovered_path = latest("static_kr_patch_uncovered_*.tsv")
    matrix_path = latest("static_audio_full_domain_handle_matrix_*_v2.tsv")
    conflict_path = latest("static_conflict_priority_rules_*.tsv")
    fallback_path = latest("static_stream_fallback_policy_table_*.tsv")

    sound_names_path = os.path.join(ROOT, "sound_names.csv")
    sub_h_path = os.path.join(ROOT, "subtitles_by_handle.txt")
    sub_n_path = os.path.join(ROOT, "subtitles_by_name.txt")
    mobile_path = os.path.join(ROOT, "subtitles_ko_sentences.json")
    hash_path = os.path.join(ROOT, "sound_hash_map.tsv")
    duration_path = os.path.join(ROOT, "audio_durations.csv")
    cond_path = os.path.join(ROOT, "_static_mfa_sound_actions_with_conditions.tsv")

    matrix_rows = read_tsv(matrix_path)
    matrix_by_handle = {to_int(r.get("handle", ""), -1): r for r in matrix_rows if to_int(r.get("handle", ""), -1) >= 0}

    master_rows = read_tsv(master_path)
    master_by_handle = {to_int(r.get("handle", ""), -1): r for r in master_rows if to_int(r.get("handle", ""), -1) >= 0}

    review_rows = read_tsv(review_path)
    review_handles = [to_int(r.get("handle", ""), -1) for r in review_rows if to_int(r.get("handle", ""), -1) >= 0]

    uncovered_rows = read_tsv(uncovered_path)

    sound_names = load_sound_names(sound_names_path)
    sub_h = load_subs_by_handle(sub_h_path)
    sub_n_exact, sub_n_norm = load_subs_by_name(sub_n_path)
    mobile_map = load_mobile(mobile_path)
    duration_map = load_duration_map(duration_path)
    handle_to_collision_keys, collision_key_rows = load_hash_collisions(hash_path)

    raw_policy: dict[int, str] = {}
    incoming_policy: dict[int, str] = {}
    for row in read_tsv(conflict_path):
        raw = to_int(row.get("raw_handle", ""), -1)
        plus = to_int(row.get("plus_handle", ""), -1)
        policy = row.get("final_policy", "").strip().strip('"')
        if raw >= 0:
            raw_policy[raw] = policy
        if plus >= 0:
            incoming_policy[plus] = policy

    fallback_by_handle: dict[int, dict[str, str]] = {}
    for row in read_tsv(fallback_path):
        h = to_int(row.get("canonical_handle", ""), -1)
        if h < 0:
            continue
        fallback_by_handle[h] = row

    cond_rows = read_tsv(cond_path)
    cond_by_sample0: dict[int, list[dict[str, str]]] = defaultdict(list)
    for row in cond_rows:
        sample0 = to_int(row.get("sample_handle", ""), -1)
        if sample0 >= 0:
            cond_by_sample0[sample0].append(row)

    out_deep = os.path.join(ROOT, f"static_review25_deep_dive_{date_tag}.tsv")
    out_stream = os.path.join(ROOT, f"static_stream18_policy_context_round2_{date_tag}.tsv")
    out_hash = os.path.join(ROOT, f"static_hash_collision_deep_profile_round2_{date_tag}.tsv")
    out_uncovered = os.path.join(ROOT, f"static_uncovered25_noncore_profile_round2_{date_tag}.tsv")
    out_source_drift = os.path.join(ROOT, f"static_round2_source_drift_profile_{date_tag}.tsv")
    out_text_matrix = os.path.join(ROOT, f"static_review25_text_source_matrix_{date_tag}.tsv")
    out_summary = os.path.join(ROOT, f"static_extended_round2_summary_{date_tag}.txt")

    deep_rows: list[dict[str, str]] = []
    stream_rows: list[dict[str, str]] = []
    uncovered_profile_rows: list[dict[str, str]] = []
    source_drift_rows: list[dict[str, str]] = []
    text_matrix_rows: list[dict[str, str]] = []

    alignment_counter: Counter[str] = Counter()
    risk_tier_counter: Counter[str] = Counter()
    stream_policy_counter: Counter[str] = Counter()

    for handle in sorted(set(review_handles)):
        m = master_by_handle.get(handle, {})
        matrix = matrix_by_handle.get(handle, {})
        name = sound_names.get(handle, m.get("name", ""))

        h_text = sub_h.get(handle, {}).get("text", "")
        n_rec = sub_n_exact.get(name) or sub_n_norm.get(norm_name(name))
        n_text = n_rec.get("text", "") if n_rec else ""
        mobile = mobile_map.get(handle, {})
        mob_text = mobile.get("text", "")

        sim_hn = sim_ratio(h_text, n_text)
        sim_hm = sim_ratio(h_text, mob_text)
        sim_nm = sim_ratio(n_text, mob_text)

        if h_text and n_text:
            v = float(sim_hn) if sim_hn else 0.0
            if v >= 0.97:
                align = "handle_name_near_exact"
            elif v >= 0.75:
                align = "handle_name_partial"
            else:
                align = "handle_name_divergent"
        elif h_text and not n_text:
            align = "handle_only"
        elif (not h_text) and n_text:
            align = "name_only"
        elif (not h_text) and (not n_text) and mob_text:
            align = "mobile_only"
        else:
            align = "no_text"

        alignment_counter[align] += 1

        sample0 = handle - 1
        conds = cond_by_sample0.get(sample0, [])
        cond_token_counter: Counter[str] = Counter()
        action_counter: Counter[str] = Counter()
        for c in conds:
            for token in split_pipes(c.get("condition_names", "")):
                cond_token_counter[token] += 1
            action = c.get("action", "").strip()
            if action:
                action_counter[action] += 1

        cond_tokens_top = "|".join(f"{k}:{v}" for k, v in cond_token_counter.most_common(8))
        actions_top = "|".join(f"{k}:{v}" for k, v in action_counter.most_common(6))

        rflags = m.get("risk_flags", "")
        stream_expected = m.get("stream_expected", matrix.get("stream_expected", "0"))

        raw_pol = raw_policy.get(handle, "")
        in_pol = incoming_policy.get(handle, "")
        fallback = fallback_by_handle.get(handle, {})
        fallback_allowed = fallback.get("fallback_allowed", "")
        det_verdict = fallback.get("deterministic_verdict", "")

        if in_pol == "ALLOW_SHIFT_TO_PLUS":
            policy_role = "plus_allow_target"
        elif raw_pol.startswith("BLOCK_RAW_STREAM_EXPECTED"):
            policy_role = "raw_stream_block_anchor"
        elif raw_pol.startswith("BLOCK_RAW_DIALOGUE"):
            policy_role = "raw_dialog_block_anchor"
        elif in_pol.startswith("BLOCK"):
            policy_role = "plus_block_target"
        else:
            policy_role = "neutral"

        if stream_expected == "1":
            risk_tier = "P1_STREAM"
        elif handle in handle_to_collision_keys:
            risk_tier = "P1_HASH_COLLISION"
        elif "matrix_source_drift_from_handle" in rflags:
            risk_tier = "P2_SOURCE_DRIFT"
        elif "non_dialogue_has_subtitle" in rflags:
            risk_tier = "P3_NON_DIALOGUE_SUBTITLE"
        else:
            risk_tier = "P4_LOW"
        risk_tier_counter[risk_tier] += 1

        if stream_expected == "1":
            if det_verdict:
                stream_policy_counter[det_verdict] += 1
            else:
                stream_policy_counter[policy_role] += 1

        if stream_expected == "1":
            next_focus = "verify_stream_chain_policy"
        elif handle in handle_to_collision_keys:
            next_focus = "compare_duplicate_asset_semantics"
        elif "matrix_source_drift_from_handle" in rflags:
            next_focus = "reconcile_matrix_vs_sub_source"
        elif "non_dialogue_has_subtitle" in rflags:
            next_focus = "confirm_intentional_sfx_caption"
        else:
            next_focus = "none"

        deep_rows.append(
            {
                "handle": str(handle),
                "name": name,
                "dialogue_class": m.get("dialogue_class", matrix.get("dialogue_class", "")),
                "stream_expected": stream_expected,
                "selected_source": m.get("selected_source", ""),
                "risk_tier": risk_tier,
                "risk_flags": rflags,
                "text_alignment_class": align,
                "has_handle_text": "1" if h_text else "0",
                "has_name_text": "1" if n_text else "0",
                "has_mobile_text": "1" if mob_text else "0",
                "sim_handle_name": sim_hn,
                "sim_handle_mobile": sim_hm,
                "sim_name_mobile": sim_nm,
                "handle_text_len": str(len(h_text)),
                "name_text_len": str(len(n_text)),
                "mobile_text_len": str(len(mob_text)),
                "raw_policy": raw_pol,
                "incoming_policy": in_pol,
                "policy_role": policy_role,
                "fallback_allowed": fallback_allowed,
                "deterministic_verdict": det_verdict,
                "duration_sec": f"{duration_map.get(handle, 0.0):.3f}",
                "container_class": m.get("container_class", matrix.get("container_class", "")),
                "route_rows_observed": m.get("route_rows_observed", matrix.get("route_rows_observed", "")),
                "route_frames": m.get("route_frames", matrix.get("route_frames", "")),
                "route_groups": m.get("route_groups", matrix.get("route_groups", "")),
                "condition_row_count_matrix": matrix.get("condition_row_count", ""),
                "condition_rows_mfa": str(len(conds)),
                "condition_tokens_top": cond_tokens_top,
                "actions_top": actions_top,
                "hash_collision_member": "1" if handle in handle_to_collision_keys else "0",
                "hash_collision_key_count": str(len(handle_to_collision_keys.get(handle, []))),
                "next_static_focus": next_focus,
            }
        )

        text_matrix_rows.append(
            {
                "handle": str(handle),
                "name": name,
                "selected_source": m.get("selected_source", ""),
                "text_alignment_class": align,
                "sim_handle_name": sim_hn,
                "sim_handle_mobile": sim_hm,
                "sim_name_mobile": sim_nm,
                "handle_text": h_text,
                "name_text": n_text,
                "mobile_text": mob_text,
                "mobile_speaker": mobile.get("speaker", ""),
            }
        )

        if risk_tier == "P2_SOURCE_DRIFT":
            source_drift_rows.append(
                {
                    "handle": str(handle),
                    "name": name,
                    "dialogue_class": m.get("dialogue_class", matrix.get("dialogue_class", "")),
                    "selected_source": m.get("selected_source", ""),
                    "risk_flags": rflags,
                    "route_rows_observed": m.get("route_rows_observed", matrix.get("route_rows_observed", "")),
                    "route_frames": m.get("route_frames", matrix.get("route_frames", "")),
                    "route_groups": m.get("route_groups", matrix.get("route_groups", "")),
                    "condition_tokens_top": cond_tokens_top,
                    "actions_top": actions_top,
                    "name_text": n_text,
                }
            )

    stream_handles = sorted(
        h
        for h, row in matrix_by_handle.items()
        if row.get("stream_expected", "").strip().strip('"') == "1"
    )
    for h in stream_handles:
        m = master_by_handle.get(h, {})
        matrix = matrix_by_handle.get(h, {})
        deep = next((r for r in deep_rows if to_int(r["handle"], -1) == h), None)
        fb = fallback_by_handle.get(h, {})
        stream_rows.append(
            {
                "handle": str(h),
                "name": sound_names.get(h, matrix.get("name", "")),
                "dialogue_class": matrix.get("dialogue_class", ""),
                "container_class": matrix.get("container_class", ""),
                "selected_source": m.get("selected_source", ""),
                "raw_policy": raw_policy.get(h, ""),
                "incoming_policy": incoming_policy.get(h, ""),
                "edge_policy": fb.get("edge_policy", ""),
                "fallback_allowed": fb.get("fallback_allowed", ""),
                "deterministic_verdict": fb.get("deterministic_verdict", ""),
                "subtitle_resolved_from": fb.get("subtitle_resolved_from", ""),
                "route_frames": matrix.get("route_frames", ""),
                "route_groups": matrix.get("route_groups", ""),
                "condition_names": matrix.get("condition_names", ""),
                "policy_role": (deep or {}).get("policy_role", ""),
                "risk_tier": (deep or {}).get("risk_tier", ""),
                "risk_flags": (deep or {}).get("risk_flags", ""),
            }
        )

    hash_rows: list[dict[str, str]] = []
    for c in collision_key_rows:
        handles = [to_int(x, -1) for x in c["handles"].split("|") if to_int(x, -1) >= 0]
        names = [sound_names.get(h, "") for h in handles]
        dclasses = [matrix_by_handle.get(h, {}).get("dialogue_class", "") for h in handles]
        sources = [master_by_handle.get(h, {}).get("selected_source", "") for h in handles]
        durations = [duration_map.get(h, 0.0) for h in handles]
        containers = [matrix_by_handle.get(h, {}).get("container_class", "") for h in handles]
        route_frames = [matrix_by_handle.get(h, {}).get("route_frames", "") for h in handles]
        same_duration = "1" if len(set(f"{x:.3f}" for x in durations)) == 1 else "0"
        same_container = "1" if len(set(containers)) == 1 else "0"
        impact = "high_dialogue" if any(dc == "dialogue" for dc in dclasses) else "low_non_dialogue"
        hash_rows.append(
            {
                "hash": c["hash"],
                "nbytes": c["nbytes"],
                "rate": c["rate"],
                "ch": c["ch"],
                "bits": c["bits"],
                "handles": c["handles"],
                "names": "|".join(names),
                "dialogue_classes": "|".join(dclasses),
                "selected_sources": "|".join(sources),
                "durations_sec": "|".join(f"{x:.3f}" for x in durations),
                "same_duration": same_duration,
                "containers": "|".join(containers),
                "same_container": same_container,
                "route_frames": "|".join(route_frames),
                "impact": impact,
            }
        )

    for r in uncovered_rows:
        h = to_int(r.get("handle", ""), -1)
        if h < 0:
            continue
        name = r.get("name", sound_names.get(h, ""))
        frames = r.get("route_frames", "")
        container = r.get("container_class", matrix_by_handle.get(h, {}).get("container_class", ""))
        if name.lower().startswith("giggle"):
            cat = "ambient_giggle_cluster"
        elif "custom level" in frames.lower():
            cat = "custom_level_sfx_cluster"
        elif container == "mp3_frame_in_wav_ext":
            cat = "non_dialogue_music_or_mood"
        else:
            cat = "non_dialogue_sfx"
        uncovered_profile_rows.append(
            {
                "handle": str(h),
                "name": name,
                "container_class": container,
                "route_frames": frames,
                "route_groups": r.get("route_groups", ""),
                "duration_sec": f"{duration_map.get(h, 0.0):.3f}",
                "likely_category": cat,
            }
        )

    with open(out_deep, "w", encoding="utf-8", newline="") as f:
        fields = [
            "handle",
            "name",
            "dialogue_class",
            "stream_expected",
            "selected_source",
            "risk_tier",
            "risk_flags",
            "text_alignment_class",
            "has_handle_text",
            "has_name_text",
            "has_mobile_text",
            "sim_handle_name",
            "sim_handle_mobile",
            "sim_name_mobile",
            "handle_text_len",
            "name_text_len",
            "mobile_text_len",
            "raw_policy",
            "incoming_policy",
            "policy_role",
            "fallback_allowed",
            "deterministic_verdict",
            "duration_sec",
            "container_class",
            "route_rows_observed",
            "route_frames",
            "route_groups",
            "condition_row_count_matrix",
            "condition_rows_mfa",
            "condition_tokens_top",
            "actions_top",
            "hash_collision_member",
            "hash_collision_key_count",
            "next_static_focus",
        ]
        w = csv.DictWriter(f, fieldnames=fields, delimiter="\t")
        w.writeheader()
        w.writerows(deep_rows)

    with open(out_stream, "w", encoding="utf-8", newline="") as f:
        fields = [
            "handle",
            "name",
            "dialogue_class",
            "container_class",
            "selected_source",
            "raw_policy",
            "incoming_policy",
            "edge_policy",
            "fallback_allowed",
            "deterministic_verdict",
            "subtitle_resolved_from",
            "route_frames",
            "route_groups",
            "condition_names",
            "policy_role",
            "risk_tier",
            "risk_flags",
        ]
        w = csv.DictWriter(f, fieldnames=fields, delimiter="\t")
        w.writeheader()
        w.writerows(stream_rows)

    with open(out_hash, "w", encoding="utf-8", newline="") as f:
        fields = [
            "hash",
            "nbytes",
            "rate",
            "ch",
            "bits",
            "handles",
            "names",
            "dialogue_classes",
            "selected_sources",
            "durations_sec",
            "same_duration",
            "containers",
            "same_container",
            "route_frames",
            "impact",
        ]
        w = csv.DictWriter(f, fieldnames=fields, delimiter="\t")
        w.writeheader()
        w.writerows(hash_rows)

    with open(out_uncovered, "w", encoding="utf-8", newline="") as f:
        fields = [
            "handle",
            "name",
            "container_class",
            "route_frames",
            "route_groups",
            "duration_sec",
            "likely_category",
        ]
        w = csv.DictWriter(f, fieldnames=fields, delimiter="\t")
        w.writeheader()
        w.writerows(uncovered_profile_rows)

    with open(out_source_drift, "w", encoding="utf-8", newline="") as f:
        fields = [
            "handle",
            "name",
            "dialogue_class",
            "selected_source",
            "risk_flags",
            "route_rows_observed",
            "route_frames",
            "route_groups",
            "condition_tokens_top",
            "actions_top",
            "name_text",
        ]
        w = csv.DictWriter(f, fieldnames=fields, delimiter="\t")
        w.writeheader()
        w.writerows(source_drift_rows)

    with open(out_text_matrix, "w", encoding="utf-8", newline="") as f:
        fields = [
            "handle",
            "name",
            "selected_source",
            "text_alignment_class",
            "sim_handle_name",
            "sim_handle_mobile",
            "sim_name_mobile",
            "handle_text",
            "name_text",
            "mobile_text",
            "mobile_speaker",
        ]
        w = csv.DictWriter(f, fieldnames=fields, delimiter="\t")
        w.writeheader()
        w.writerows(text_matrix_rows)

    with open(out_summary, "w", encoding="utf-8") as f:
        f.write("Extended static analysis round2 summary\n")
        f.write(f"date_tag: {date_tag}\n")
        f.write(f"review_source: {os.path.basename(review_path)}\n")
        f.write(f"master_source: {os.path.basename(master_path)}\n")
        f.write(f"matrix_source: {os.path.basename(matrix_path)}\n")
        f.write(f"fallback_source: {os.path.basename(fallback_path)}\n")
        f.write("\n")
        f.write(f"review_handles: {len(deep_rows)}\n")
        f.write(f"stream_handles_total: {len(stream_rows)}\n")
        f.write(f"hash_collision_keys: {len(hash_rows)}\n")
        f.write(f"uncovered_handles_profiled: {len(uncovered_profile_rows)}\n")
        f.write("\n")
        f.write("alignment_breakdown:\n")
        for k, v in alignment_counter.most_common():
            f.write(f"- {k}: {v}\n")
        f.write("\n")
        f.write("risk_tier_breakdown:\n")
        for k, v in risk_tier_counter.most_common():
            f.write(f"- {k}: {v}\n")
        f.write("\n")
        f.write("stream_policy_breakdown:\n")
        for k, v in stream_policy_counter.most_common():
            f.write(f"- {k}: {v}\n")
        f.write("\n")
        p1_stream = [r["handle"] for r in deep_rows if r["risk_tier"] == "P1_STREAM"]
        p1_hash = [r["handle"] for r in deep_rows if r["risk_tier"] == "P1_HASH_COLLISION"]
        f.write("priority_handles:\n")
        f.write(f"- P1_STREAM: {','.join(p1_stream)}\n")
        f.write(f"- P1_HASH_COLLISION: {','.join(p1_hash)}\n")

    print("out_deep", out_deep)
    print("out_stream", out_stream)
    print("out_hash", out_hash)
    print("out_uncovered", out_uncovered)
    print("out_source_drift", out_source_drift)
    print("out_text_matrix", out_text_matrix)
    print("out_summary", out_summary)
    print("review_handles", len(deep_rows))
    print("stream_handles_total", len(stream_rows))
    print("hash_collision_keys", len(hash_rows))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
