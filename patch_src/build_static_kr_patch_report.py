#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Build static, runtime-free artifacts for KR subtitle patch operations.

Outputs:
- static_kr_patch_handle_master_YYYYMMDD.tsv
- static_kr_patch_dialogue_plan_YYYYMMDD.tsv
- static_kr_patch_review_surface_YYYYMMDD.tsv
- static_kr_patch_uncovered_YYYYMMDD.tsv
- static_kr_patch_summary_YYYYMMDD.txt
"""

from __future__ import annotations

import csv
import datetime as dt
import glob
import json
import os
import re
from collections import Counter, defaultdict


ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def pick_latest(pattern: str) -> str:
    paths = glob.glob(os.path.join(ROOT, pattern))
    if not paths:
        raise FileNotFoundError(f"missing required artifact by pattern: {pattern}")
    return max(paths, key=os.path.getmtime)


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


def one_line(text: str) -> str:
    if not text:
        return ""
    text = text.replace("\r", " ").replace("\n", " | ").replace("\t", " ")
    text = re.sub(r"\s+", " ", text).strip()
    return text


def norm_text(text: str) -> str:
    text = one_line(text).lower()
    text = re.sub(r"[|,:;.!?\"'`~()\[\]{}<>-]", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def load_sound_names(path: str) -> dict[int, str]:
    out: dict[int, str] = {}
    with open(path, "r", encoding="utf-8-sig", errors="replace", newline="") as f:
        r = csv.reader(f)
        next(r, None)
        for row in r:
            if not row:
                continue
            try:
                handle = int((row[0] if len(row) > 0 else "").strip())
            except Exception:
                continue
            name = (row[1] if len(row) > 1 else "").strip()
            if name:
                out[handle] = name
    return out


def load_tsv_dicts(path: str) -> list[dict[str, str]]:
    with open(path, "r", encoding="utf-8-sig", errors="replace", newline="") as f:
        return list(csv.DictReader(f, delimiter="\t"))


def load_subtitles_by_handle(path: str) -> dict[int, dict[str, str]]:
    out: dict[int, dict[str, str]] = {}
    with open(path, "r", encoding="utf-8-sig", errors="replace") as f:
        for line in f:
            line = line.rstrip("\n")
            if not line:
                continue
            cols = line.split("\t")
            if len(cols) < 4:
                continue
            try:
                handle = int(cols[0].strip(), 10)
            except Exception:
                try:
                    handle = int((cols[0].strip().lstrip("0") or "0"), 10)
                except Exception:
                    continue
            out[handle] = {
                "color": cols[1].strip(),
                "duration": cols[2].strip(),
                "text": one_line(cols[3].strip()),
            }
    return out


def load_subtitles_by_name(path: str) -> tuple[
    dict[str, dict[str, str]],
    dict[str, dict[str, str]],
    dict[str, list[str]],
]:
    exact: dict[str, dict[str, str]] = {}
    norm_first: dict[str, dict[str, str]] = {}
    norm_all: dict[str, list[str]] = defaultdict(list)
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
            if n:
                norm_all[n].append(name)
                if n not in norm_first:
                    norm_first[n] = rec
    return exact, norm_first, dict(norm_all)


def load_mobile_sentences(path: str) -> dict[int, dict[str, str]]:
    out: dict[int, dict[str, str]] = {}
    if not os.path.exists(path):
        return out
    with open(path, "r", encoding="utf-8-sig", errors="replace") as f:
        data = json.load(f)
    if not isinstance(data, list):
        return out
    for entry in data:
        if not isinstance(entry, dict):
            continue
        try:
            handle = int(entry.get("source_index"))
        except Exception:
            continue
        speaker = one_line(str(entry.get("speaker", "")))
        color = one_line(str(entry.get("color", "")))
        sentences = entry.get("sentences", [])
        lines: list[str] = []
        if isinstance(sentences, list):
            for s in sentences:
                if isinstance(s, dict):
                    t = one_line(str(s.get("text", "")))
                else:
                    t = one_line(str(s))
                if t:
                    lines.append(t)
        out[handle] = {
            "speaker": speaker,
            "color": color,
            "sentence_count": str(len(lines)),
            "text": one_line(" | ".join(lines)),
        }
    return out


def load_conflict_rules(path: str) -> tuple[dict[int, str], dict[int, str], set[int], set[int], list[tuple[int, int]]]:
    raw_policy: dict[int, str] = {}
    plus_incoming: dict[int, str] = {}
    raw_allow: set[int] = set()
    raw_block: set[int] = set()
    allow_pairs: list[tuple[int, int]] = []
    for row in load_tsv_dicts(path):
        try:
            raw = int(row.get("raw_handle", "").strip().strip('"'))
            plus = int(row.get("plus_handle", "").strip().strip('"'))
        except Exception:
            continue
        policy = row.get("final_policy", "").strip().strip('"')
        raw_policy[raw] = policy
        plus_incoming[plus] = policy
        if policy == "ALLOW_SHIFT_TO_PLUS":
            raw_allow.add(raw)
            allow_pairs.append((raw, plus))
        elif policy.startswith("BLOCK"):
            raw_block.add(raw)
    return raw_policy, plus_incoming, raw_allow, raw_block, allow_pairs


def load_hash_collision_handles(path: str) -> tuple[set[int], list[tuple[tuple[str, int, int, int, int], list[int]]]]:
    key_to_handles: dict[tuple[str, int, int, int, int], set[int]] = defaultdict(set)
    if not os.path.exists(path):
        return set(), []
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
    collisions: list[tuple[tuple[str, int, int, int, int], list[int]]] = []
    collision_handles: set[int] = set()
    for key, handles in key_to_handles.items():
        if len(handles) > 1:
            hs = sorted(handles)
            collisions.append((key, hs))
            collision_handles.update(hs)
    collisions.sort(key=lambda x: (-len(x[1]), x[0][0]))
    return collision_handles, collisions


def source_from_matrix(matrix_source: str) -> str:
    matrix_source = (matrix_source or "").strip()
    if matrix_source == "handle":
        return "handle"
    if matrix_source == "name":
        return "name_like"
    if matrix_source == "none":
        return "none"
    return matrix_source


def main() -> int:
    date_tag = dt.datetime.now().strftime("%Y%m%d")

    sound_names_path = os.path.join(ROOT, "sound_names.csv")
    subs_by_handle_path = os.path.join(ROOT, "subtitles_by_handle.txt")
    subs_by_name_path = os.path.join(ROOT, "subtitles_by_name.txt")
    mobile_sentences_path = os.path.join(ROOT, "subtitles_ko_sentences.json")
    hash_map_path = os.path.join(ROOT, "sound_hash_map.tsv")

    matrix_path = pick_latest("static_audio_full_domain_handle_matrix_*_v2.tsv")
    conflict_path = pick_latest("static_conflict_priority_rules_*.tsv")

    for req in (
        sound_names_path,
        subs_by_handle_path,
        subs_by_name_path,
        mobile_sentences_path,
        matrix_path,
        conflict_path,
    ):
        if not os.path.exists(req):
            raise FileNotFoundError(f"missing required input: {req}")

    sound_names = load_sound_names(sound_names_path)
    subs_by_handle = load_subtitles_by_handle(subs_by_handle_path)
    subs_by_name_exact, subs_by_name_norm, subs_name_norm_variants = load_subtitles_by_name(subs_by_name_path)
    mobile_map = load_mobile_sentences(mobile_sentences_path)
    matrix_rows = load_tsv_dicts(matrix_path)
    matrix_by_handle = {int(r["handle"]): r for r in matrix_rows if (r.get("handle") or "").isdigit()}
    raw_policy, plus_incoming_policy, raw_allow, raw_block, allow_pairs = load_conflict_rules(conflict_path)
    hash_collision_handles, hash_collisions = load_hash_collision_handles(hash_map_path)

    out_master = os.path.join(ROOT, f"static_kr_patch_handle_master_{date_tag}.tsv")
    out_dialogue = os.path.join(ROOT, f"static_kr_patch_dialogue_plan_{date_tag}.tsv")
    out_review = os.path.join(ROOT, f"static_kr_patch_review_surface_{date_tag}.tsv")
    out_uncovered = os.path.join(ROOT, f"static_kr_patch_uncovered_{date_tag}.tsv")
    out_summary = os.path.join(ROOT, f"static_kr_patch_summary_{date_tag}.txt")

    master_rows: list[dict[str, str]] = []
    review_rows: list[dict[str, str]] = []
    dialogue_rows: list[dict[str, str]] = []
    uncovered_rows: list[dict[str, str]] = []

    source_counts: Counter[str] = Counter()
    dialogue_source_counts: Counter[str] = Counter()
    stream_source_counts: Counter[str] = Counter()
    flag_counts: Counter[str] = Counter()

    for handle in sorted(sound_names.keys()):
        name = sound_names.get(handle, "")
        mrow = matrix_by_handle.get(handle, {})
        hrec = subs_by_handle.get(handle)
        nrec_exact = subs_by_name_exact.get(name)
        nrec_norm = subs_by_name_norm.get(norm_name(name))
        mobile = mobile_map.get(handle)

        has_handle_sub = hrec is not None
        has_name_exact = nrec_exact is not None
        has_name_norm = nrec_norm is not None
        has_mobile = mobile is not None and int(mobile.get("sentence_count", "0") or 0) > 0

        selected_source = "none"
        selected_text = ""
        selected_color = ""
        selected_speaker = ""
        source_confidence = "none"

        if has_handle_sub:
            selected_source = "handle"
            selected_text = hrec.get("text", "")
            selected_color = hrec.get("color", "")
            source_confidence = "high"
        elif has_name_exact:
            selected_source = "name_exact"
            selected_text = nrec_exact.get("text", "")
            selected_color = nrec_exact.get("color", "")
            source_confidence = "high"
        elif has_name_norm:
            selected_source = "name_norm"
            selected_text = nrec_norm.get("text", "")
            selected_color = nrec_norm.get("color", "")
            source_confidence = "medium"
        elif has_mobile:
            selected_source = "mobile_source_index"
            selected_text = mobile.get("text", "")
            selected_color = mobile.get("color", "")
            selected_speaker = mobile.get("speaker", "")
            source_confidence = "medium"

        dialogue_class = mrow.get("dialogue_class", "")
        stream_expected = mrow.get("stream_expected", "")
        matrix_source = source_from_matrix(mrow.get("effective_subtitle_source", ""))
        matrix_name = mrow.get("name", "")

        risk_flags: list[str] = []
        if matrix_name and matrix_name != name:
            risk_flags.append("name_mismatch_between_matrix_and_sound_table")
        if dialogue_class == "dialogue" and selected_source == "none":
            risk_flags.append("dialogue_uncovered")
        if stream_expected == "1":
            risk_flags.append("stream_expected")
            if selected_source not in ("handle", "name_exact", "name_norm"):
                risk_flags.append("stream_expected_non_pc_source")
        if handle in hash_collision_handles:
            risk_flags.append("hash_collision")
        if handle in raw_allow:
            risk_flags.append("raw_plus_allow_shift")
        if handle in raw_block:
            risk_flags.append("raw_plus_block_shift")
        if plus_incoming_policy.get(handle) == "ALLOW_SHIFT_TO_PLUS":
            risk_flags.append("plus_incoming_allow_shift")
        if plus_incoming_policy.get(handle, "").startswith("BLOCK"):
            risk_flags.append("plus_incoming_block_shift")
        if (
            hrec is not None
            and nrec_exact is not None
            and norm_text(hrec.get("text", "")) != norm_text(nrec_exact.get("text", ""))
        ):
            risk_flags.append("handle_name_text_diverge")
        if dialogue_class != "dialogue" and selected_source != "none":
            risk_flags.append("non_dialogue_has_subtitle")
        if dialogue_class == "dialogue" and mrow.get("container_class", "") == "mp3_frame_in_wav_ext":
            risk_flags.append("dialogue_mp3_frame_container")
        if len(subs_name_norm_variants.get(norm_name(name), [])) > 1:
            risk_flags.append("name_norm_variant_collision")
        if matrix_source == "handle" and selected_source != "handle":
            risk_flags.append("matrix_source_drift_from_handle")
        if matrix_source == "name_like" and selected_source not in ("name_exact", "name_norm"):
            risk_flags.append("matrix_source_drift_from_name")
        if matrix_source == "none" and selected_source != "none":
            risk_flags.append("matrix_none_but_text_present")

        needs_manual_translation = 1 if dialogue_class == "dialogue" and selected_source == "none" else 0
        review_keys = {
            "dialogue_uncovered",
            "stream_expected_non_pc_source",
            "hash_collision",
            "matrix_source_drift_from_handle",
            "matrix_source_drift_from_name",
            "name_mismatch_between_matrix_and_sound_table",
            "name_norm_variant_collision",
        }
        if stream_expected == "1":
            review_keys.update(
                {
                    "raw_plus_allow_shift",
                    "raw_plus_block_shift",
                    "plus_incoming_allow_shift",
                    "plus_incoming_block_shift",
                }
            )
        needs_manual_review = 1 if any(flag in review_keys for flag in risk_flags) else 0
        needs_runtime_validation = 1 if (stream_expected == "1" or "hash_collision" in risk_flags) else 0

        mobile_sentence_count = "0"
        mobile_speaker = ""
        if mobile is not None:
            mobile_sentence_count = mobile.get("sentence_count", "0")
            mobile_speaker = mobile.get("speaker", "")

        row = {
            "handle": str(handle),
            "name": name,
            "dialogue_class": dialogue_class,
            "stream_expected": stream_expected,
            "container_class": mrow.get("container_class", ""),
            "effective_subtitle_source_matrix": mrow.get("effective_subtitle_source", ""),
            "has_subtitle_by_handle": "1" if has_handle_sub else "0",
            "has_subtitle_by_name_exact": "1" if has_name_exact else "0",
            "has_subtitle_by_name_norm": "1" if has_name_norm else "0",
            "mobile_sentence_count": mobile_sentence_count,
            "mobile_speaker": mobile_speaker,
            "selected_source": selected_source,
            "selected_text": selected_text,
            "selected_color": selected_color,
            "selected_speaker": selected_speaker,
            "source_confidence": source_confidence,
            "risk_flags": "|".join(risk_flags),
            "needs_manual_translation": str(needs_manual_translation),
            "needs_manual_review": str(needs_manual_review),
            "needs_runtime_validation": str(needs_runtime_validation),
            "route_rows_observed": mrow.get("route_rows_observed", ""),
            "route_frames": mrow.get("route_frames", ""),
            "route_groups": mrow.get("route_groups", ""),
            "route_action_nums": mrow.get("route_action_nums", ""),
            "route_action_names": mrow.get("route_action_names", ""),
            "dispatch_targets": mrow.get("dispatch_targets", ""),
            "route_callees": mrow.get("route_callees", ""),
            "expected_runtime_path": mrow.get("expected_runtime_path", ""),
            "static_confidence": mrow.get("static_confidence", ""),
        }
        master_rows.append(row)

        source_counts[selected_source] += 1
        if dialogue_class == "dialogue":
            dialogue_source_counts[selected_source] += 1
        if stream_expected == "1":
            stream_source_counts[selected_source] += 1
        for f in risk_flags:
            flag_counts[f] += 1

        if needs_manual_review:
            review_rows.append(row)
        if selected_source == "none":
            uncovered_rows.append(row)
        if dialogue_class == "dialogue":
            dialogue_rows.append(row)

    fields = [
        "handle",
        "name",
        "dialogue_class",
        "stream_expected",
        "container_class",
        "effective_subtitle_source_matrix",
        "has_subtitle_by_handle",
        "has_subtitle_by_name_exact",
        "has_subtitle_by_name_norm",
        "mobile_sentence_count",
        "mobile_speaker",
        "selected_source",
        "selected_text",
        "selected_color",
        "selected_speaker",
        "source_confidence",
        "risk_flags",
        "needs_manual_translation",
        "needs_manual_review",
        "needs_runtime_validation",
        "route_rows_observed",
        "route_frames",
        "route_groups",
        "route_action_nums",
        "route_action_names",
        "dispatch_targets",
        "route_callees",
        "expected_runtime_path",
        "static_confidence",
    ]

    for path, rows in (
        (out_master, master_rows),
        (out_dialogue, dialogue_rows),
        (out_review, review_rows),
        (out_uncovered, uncovered_rows),
    ):
        with open(path, "w", encoding="utf-8", newline="") as f:
            w = csv.DictWriter(f, fieldnames=fields, delimiter="\t")
            w.writeheader()
            w.writerows(rows)

    dialogue_total = sum(1 for r in master_rows if r["dialogue_class"] == "dialogue")
    stream_total = sum(1 for r in master_rows if r["stream_expected"] == "1")
    dialogue_uncovered = sum(
        1
        for r in master_rows
        if r["dialogue_class"] == "dialogue" and r["selected_source"] == "none"
    )
    covered_any = sum(1 for r in master_rows if r["selected_source"] != "none")
    hash_collision_dialogue = [
        r for r in master_rows if r["dialogue_class"] == "dialogue" and "hash_collision" in r["risk_flags"]
    ]

    with open(out_summary, "w", encoding="utf-8") as f:
        f.write("Static KR patch readiness summary (runtime-free)\n")
        f.write(f"date_tag: {date_tag}\n")
        f.write(f"matrix_source: {os.path.basename(matrix_path)}\n")
        f.write(f"conflict_source: {os.path.basename(conflict_path)}\n")
        f.write("\n")
        f.write(f"handles_total: {len(master_rows)}\n")
        f.write(f"dialogue_total: {dialogue_total}\n")
        f.write(f"stream_expected_total: {stream_total}\n")
        f.write(f"covered_any_source: {covered_any}\n")
        f.write(f"uncovered_total: {len(uncovered_rows)}\n")
        f.write(f"dialogue_uncovered: {dialogue_uncovered}\n")
        f.write("\n")
        f.write(
            "source_breakdown_total: "
            + ", ".join(f"{k}={v}" for k, v in sorted(source_counts.items()))
            + "\n"
        )
        f.write(
            "source_breakdown_dialogue: "
            + ", ".join(f"{k}={v}" for k, v in sorted(dialogue_source_counts.items()))
            + "\n"
        )
        f.write(
            "source_breakdown_stream_expected: "
            + ", ".join(f"{k}={v}" for k, v in sorted(stream_source_counts.items()))
            + "\n"
        )
        f.write("\n")
        f.write(f"review_rows: {len(review_rows)}\n")
        f.write("top_risk_flags:\n")
        for key, val in flag_counts.most_common(20):
            f.write(f"- {key}: {val}\n")
        f.write("\n")
        f.write(f"hash_collision_keys: {len(hash_collisions)}\n")
        f.write(f"hash_collision_handles_total: {len(hash_collision_handles)}\n")
        f.write(f"hash_collision_dialogue_rows: {len(hash_collision_dialogue)}\n")
        if hash_collisions:
            f.write("hash_collision_examples:\n")
            for key, handles in hash_collisions[:10]:
                f.write(
                    f"- hash={key[0]} bytes={key[1]} rate={key[2]} ch={key[3]} bits={key[4]} handles={handles}\n"
                )
        f.write("\n")
        f.write(f"raw_plus_allow_pairs_total: {len(allow_pairs)}\n")
        if allow_pairs:
            f.write(
                "raw_plus_allow_pairs: "
                + ", ".join(f"{a}->{b}" for a, b in allow_pairs)
                + "\n"
            )
        f.write("\n")
        f.write("notes:\n")
        f.write("- Text assets decode cleanly as UTF-8/UTF-8-SIG; console mojibake is display-layer, not file corruption.\n")
        f.write("- Use review_surface rows as static-first queue for Korean patch safety checks.\n")

    print("out_master", out_master)
    print("out_dialogue", out_dialogue)
    print("out_review", out_review)
    print("out_uncovered", out_uncovered)
    print("out_summary", out_summary)
    print("handles_total", len(master_rows))
    print("dialogue_total", dialogue_total)
    print("dialogue_uncovered", dialogue_uncovered)
    print("review_rows", len(review_rows))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
