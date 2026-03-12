#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Build a static "gap closure" report for subtitle-matching readiness.

This script consolidates static artifacts and classifies each concern as:
- CLOSED: statically resolved
- GAP: static data gap remains
- RUNTIME_ONLY: cannot be finalized without runtime evidence
"""

from __future__ import annotations

import csv
import datetime as dt
import glob
import os
import re
from collections import defaultdict
from dataclasses import dataclass


@dataclass
class CheckRow:
    check_id: str
    status: str
    metric: str
    details: str
    evidence: str


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


def read_tsv_dicts(path: str) -> list[dict[str, str]]:
    with open(path, "r", encoding="utf-8-sig", errors="replace", newline="") as f:
        return list(csv.DictReader(f, delimiter="\t"))


def read_lines(path: str) -> list[str]:
    with open(path, "r", encoding="utf-8-sig", errors="replace") as f:
        return [line.rstrip("\n") for line in f]


def latest_classification(root: str) -> str:
    candidates = glob.glob(os.path.join(root, "static_audio_ingress_classification_*.tsv"))
    if not candidates:
        raise FileNotFoundError("static_audio_ingress_classification_*.tsv not found")
    return max(candidates, key=os.path.getmtime)


def load_sound_names(path: str) -> dict[int, str]:
    out: dict[int, str] = {}
    with open(path, "r", encoding="utf-8-sig", errors="replace", newline="") as f:
        r = csv.reader(f)
        next(r, None)
        for row in r:
            if not row:
                continue
            try:
                h = int(row[0].strip())
            except Exception:
                continue
            name = (row[1] if len(row) > 1 else "").strip()
            if name:
                out[h] = name
    return out


def load_handle_subs(path: str) -> set[int]:
    out: set[int] = set()
    for line in read_lines(path):
        m = re.match(r"^\s*(\d+)\t", line)
        if m:
            out.add(int(m.group(1)))
    return out


def load_name_subs(path: str) -> tuple[set[str], set[str]]:
    raw: set[str] = set()
    norm: set[str] = set()
    for line in read_lines(path):
        if not line:
            continue
        m = re.match(r"^([^\t]+)\t", line)
        if not m:
            continue
        name = m.group(1)
        raw.add(name)
        n = norm_name(name)
        if n:
            norm.add(n)
    return raw, norm


def load_hash_rows(path: str) -> list[tuple[str, int, int, int, int, int]]:
    rows: list[tuple[str, int, int, int, int, int]] = []
    if not os.path.exists(path):
        return rows
    for line in read_lines(path):
        if not line or line.startswith("#"):
            continue
        cols = line.split("\t")
        if len(cols) < 6:
            continue
        try:
            rows.append(
                (
                    cols[0],
                    int(cols[1]),
                    int(cols[2]),
                    int(cols[3]),
                    int(cols[4]),
                    int(cols[5]),
                )
            )
        except Exception:
            continue
    return rows


def main() -> int:
    root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    date_tag = dt.datetime.now().strftime("%Y%m%d")

    cls_path = latest_classification(root)
    sound_names_path = os.path.join(root, "sound_names.csv")
    subs_handle_path = os.path.join(root, "subtitles_by_handle.txt")
    subs_name_path = os.path.join(root, "subtitles_by_name.txt")
    hash_active_path = os.path.join(root, "sound_hash_map.tsv")
    hash_full_path = os.path.join(root, "sound_hash_map_new.tsv")
    stream_map_path = os.path.join(root, "sound_stream_chunk_map.tsv")
    runtime_unknowns_path = os.path.join(
        root, "_static_remaining_runtime_only_unknowns_matrix_20260219.tsv"
    )
    unknown347_path = os.path.join(root, "_static_unknown347_composition_profile_20260219.tsv")

    required = [sound_names_path, subs_handle_path, subs_name_path, hash_active_path, stream_map_path]
    for p in required:
        if not os.path.exists(p):
            raise FileNotFoundError(f"required input missing: {p}")

    cls_rows = read_tsv_dicts(cls_path)
    sound_names = load_sound_names(sound_names_path)
    handle_subs = load_handle_subs(subs_handle_path)
    name_subs_raw, name_subs_norm = load_name_subs(subs_name_path)
    hash_active_rows = load_hash_rows(hash_active_path)
    hash_full_rows = load_hash_rows(hash_full_path)
    stream_rows = load_hash_rows(stream_map_path)

    hash_active_handles = {r[5] for r in hash_active_rows}
    hash_full_handles = {r[5] for r in hash_full_rows}
    stream_handles = {r[5] for r in stream_rows}

    dialogue_rows = [r for r in cls_rows if r.get("dialogue_class") == "dialogue"]
    stream_expected_rows = [r for r in cls_rows if r.get("stream_expected") == "1"]
    handunit_rows = [r for r in cls_rows if re.search(r"handunit", r.get("name", ""), re.IGNORECASE)]
    baby_like_rows = [r for r in cls_rows if re.search(r"baby", r.get("name", ""), re.IGNORECASE)]

    dialogue_uncovered: list[tuple[int, str]] = []
    for r in dialogue_rows:
        h = int(r["handle"])
        name = r["name"]
        covered = (h in handle_subs) or (name in name_subs_raw) or (norm_name(name) in name_subs_norm)
        if not covered:
            dialogue_uncovered.append((h, name))

    dialogue_no_active_hash = [
        (int(r["handle"]), r["name"]) for r in dialogue_rows if int(r["handle"]) not in hash_active_handles
    ]
    stream_expected_no_chunk = [
        (int(r["handle"]), r["name"]) for r in stream_expected_rows if int(r["handle"]) not in stream_handles
    ]

    handunit_path2_count = sum(
        1 for r in handunit_rows if "PATH2_stream_expected_unlock_fallback" in r.get("expected_runtime_path", "")
    )
    stream_expected_non_baby_count = sum(
        1 for r in stream_expected_rows if not re.search(r"baby", r.get("name", ""), re.IGNORECASE)
    )
    high_conf_count = sum(1 for r in cls_rows if r.get("static_confidence") == "high")

    hash_key_to_handles: dict[tuple[str, int, int, int, int], set[int]] = defaultdict(set)
    for hhex, nbytes, rate, ch, bits, handle in hash_active_rows:
        hash_key_to_handles[(hhex, nbytes, rate, ch, bits)].add(handle)
    active_hash_collision_count = sum(1 for hs in hash_key_to_handles.values() if len(hs) > 1)

    dialogue_handles = {int(r["handle"]) for r in dialogue_rows}
    dialogue_collision_keys = 0
    for handles in hash_key_to_handles.values():
        if len(handles) <= 1:
            continue
        if any(h in dialogue_handles for h in handles):
            dialogue_collision_keys += 1

    unknown347_total = None
    if os.path.exists(unknown347_path):
        rows = read_tsv_dicts(unknown347_path)
        unknown347_total = sum(int(r.get("count_in_mfa", "0") or 0) for r in rows)

    runtime_only_count = 0
    if os.path.exists(runtime_unknowns_path):
        runtime_only_count = max(0, len(read_tsv_dicts(runtime_unknowns_path)))

    checks: list[CheckRow] = []
    checks.append(
        CheckRow(
            "C1_CLASSIFICATION_COMPLETENESS",
            "CLOSED" if len(cls_rows) == len(sound_names) and high_conf_count == len(cls_rows) else "GAP",
            f"rows={len(cls_rows)}, sound_names={len(sound_names)}, high_conf={high_conf_count}",
            "Ingress classification rows align with sound table and confidence is fully high.",
            os.path.basename(cls_path),
        )
    )
    checks.append(
        CheckRow(
            "C2_HANDUNIT_PATH2_SCOPE",
            "CLOSED" if handunit_path2_count == 0 else "GAP",
            f"handunit_rows={len(handunit_rows)}, handunit_path2={handunit_path2_count}",
            "HandUnit should stay on PATH1 only (non-stream).",
            os.path.basename(cls_path),
        )
    )
    checks.append(
        CheckRow(
            "C3_STREAM_EXPECTED_SCOPE",
            "CLOSED" if stream_expected_non_baby_count == 0 else "GAP",
            f"stream_expected={len(stream_expected_rows)}, stream_expected_non_baby={stream_expected_non_baby_count}, baby_like_rows={len(baby_like_rows)}",
            "stream_expected scope should stay inside Baby-like family.",
            os.path.basename(cls_path),
        )
    )
    checks.append(
        CheckRow(
            "C4_DIALOGUE_SUBTITLE_COVERAGE",
            "CLOSED" if not dialogue_uncovered else "GAP",
            f"dialogue_total={len(dialogue_rows)}, uncovered={len(dialogue_uncovered)}",
            "Dialogue handles must be covered by handle-subtitle or name-subtitle.",
            "subtitles_by_handle.txt;subtitles_by_name.txt;sound_names.csv",
        )
    )
    checks.append(
        CheckRow(
            "C5_ACTIVE_HASH_COVERAGE_DIALOGUE",
            "CLOSED" if not dialogue_no_active_hash else "GAP",
            f"dialogue_total={len(dialogue_rows)}, active_hash_missing={len(dialogue_no_active_hash)}",
            "Missing active hash entries are static data gaps for hash-first matching.",
            "sound_hash_map.tsv",
        )
    )
    if hash_full_rows:
        checks.append(
            CheckRow(
                "C6_FULL_HASH_SUPERSET_CHECK",
                "CLOSED" if len(hash_full_handles) >= len(hash_active_handles) else "GAP",
                f"active_handles={len(hash_active_handles)}, full_handles={len(hash_full_handles)}",
                "sound_hash_map_new.tsv is checked as static candidate superset.",
                "sound_hash_map.tsv;sound_hash_map_new.tsv",
            )
        )
    checks.append(
        CheckRow(
            "C7_STREAM_CHUNK_COVERAGE",
            "CLOSED" if not stream_expected_no_chunk else "GAP",
            f"stream_expected_total={len(stream_expected_rows)}, chunk_missing={len(stream_expected_no_chunk)}",
            "stream_expected handles should have chunk-map support to avoid stream miss bias.",
            "sound_stream_chunk_map.tsv",
        )
    )
    checks.append(
        CheckRow(
            "C8_ACTIVE_HASH_COLLISION",
            "CLOSED" if dialogue_collision_keys == 0 else "GAP",
            f"collision_keys_total={active_hash_collision_count}, collision_keys_dialogue={dialogue_collision_keys}",
            "Only dialogue-touching collisions are treated as matching-risk gaps.",
            "sound_hash_map.tsv",
        )
    )
    if unknown347_total is not None:
        checks.append(
            CheckRow(
                "C9_UNKNOWN347_CONTROL_DECOMP",
                "CLOSED" if unknown347_total == 347 else "GAP",
                f"unknown347_total={unknown347_total}",
                "Previously unknown action-route mass should stay fully decomposed into control-only actions.",
                os.path.basename(unknown347_path),
            )
        )
    if runtime_only_count > 0:
        checks.append(
            CheckRow(
                "C10_RUNTIME_ONLY_ITEMS",
                "RUNTIME_ONLY",
                f"runtime_only_items={runtime_only_count}",
                "These items are explicitly outside static-only closure.",
                os.path.basename(runtime_unknowns_path),
            )
        )

    out_matrix = os.path.join(root, f"_static_gap_closure_matrix_{date_tag}.tsv")
    with open(out_matrix, "w", encoding="utf-8", newline="") as f:
        w = csv.writer(f, delimiter="\t")
        w.writerow(["check_id", "status", "metric", "details", "evidence"])
        for c in checks:
            w.writerow([c.check_id, c.status, c.metric, c.details, c.evidence])

    out_handles = os.path.join(root, f"_static_gap_handles_{date_tag}.tsv")
    with open(out_handles, "w", encoding="utf-8", newline="") as f:
        w = csv.writer(f, delimiter="\t")
        w.writerow(["gap_type", "handle", "name"])
        for h, n in sorted(dialogue_uncovered):
            w.writerow(["dialogue_uncovered_subtitle", h, n])
        for h, n in sorted(dialogue_no_active_hash):
            w.writerow(["dialogue_missing_active_hash", h, n])
        for h, n in sorted(stream_expected_no_chunk):
            w.writerow(["stream_expected_missing_chunk", h, n])

    summary_lines = []
    summary_lines.append("Static gap closure summary")
    summary_lines.append(f"Date: {date_tag}")
    summary_lines.append(f"Classification source: {os.path.basename(cls_path)}")
    summary_lines.append("")
    status_counts: dict[str, int] = defaultdict(int)
    for c in checks:
        status_counts[c.status] += 1
    summary_lines.append(
        "Status counts: "
        + ", ".join(f"{k}={status_counts.get(k, 0)}" for k in ("CLOSED", "GAP", "RUNTIME_ONLY"))
    )
    summary_lines.append("")
    summary_lines.append("Static gaps:")
    if not dialogue_uncovered and not dialogue_no_active_hash and not stream_expected_no_chunk and dialogue_collision_keys == 0:
        summary_lines.append("- none")
    else:
        summary_lines.append(f"- dialogue_uncovered_subtitle: {len(dialogue_uncovered)}")
        summary_lines.append(f"- dialogue_missing_active_hash: {len(dialogue_no_active_hash)}")
        summary_lines.append(f"- stream_expected_missing_chunk: {len(stream_expected_no_chunk)}")
        summary_lines.append(
            f"- active_hash_collision_keys_dialogue: {dialogue_collision_keys} (total_keys={active_hash_collision_count})"
        )
    summary_lines.append("")
    summary_lines.append("Runtime-only items:")
    summary_lines.append(f"- {runtime_only_count}")

    out_closure = os.path.join(root, f"_static_gap_closure_summary_{date_tag}.txt")
    with open(out_closure, "w", encoding="utf-8") as f:
        f.write("\n".join(summary_lines) + "\n")

    print(f"out_matrix\t{out_matrix}")
    print(f"out_handles\t{out_handles}")
    print(f"out_summary\t{out_closure}")
    print(f"checks\t{len(checks)}")
    print(f"gaps\t{sum(1 for c in checks if c.status == 'GAP')}")
    print(f"runtime_only\t{sum(1 for c in checks if c.status == 'RUNTIME_ONLY')}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
