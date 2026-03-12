#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Build a handle-centric static audio truth table from existing reverse/static artifacts.

Outputs:
  - static_audio_truth_table_YYYYMMDD.tsv
  - static_audio_truth_summary_YYYYMMDD.txt
"""

from __future__ import annotations

import csv
import datetime as dt
import fnmatch
import glob
import json
import os
import re
from collections import defaultdict
from typing import Dict, Iterable, List, Set, Tuple


ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def latest(pattern: str) -> str:
    paths = glob.glob(os.path.join(ROOT, pattern))
    # glob() ignores dotfiles unless pattern itself starts with '.'
    if not paths and os.sep not in pattern and "/" not in pattern:
        for name in os.listdir(ROOT):
            if fnmatch.fnmatch(name, pattern):
                paths.append(os.path.join(ROOT, name))
    if not paths:
        raise FileNotFoundError(f"missing required artifact: {pattern}")
    return max(paths, key=os.path.getmtime)


def to_int(v: object, default: int = 0) -> int:
    try:
        return int(str(v).strip().strip('"'))
    except Exception:
        return default


def one_line(text: object) -> str:
    out = str(text or "").replace("\r", " ").replace("\n", " | ").replace("\t", " ")
    out = re.sub(r"\s+", " ", out).strip()
    return out


def norm_name(text: str) -> str:
    out: List[str] = []
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


def read_tsv(path: str) -> List[Dict[str, str]]:
    with open(path, "r", encoding="utf-8-sig", errors="replace", newline="") as f:
        return list(csv.DictReader(f, delimiter="\t"))


def load_sound_names(path: str) -> Dict[int, str]:
    out: Dict[int, str] = {}
    with open(path, "r", encoding="utf-8-sig", errors="replace", newline="") as f:
        rows = csv.DictReader(f)
        for row in rows:
            h = to_int(row.get("handle", ""), -1)
            if h < 0:
                continue
            name = one_line(row.get("name", ""))
            if name:
                out[h] = name
    return out


def load_subtitles_by_handle(path: str) -> Set[int]:
    out: Set[int] = set()
    with open(path, "r", encoding="utf-8-sig", errors="replace") as f:
        for line in f:
            cols = line.rstrip("\n").split("\t")
            if len(cols) < 4:
                continue
            h = to_int(cols[0], -1)
            if h >= 0:
                out.add(h)
    return out


def load_subtitles_by_name_norm(path: str) -> Dict[str, Dict[str, str]]:
    out: Dict[str, Dict[str, str]] = {}
    with open(path, "r", encoding="utf-8-sig", errors="replace") as f:
        for line in f:
            cols = line.rstrip("\n").split("\t")
            if len(cols) < 4:
                continue
            name = one_line(cols[0])
            if not name:
                continue
            n = norm_name(name)
            if not n or n in out:
                continue
            out[n] = {
                "name": name,
                "duration": one_line(cols[2]),
                "text": one_line(cols[3]),
            }
    return out


def load_mobile_handles(path: str) -> Set[int]:
    out: Set[int] = set()
    if not os.path.exists(path):
        return out
    with open(path, "r", encoding="utf-8-sig", errors="replace") as f:
        data = json.load(f)
    if not isinstance(data, list):
        return out
    for entry in data:
        if not isinstance(entry, dict):
            continue
        h = to_int(entry.get("source_index", ""), -1)
        if h >= 0:
            out.add(h)
    return out


def parse_hash_table(path: str) -> Tuple[Dict[int, int], Dict[int, Set[str]], Dict[int, Tuple[int, int]], Dict[int, int]]:
    by_handle_count: Dict[int, int] = defaultdict(int)
    by_handle_formats: Dict[int, Set[str]] = defaultdict(set)
    by_handle_minmax: Dict[int, Tuple[int, int]] = {}
    key_to_handles: Dict[Tuple[str, int, int, int, int], Set[int]] = defaultdict(set)

    with open(path, "r", encoding="utf-8-sig", errors="replace") as f:
        for raw in f:
            line = raw.strip()
            if not line or line.startswith("#"):
                continue
            cols = line.split("\t")
            if len(cols) < 6:
                continue
            h = to_int(cols[5], -1)
            if h < 0:
                continue
            nbytes = to_int(cols[1], 0)
            rate = to_int(cols[2], 0)
            ch = to_int(cols[3], 0)
            bits = to_int(cols[4], 0)
            key = (cols[0], nbytes, rate, ch, bits)
            key_to_handles[key].add(h)

            by_handle_count[h] += 1
            by_handle_formats[h].add(f"{rate}/{ch}/{bits}")
            prev = by_handle_minmax.get(h)
            if prev is None:
                by_handle_minmax[h] = (nbytes, nbytes)
            else:
                by_handle_minmax[h] = (min(prev[0], nbytes), max(prev[1], nbytes))

    collision_keys_by_handle: Dict[int, int] = defaultdict(int)
    for hs in key_to_handles.values():
        if len(hs) <= 1:
            continue
        for h in hs:
            collision_keys_by_handle[h] += 1

    return by_handle_count, by_handle_formats, by_handle_minmax, collision_keys_by_handle


def parse_chunk_table(path: str) -> Tuple[Dict[int, int], Dict[int, Set[str]], Dict[int, Tuple[int, int]]]:
    by_handle_count: Dict[int, int] = defaultdict(int)
    by_handle_formats: Dict[int, Set[str]] = defaultdict(set)
    by_handle_minmax: Dict[int, Tuple[int, int]] = {}
    with open(path, "r", encoding="utf-8-sig", errors="replace") as f:
        for raw in f:
            line = raw.strip()
            if not line or line.startswith("#"):
                continue
            cols = line.split("\t")
            if len(cols) < 6:
                continue
            h = to_int(cols[5], -1)
            if h < 0:
                continue
            nbytes = to_int(cols[1], 0)
            rate = to_int(cols[2], 0)
            ch = to_int(cols[3], 0)
            bits = to_int(cols[4], 0)
            by_handle_count[h] += 1
            by_handle_formats[h].add(f"{rate}/{ch}/{bits}")
            prev = by_handle_minmax.get(h)
            if prev is None:
                by_handle_minmax[h] = (nbytes, nbytes)
            else:
                by_handle_minmax[h] = (min(prev[0], nbytes), max(prev[1], nbytes))
    return by_handle_count, by_handle_formats, by_handle_minmax


def collect_routes(play_routes_path: str) -> Tuple[Dict[int, int], Dict[int, Set[str]], Dict[int, Set[str]]]:
    rows = read_tsv(play_routes_path)
    by_handle_count: Dict[int, int] = defaultdict(int)
    by_handle_frames: Dict[int, Set[str]] = defaultdict(set)
    by_handle_actions: Dict[int, Set[str]] = defaultdict(set)
    for row in rows:
        sample = to_int(row.get("sample_handle", ""), -1)
        if sample < 0:
            continue
        handle = sample + 1
        by_handle_count[handle] += 1
        frame = one_line(row.get("frame", ""))
        group = one_line(row.get("group_index", ""))
        if frame:
            by_handle_frames[handle].add(f"{frame}:g{group}" if group else frame)
        action = one_line(row.get("action_num", ""))
        if action:
            by_handle_actions[handle].add(action)
    return by_handle_count, by_handle_frames, by_handle_actions


def join_sorted(vals: Iterable[str], limit: int = 6) -> str:
    arr = sorted(set(v for v in vals if v))
    if len(arr) > limit:
        arr = arr[:limit] + ["..."]
    return ",".join(arr)


def main() -> int:
    date_tag = dt.datetime.now().strftime("%Y%m%d")

    sound_names_path = os.path.join(ROOT, "sound_names.csv")
    subs_by_handle_path = os.path.join(ROOT, "subtitles_by_handle.txt")
    subs_by_name_path = os.path.join(ROOT, "subtitles_by_name.txt")
    mobile_json_path = os.path.join(ROOT, "subtitles_ko_sentences.json")
    hash_map_path = os.path.join(ROOT, "sound_hash_map.tsv")
    chunk_map_path = os.path.join(ROOT, "sound_stream_chunk_map.tsv")

    ingress_path = latest("static_audio_ingress_classification_*.tsv")
    semantics_path = latest("*all_handles_effective_subtitle_semantics*.tsv")
    play_routes_path = latest("*mfa_play_routes_numeric*.tsv")

    sound_names = load_sound_names(sound_names_path)
    subs_by_handle = load_subtitles_by_handle(subs_by_handle_path)
    subs_by_name_norm = load_subtitles_by_name_norm(subs_by_name_path)
    mobile_handles = load_mobile_handles(mobile_json_path)

    hash_count, hash_formats, hash_minmax, hash_collision_keys = parse_hash_table(hash_map_path)
    chunk_count, chunk_formats, chunk_minmax = parse_chunk_table(chunk_map_path)

    ingress_rows = read_tsv(ingress_path)
    semantics_rows = read_tsv(semantics_path)
    route_count, route_frames, route_actions = collect_routes(play_routes_path)

    ingress_by_handle: Dict[int, Dict[str, str]] = {}
    for row in ingress_rows:
        h = to_int(row.get("handle", ""), -1)
        if h >= 0:
            ingress_by_handle[h] = row

    semantics_by_handle: Dict[int, Dict[str, str]] = {}
    for row in semantics_rows:
        h = to_int(row.get("handle", ""), -1)
        if h >= 0:
            semantics_by_handle[h] = row

    handles: Set[int] = set(sound_names.keys())
    handles.update(ingress_by_handle.keys())
    handles.update(semantics_by_handle.keys())
    handles.update(hash_count.keys())
    handles.update(chunk_count.keys())
    handles.update(route_count.keys())

    out_rows: List[Dict[str, str]] = []
    for h in sorted(handles):
        name = sound_names.get(h, "")
        nname = norm_name(name)
        sem = semantics_by_handle.get(h, {})
        ing = ingress_by_handle.get(h, {})

        sem_is_dialogue = to_int(sem.get("is_dialogue", "-1"), -1)
        sem_is_sound_only = to_int(sem.get("is_sound_only", "-1"), -1)
        sem_effective_source = one_line(sem.get("effective_source", ""))

        has_sub_handle = 1 if h in subs_by_handle else 0
        has_sub_name = 1 if (nname and nname in subs_by_name_norm) else 0
        has_mobile = 1 if h in mobile_handles else 0
        has_any_sub = 1 if (has_sub_handle or has_sub_name or has_mobile) else 0
        has_dialogue_sub = 1 if (has_any_sub and sem_is_dialogue == 1 and sem_is_sound_only != 1) else 0

        dialog_class = one_line(ing.get("dialogue_class", sem.get("is_dialogue", "")))
        stream_expected = to_int(ing.get("stream_expected", sem.get("stream_expected", "0")), 0)
        expected_path = one_line(ing.get("expected_runtime_path", ""))
        expected_backend = one_line(ing.get("expected_backend", ""))
        confidence = one_line(ing.get("static_confidence", ""))

        h_hash_count = hash_count.get(h, 0)
        h_chunk_count = chunk_count.get(h, 0)
        h_collision_keys = hash_collision_keys.get(h, 0)
        h_hash_min, h_hash_max = hash_minmax.get(h, (0, 0))
        h_chunk_min, h_chunk_max = chunk_minmax.get(h, (0, 0))

        frames = route_frames.get(h, set())
        menu_surface = 1 if any("title screen" in f.lower() for f in frames) else 0

        risk_notes: List[str] = []
        if has_dialogue_sub and menu_surface:
            risk_notes.append("menu_surface_has_subtitle")
        if has_dialogue_sub and h_hash_count == 0 and h_chunk_count == 0:
            risk_notes.append("no_fingerprint_entry")
        if h_collision_keys > 0:
            risk_notes.append("hash_collision_key")
        if has_dialogue_sub and stream_expected == 1:
            risk_notes.append("stream_expected_runtime_sensitive")
        if has_dialogue_sub and h_chunk_count > 0 and h_chunk_min and h_chunk_min < 4096:
            risk_notes.append("tiny_chunk_signature_present")

        out_rows.append(
            {
                "handle": str(h),
                "name": name,
                "subtitle_handle": str(has_sub_handle),
                "subtitle_name_norm": str(has_sub_name),
                "subtitle_mobile": str(has_mobile),
                "subtitle_any": str(has_any_sub),
                "subtitle_dialogue_candidate": str(has_dialogue_sub),
                "dialogue_class": dialog_class,
                "sem_is_dialogue": str(sem_is_dialogue),
                "sem_is_sound_only": str(sem_is_sound_only),
                "sem_effective_source": sem_effective_source,
                "stream_expected": str(stream_expected),
                "expected_backend": expected_backend,
                "expected_runtime_path": expected_path,
                "static_confidence": confidence,
                "route_rows": str(route_count.get(h, 0)),
                "route_frames_top": join_sorted(frames, limit=8),
                "route_actions": join_sorted(route_actions.get(h, set()), limit=8),
                "menu_surface": str(menu_surface),
                "hash_entries": str(h_hash_count),
                "hash_formats": join_sorted(hash_formats.get(h, set()), limit=6),
                "hash_bytes_min": str(h_hash_min),
                "hash_bytes_max": str(h_hash_max),
                "hash_collision_keys": str(h_collision_keys),
                "chunk_entries": str(h_chunk_count),
                "chunk_formats": join_sorted(chunk_formats.get(h, set()), limit=6),
                "chunk_bytes_min": str(h_chunk_min),
                "chunk_bytes_max": str(h_chunk_max),
                "risk_notes": ",".join(risk_notes),
                "evidence_sources": ";".join(
                    [
                        os.path.basename(sound_names_path),
                        os.path.basename(subs_by_handle_path),
                        os.path.basename(subs_by_name_path),
                        os.path.basename(mobile_json_path),
                        os.path.basename(hash_map_path),
                        os.path.basename(chunk_map_path),
                        os.path.basename(ingress_path),
                        os.path.basename(semantics_path),
                        os.path.basename(play_routes_path),
                    ]
                ),
            }
        )

    out_tsv = os.path.join(ROOT, f"static_audio_truth_table_{date_tag}.tsv")
    with open(out_tsv, "w", encoding="utf-8-sig", newline="") as f:
        w = csv.DictWriter(f, fieldnames=list(out_rows[0].keys()), delimiter="\t")
        w.writeheader()
        w.writerows(out_rows)

    total = len(out_rows)
    sub_any = sum(1 for r in out_rows if r["subtitle_any"] == "1")
    sub_dialogue = sum(1 for r in out_rows if r["subtitle_dialogue_candidate"] == "1")
    no_fp_with_sub = sum(
        1
        for r in out_rows
        if r["subtitle_dialogue_candidate"] == "1"
        and to_int(r["hash_entries"], 0) == 0
        and to_int(r["chunk_entries"], 0) == 0
    )
    menu_surface_with_sub = sum(
        1 for r in out_rows if r["subtitle_dialogue_candidate"] == "1" and r["menu_surface"] == "1"
    )
    stream_expected_sub = sum(
        1 for r in out_rows if r["subtitle_dialogue_candidate"] == "1" and r["stream_expected"] == "1"
    )
    collisions = sum(1 for r in out_rows if to_int(r["hash_collision_keys"], 0) > 0)

    summary_lines = [
        f"out_tsv={out_tsv}",
        f"rows_total={total}",
        f"subtitle_any={sub_any}",
        f"subtitle_dialogue_candidate={sub_dialogue}",
        f"subtitle_with_no_fingerprint={no_fp_with_sub}",
        f"subtitle_on_menu_surface={menu_surface_with_sub}",
        f"subtitle_stream_expected={stream_expected_sub}",
        f"handles_with_hash_collision_keys={collisions}",
    ]
    out_summary = os.path.join(ROOT, f"static_audio_truth_summary_{date_tag}.txt")
    with open(out_summary, "w", encoding="utf-8-sig") as f:
        f.write("\n".join(summary_lines) + "\n")

    print("\n".join(summary_lines))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
