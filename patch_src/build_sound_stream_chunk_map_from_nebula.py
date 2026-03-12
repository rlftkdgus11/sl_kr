#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Build sound_stream_chunk_map.tsv from Nebula sound dumps.

Map format (compatible with proxy_dsound.cpp loader):
  hash_hex<TAB>bytes<TAB>rate<TAB>channels<TAB>bits<TAB>handle

Hash algorithm and slicing policy intentionally match runtime code:
- FNV-1a 64-bit over (fmt_tag, channels, rate, bits, bytes, pcm_pair)
- fixed 50ms slices (rate // 20 samples)
- adjacent non-overlapping slice pairs
"""

from __future__ import annotations

import argparse
import csv
import glob
import os
import re
import shutil
import struct
import subprocess
import tempfile
import wave
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Set, Tuple


FNV64_OFFSET_BASIS = 14695981039346656037
FNV64_PRIME = 1099511628211


def fnv1a64_update(h: int, data: bytes) -> int:
    for b in data:
        h ^= b
        h = (h * FNV64_PRIME) & 0xFFFFFFFFFFFFFFFF
    return h


def hash_with_format(fmt_tag: int, channels: int, rate: int, bits: int, pair_data: bytes) -> int:
    h = FNV64_OFFSET_BASIS
    h = fnv1a64_update(h, struct.pack("<H", fmt_tag))
    h = fnv1a64_update(h, struct.pack("<H", channels))
    h = fnv1a64_update(h, struct.pack("<I", rate))
    h = fnv1a64_update(h, struct.pack("<H", bits))
    h = fnv1a64_update(h, struct.pack("<I", len(pair_data)))
    h = fnv1a64_update(h, pair_data)
    return h


def parse_pcm_wav(path: Path) -> Tuple[int, int, int, int, bytes]:
    with wave.open(str(path), "rb") as wf:
        channels = wf.getnchannels()
        rate = wf.getframerate()
        bits = wf.getsampwidth() * 8
        data = wf.readframes(wf.getnframes())
    # Runtime hashes with WAVEFORMATEX from DS buffer; for decoded PCM this is tag=1.
    fmt_tag = 1
    return fmt_tag, channels, rate, bits, data


def decode_any_to_pcm_wav(path: Path) -> Tuple[int, int, int, int, bytes]:
    ffmpeg = shutil.which("ffmpeg")
    if not ffmpeg:
        raise RuntimeError("ffmpeg not found on PATH")
    with tempfile.TemporaryDirectory(prefix="sl_stream_chunk_") as td:
        out_wav = Path(td) / "decoded.wav"
        cmd = [
            ffmpeg,
            "-y",
            "-hide_banner",
            "-nostdin",
            "-loglevel",
            "error",
            "-i",
            str(path),
            "-acodec",
            "pcm_s16le",
            "-f",
            "wav",
            str(out_wav),
        ]
        r = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        if r.returncode != 0 or not out_wav.exists():
            err = r.stderr.decode("utf-8", "ignore").strip()
            raise RuntimeError(f"ffmpeg decode failed: {err[:240]}")
        return parse_pcm_wav(out_wav)


def normalize_key(s: str) -> str:
    return s.strip().lower()


def build_wav_index(sound_dir: Path) -> Dict[str, Path]:
    out: Dict[str, Path] = {}
    for p in sound_dir.glob("*.wav"):
        out.setdefault(normalize_key(p.stem), p)
    return out


def read_classification(path: Path) -> List[dict]:
    with path.open("r", encoding="utf-8-sig", errors="replace", newline="") as f:
        return list(csv.DictReader(f, delimiter="\t"))


def pick_latest_classification(root: Path) -> Path:
    cands = [Path(p) for p in glob.glob(str(root / "static_audio_ingress_classification_*.tsv"))]
    if not cands:
        raise FileNotFoundError("static_audio_ingress_classification_*.tsv not found")
    return max(cands, key=lambda p: p.stat().st_mtime)


def select_handles(rows: List[dict], only_stream_expected: bool) -> List[Tuple[int, str]]:
    out: List[Tuple[int, str]] = []
    for r in rows:
        try:
            h = int((r.get("handle") or "").strip())
        except Exception:
            continue
        name = (r.get("name") or "").strip()
        if h <= 0 or not name:
            continue
        if only_stream_expected and (r.get("stream_expected") or "") != "1":
            continue
        out.append((h, name))
    out.sort(key=lambda x: x[0])
    return out


def find_audio_file(handle: int, name: str, wav_index: Dict[str, Path], sound_dir: Path) -> Optional[Path]:
    key = normalize_key(name)
    p = wav_index.get(key)
    if p and p.exists():
        return p
    p2 = sound_dir / f"{handle}.wav"
    if p2.exists():
        return p2
    safe = name
    for ch in '<>:"/\\|?*':
        safe = safe.replace(ch, "_")
    p3 = wav_index.get(normalize_key(safe))
    if p3 and p3.exists():
        return p3
    return None


def generate_entries_for_pcm(handle: int, fmt_tag: int, channels: int, rate: int, bits: int, pcm: bytes) -> Set[Tuple[str, int, int, int, int, int]]:
    if channels <= 0 or rate <= 0 or bits <= 0:
        return set()
    block_align = (bits // 8) * channels
    if block_align <= 0:
        return set()
    slice_samples = rate // 20  # 50ms
    if slice_samples <= 0:
        return set()
    slice_bytes = slice_samples * block_align
    if slice_bytes <= 0:
        return set()
    pair_bytes = slice_bytes * 2
    if len(pcm) < pair_bytes:
        return set()

    out: Set[Tuple[str, int, int, int, int, int]] = set()
    max_off = len(pcm) - pair_bytes
    off = 0
    while off <= max_off:
        pair = pcm[off : off + pair_bytes]
        h = hash_with_format(fmt_tag, channels, rate, bits, pair)
        out.add((f"{h:016x}", pair_bytes, rate, channels, bits, handle))
        off += slice_bytes
    return out


def main() -> int:
    root = Path(__file__).resolve().parent.parent
    ap = argparse.ArgumentParser()
    ap.add_argument(
        "--classification",
        default="",
        help="Path to static_audio_ingress_classification_*.tsv (default: latest in game root)",
    )
    ap.add_argument(
        "--nebula-sounds",
        default=r"C:\Users\user\Downloads\NebulaFD-2024\Dumps\Sister Location\Sounds",
        help="Nebula sounds directory (*.wav)",
    )
    ap.add_argument(
        "--out",
        default=str(root / "sound_stream_chunk_map.tsv"),
        help="Output map path",
    )
    ap.add_argument(
        "--all-handles",
        action="store_true",
        help="Include all handles from classification (default: stream_expected only)",
    )
    args = ap.parse_args()

    cls_path = Path(args.classification) if args.classification else pick_latest_classification(root)
    sound_dir = Path(args.nebula_sounds)
    out_path = Path(args.out)
    only_stream_expected = not args.all_handles

    if not cls_path.exists():
        raise FileNotFoundError(f"classification not found: {cls_path}")
    if not sound_dir.exists():
        raise FileNotFoundError(f"nebula sounds dir not found: {sound_dir}")

    rows = read_classification(cls_path)
    targets = select_handles(rows, only_stream_expected=only_stream_expected)
    wav_index = build_wav_index(sound_dir)

    all_entries: Set[Tuple[str, int, int, int, int, int]] = set()
    missing: List[str] = []
    failed: List[str] = []
    per_handle_counts: List[Tuple[int, str, int]] = []

    for handle, name in targets:
        src = find_audio_file(handle, name, wav_index, sound_dir)
        if not src:
            missing.append(f"{handle}\t{name}\tNOT_FOUND")
            continue
        try:
            try:
                fmt_tag, ch, rate, bits, pcm = parse_pcm_wav(src)
            except Exception:
                fmt_tag, ch, rate, bits, pcm = decode_any_to_pcm_wav(src)
            entries = generate_entries_for_pcm(handle, fmt_tag, ch, rate, bits, pcm)
        except Exception as ex:
            failed.append(f"{handle}\t{name}\tPARSE_FAIL({src.name})\t{ex}")
            continue
        all_entries |= entries
        per_handle_counts.append((handle, name, len(entries)))

    sorted_entries = sorted(all_entries, key=lambda x: (x[5], x[0]))

    out_path.parent.mkdir(parents=True, exist_ok=True)
    if out_path.exists():
        bak = out_path.with_suffix(out_path.suffix + ".bak")
        try:
            bak.write_bytes(out_path.read_bytes())
        except Exception:
            pass
    with out_path.open("w", encoding="utf-8", newline="\n") as f:
        for hhex, nbytes, rate, ch, bits, handle in sorted_entries:
            f.write(f"{hhex}\t{nbytes}\t{rate}\t{ch}\t{bits}\t{handle}\n")

    miss_path = out_path.with_suffix(".missing.tsv")
    with miss_path.open("w", encoding="utf-8", newline="\n") as f:
        for row in missing:
            f.write(row + "\n")
        for row in failed:
            f.write(row + "\n")

    detail_path = out_path.with_suffix(".handle_rows.tsv")
    with detail_path.open("w", encoding="utf-8", newline="\n") as f:
        f.write("handle\tname\trows\n")
        for handle, name, cnt in sorted(per_handle_counts, key=lambda x: x[0]):
            f.write(f"{handle}\t{name}\t{cnt}\n")

    print(f"classification\t{cls_path}")
    print(f"targets\t{len(targets)}")
    print(f"rows\t{len(sorted_entries)}")
    print(f"missing\t{len(missing)}")
    print(f"failed\t{len(failed)}")
    print(f"out\t{out_path}")
    print(f"missing_file\t{miss_path}")
    print(f"detail_file\t{detail_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
