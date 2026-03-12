#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Build sound_stream_fingerprint_index.tsv for deterministic runtime stream matching.

Output row format (loaded by proxy_dsound.cpp):
  key_hash_hex<TAB>n<TAB>rate<TAB>channels<TAB>bits<TAB>handle

Rules:
- Build per-slice tokens from decoded PCM voice chunks (50ms fixed slices).
- Build n-gram hashes from token sequence.
- Keep only globally unique keys (one key -> exactly one handle) per (n, rate, ch, bits).
- Designed to match runtime code:
  - token hash: FNV1a64 over (fmt_tag, ch, rate, bits, bytes, token_pcm)
  - n-gram hash: FNV1a64 over (n_u32, token_u64, token_u64, ...)
"""

from __future__ import annotations

import argparse
import csv
import glob
import shutil
import struct
import subprocess
import tempfile
import wave
from collections import defaultdict
from pathlib import Path
from typing import DefaultDict, Dict, Iterable, List, Optional, Sequence, Set, Tuple


FNV64_OFFSET_BASIS = 14695981039346656037
FNV64_PRIME = 1099511628211


def fnv1a64_update(h: int, data: bytes) -> int:
    for b in data:
        h ^= b
        h = (h * FNV64_PRIME) & 0xFFFFFFFFFFFFFFFF
    return h


def hash_with_format(fmt_tag: int, channels: int, rate: int, bits: int, payload: bytes) -> int:
    h = FNV64_OFFSET_BASIS
    h = fnv1a64_update(h, struct.pack("<H", fmt_tag))
    h = fnv1a64_update(h, struct.pack("<H", channels))
    h = fnv1a64_update(h, struct.pack("<I", rate))
    h = fnv1a64_update(h, struct.pack("<H", bits))
    h = fnv1a64_update(h, struct.pack("<I", len(payload)))
    h = fnv1a64_update(h, payload)
    return h


def hash_token_ngram(tokens: Sequence[int], start: int, n: int) -> int:
    h = FNV64_OFFSET_BASIS
    h = fnv1a64_update(h, struct.pack("<I", n))
    for i in range(start, start + n):
        h = fnv1a64_update(h, struct.pack("<Q", tokens[i] & 0xFFFFFFFFFFFFFFFF))
    return h


def parse_pcm_wav(path: Path) -> Tuple[int, int, int, int, bytes]:
    with wave.open(str(path), "rb") as wf:
        channels = wf.getnchannels()
        rate = wf.getframerate()
        bits = wf.getsampwidth() * 8
        data = wf.readframes(wf.getnframes())
    # Runtime normalizes stream token format tag to PCM(1) for index matching.
    fmt_tag = 1
    return fmt_tag, channels, rate, bits, data


def decode_any_to_pcm_wav(path: Path) -> Tuple[int, int, int, int, bytes]:
    ffmpeg = shutil.which("ffmpeg")
    if not ffmpeg:
        raise RuntimeError("ffmpeg not found on PATH")
    with tempfile.TemporaryDirectory(prefix="sl_stream_fp_") as td:
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


def pick_latest_classification(root: Path) -> Path:
    cands = [Path(p) for p in glob.glob(str(root / "static_audio_ingress_classification_*.tsv"))]
    if not cands:
        raise FileNotFoundError("static_audio_ingress_classification_*.tsv not found")
    return max(cands, key=lambda p: p.stat().st_mtime)


def read_classification(path: Path) -> List[dict]:
    with path.open("r", encoding="utf-8-sig", errors="replace", newline="") as f:
        return list(csv.DictReader(f, delimiter="\t"))


def select_targets(rows: Iterable[dict], dialogue_only: bool, stream_only: bool) -> List[Tuple[int, str]]:
    out: List[Tuple[int, str]] = []
    for r in rows:
        try:
            h = int((r.get("handle") or "").strip())
        except Exception:
            continue
        if h <= 0:
            continue
        name = (r.get("name") or "").strip()
        if not name:
            continue
        if dialogue_only and (r.get("dialogue_class") or "").strip() != "dialogue":
            continue
        if stream_only and (r.get("stream_expected") or "").strip() != "1":
            continue
        out.append((h, name))
    out.sort(key=lambda x: x[0])
    return out


def find_audio_file(handle: int, name: str, wav_index: Dict[str, Path], sound_dir: Path) -> Optional[Path]:
    p = wav_index.get(normalize_key(name))
    if p and p.exists():
        return p
    by_handle = sound_dir / f"{handle}.wav"
    if by_handle.exists():
        return by_handle
    safe = name
    for ch in '<>:"/\\|?*':
        safe = safe.replace(ch, "_")
    p2 = wav_index.get(normalize_key(safe))
    if p2 and p2.exists():
        return p2
    return None


def build_tokens(fmt_tag: int, channels: int, rate: int, bits: int, pcm: bytes) -> Tuple[int, List[int]]:
    if channels <= 0 or rate <= 0 or bits <= 0:
        return 0, []
    block_align = (bits // 8) * channels
    if block_align <= 0:
        return 0, []
    slice_samples = rate // 20  # 50ms
    if slice_samples <= 0:
        return 0, []
    slice_bytes = slice_samples * block_align
    if slice_bytes <= 0:
        return 0, []
    out: List[int] = []
    max_off = len(pcm) - slice_bytes
    off = 0
    while off <= max_off:
        token_data = pcm[off : off + slice_bytes]
        out.append(hash_with_format(fmt_tag, channels, rate, bits, token_data))
        off += slice_bytes
    return slice_bytes, out


def build_handle_keys(tokens: Sequence[int], rate: int, channels: int, bits: int, min_n: int, max_n: int) -> Set[Tuple[int, int, int, int, int]]:
    out: Set[Tuple[int, int, int, int, int]] = set()
    if not tokens:
        return out
    n_lo = max(1, min_n)
    n_hi = max(n_lo, max_n)
    for n in range(n_lo, n_hi + 1):
        if len(tokens) < n:
            continue
        for i in range(0, len(tokens) - n + 1):
            key_hash = hash_token_ngram(tokens, i, n)
            out.add((key_hash, n, rate, channels, bits))
    return out


def pick_default_sounds_dir(root: Path) -> Optional[Path]:
    cands = [
        Path(r"C:\Users\user\Downloads\NebulaFD-2024\Dumps\Sister Location\Sounds"),
        Path(r"C:\Users\user\Downloads\NebulaFD-2024-nebula\Dumps\Sister Location\Sounds"),
        Path(r"C:\Users\user\Downloads\CTFAK2.0-master (1)\CTFAK2.0-master\Interface\CTFAK.Cli\bin\Debug\net6.0-windows\Dumps\Sister Location\Sounds"),
        root / "Sounds",
    ]
    for p in cands:
        if p.exists():
            return p
    return None


def main() -> int:
    root = Path(__file__).resolve().parent.parent
    default_sounds = pick_default_sounds_dir(root)

    ap = argparse.ArgumentParser()
    ap.add_argument(
        "--classification",
        default="",
        help="Path to static_audio_ingress_classification_*.tsv (default: latest in game root)",
    )
    ap.add_argument(
        "--nebula-sounds",
        default=str(default_sounds) if default_sounds else "",
        help="Directory containing extracted sound files (*.wav)",
    )
    ap.add_argument(
        "--out",
        default=str(root / "sound_stream_fingerprint_index.tsv"),
        help="Output index path",
    )
    ap.add_argument("--min-n", type=int, default=1, help="Minimum n-gram size")
    ap.add_argument("--max-n", type=int, default=6, help="Maximum n-gram size")
    ap.add_argument(
        "--all-handles",
        action="store_true",
        help="Include non-dialogue handles from classification (default: dialogue only)",
    )
    ap.add_argument(
        "--stream-only",
        action="store_true",
        help="Include only stream_expected=1 rows",
    )
    args = ap.parse_args()

    cls_path = Path(args.classification) if args.classification else pick_latest_classification(root)
    sound_dir = Path(args.nebula_sounds) if args.nebula_sounds else None
    out_path = Path(args.out)
    dialogue_only = not args.all_handles

    if not cls_path.exists():
        raise FileNotFoundError(f"classification not found: {cls_path}")
    if sound_dir is None or not sound_dir.exists():
        raise FileNotFoundError(
            "nebula sounds dir not found (use --nebula-sounds)."
        )

    rows = read_classification(cls_path)
    targets = select_targets(rows, dialogue_only=dialogue_only, stream_only=args.stream_only)
    wav_index = build_wav_index(sound_dir)

    key_to_handles: DefaultDict[Tuple[int, int, int, int, int], Set[int]] = defaultdict(set)
    handle_to_keyset: Dict[int, Set[Tuple[int, int, int, int, int]]] = {}
    handle_to_name: Dict[int, str] = {}
    handle_to_token_count: Dict[int, int] = {}
    handle_to_slice_bytes: Dict[int, int] = {}
    missing_rows: List[str] = []
    failed_rows: List[str] = []

    for handle, name in targets:
        handle_to_name[handle] = name
        src = find_audio_file(handle, name, wav_index, sound_dir)
        if not src:
            missing_rows.append(f"{handle}\t{name}\tNOT_FOUND")
            continue
        try:
            try:
                fmt_tag, ch, rate, bits, pcm = parse_pcm_wav(src)
            except Exception:
                fmt_tag, ch, rate, bits, pcm = decode_any_to_pcm_wav(src)
            slice_bytes, tokens = build_tokens(fmt_tag, ch, rate, bits, pcm)
            keys = build_handle_keys(tokens, rate, ch, bits, args.min_n, args.max_n)
        except Exception as ex:
            failed_rows.append(f"{handle}\t{name}\tPARSE_FAIL({src.name})\t{ex}")
            continue

        handle_to_keyset[handle] = keys
        handle_to_token_count[handle] = len(tokens)
        handle_to_slice_bytes[handle] = slice_bytes
        for key in keys:
            key_to_handles[key].add(handle)

    unique_rows: List[Tuple[int, int, int, int, int, int]] = []
    per_handle_unique_n: DefaultDict[int, DefaultDict[int, int]] = defaultdict(lambda: defaultdict(int))
    for (key_hash, n, rate, ch, bits), handles in key_to_handles.items():
        if len(handles) != 1:
            continue
        h = next(iter(handles))
        unique_rows.append((key_hash, n, rate, ch, bits, h))
        per_handle_unique_n[h][n] += 1

    unique_rows.sort(key=lambda x: (x[5], -x[1], x[2], x[3], x[4], x[0]))

    out_path.parent.mkdir(parents=True, exist_ok=True)
    if out_path.exists():
        bak = out_path.with_suffix(out_path.suffix + ".bak")
        try:
            bak.write_bytes(out_path.read_bytes())
        except Exception:
            pass
    with out_path.open("w", encoding="utf-8", newline="\n") as f:
        for key_hash, n, rate, ch, bits, handle in unique_rows:
            f.write(f"{key_hash:016x}\t{n}\t{rate}\t{ch}\t{bits}\t{handle}\n")

    report_path = out_path.with_suffix(".report.tsv")
    with report_path.open("w", encoding="utf-8", newline="\n") as f:
        f.write("handle\tname\ttokens\tslice_bytes\tkeys_total\tkeys_unique\tmax_unique_n\n")
        for handle, name in sorted(handle_to_name.items(), key=lambda x: x[0]):
            keys_total = len(handle_to_keyset.get(handle, set()))
            uniq_map = per_handle_unique_n.get(handle, {})
            keys_unique = sum(uniq_map.values()) if uniq_map else 0
            max_unique_n = max(uniq_map.keys()) if uniq_map else 0
            tok_count = handle_to_token_count.get(handle, 0)
            slice_bytes = handle_to_slice_bytes.get(handle, 0)
            f.write(
                f"{handle}\t{name}\t{tok_count}\t{slice_bytes}\t{keys_total}\t{keys_unique}\t{max_unique_n}\n"
            )

    missing_path = out_path.with_suffix(".missing.tsv")
    with missing_path.open("w", encoding="utf-8", newline="\n") as f:
        for row in missing_rows:
            f.write(row + "\n")
        for row in failed_rows:
            f.write(row + "\n")

    target_count = len(targets)
    covered = sum(1 for h, _ in targets if per_handle_unique_n.get(h))
    print(f"classification: {cls_path}")
    print(f"sounds_dir: {sound_dir}")
    print(f"targets: {target_count}")
    print(f"covered_handles: {covered}")
    print(f"unique_rows: {len(unique_rows)}")
    print(f"missing_or_failed: {len(missing_rows) + len(failed_rows)}")
    print(f"out: {out_path}")
    print(f"report: {report_path}")
    print(f"missing: {missing_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

