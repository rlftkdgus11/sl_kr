# Project Handoff

## Goal
Patch `SisterLocation.exe` so in-game audio playback triggers Korean subtitles with deterministic matching.

## How The Patch Works
1. A proxy `dsound.dll` is placed in the game folder.
2. The proxy hooks DirectSound buffer activity and selected `MMFS2.dll` ordinal calls.
3. For stable clips, PCM hashes are matched against `sound_hash_map.tsv`.
4. The matched handle is resolved to subtitle text through `subtitles_by_handle.txt` and related tables.

## Current Stable Baseline
- The practical baseline is the older hash-based behavior restored on 2026-03-12.
- This baseline is good for normal, non-stream dialogue.
- The repo should be treated as `normal dialogue solved, streamed dialogue unresolved`.

## What Is Closed
- Static NebulaFD dump analysis is substantially closed.
- `sample_handle` in Nebula/MFA action data is raw 0-based.
- Canonical runtime/subtitle handle is `sample_handle + 1`.
- Example: `Baby01` raw `96` -> canonical handle `97`.
- The sound bank contains both RIFF and MP3-like assets.
- Normal dialogue hash matching is proven and usable.

## Current Blocker
Streamed dialogue is still blocked by runtime identity closure.

What this means:
- The engine does not expose a stable, already-finalized streamed dialogue ID through the currently proven hook points.
- Buffer reuse, chunked writes, and late selection make stream matching unstable.
- Observed runtime evidence did not close a deterministic final-ID path for all streamed dialogue.

Practical consequence:
- Current experiments can sometimes recognize a specific streamed line, but not all streamed dialogue with reliable 1:1 guarantees.
- Do not describe stream matching as solved unless the final runtime-selected ID is proven and captured deterministically.

## Key Files
- `patch_src/proxy_dsound.cpp`: main proxy/hook implementation
- `patch_src/build_dsound.cmd`: build and deploy script
- `sound_hash_map.tsv`: stable clip hash index
- `sound_names.csv`: handle/name normalization basis
- `subtitles_by_handle.txt`: canonical subtitle table
- `subtitles_by_name.txt`: fallback name subtitle table
- `audio_durations.csv`: known clip durations
- `subtitle_timeline.json`: subtitle source timeline data
- `정리.txt`: older project notes snapshot

## Build And Test
- Build with `patch_src\build_dsound.cmd`
- Output is deployed as `dsound.dll`
- Stable verification target:
  - Normal dialogue should match correctly without menu false positives.
- Unstable verification target:
  - Streamed dialogue should be treated as experimental until final-ID capture is closed.

## Recommended Next Steps
1. Preserve the normal-dialogue hash baseline; do not destabilize it while testing stream ideas.
2. Treat streamed dialogue as a separate layer, not a small extension of the normal hash path.
3. Focus on capturing the engine's final runtime-selected stream identity before expanding subtitle rules.
4. Keep new runtime logging concise and evidence-driven; prefer proofs over heuristics.

## One-Line Status
Normal dialogue is solved by hash matching; streamed dialogue remains blocked because the engine's final runtime-selected dialogue identity cannot yet be captured deterministically.
