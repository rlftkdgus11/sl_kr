# sl_kr

FNAF Sister Location Korean subtitle patch project.

## Start Here
- [docs/HANDOFF.md](docs/HANDOFF.md): current architecture, stable baseline, blocker, and next steps

## What Is In This Repo
- `patch_src/`: `dsound.dll` proxy source, build script, and helper scripts
- `sound_hash_map.tsv`: stable clip PCM hash -> handle map
- `sound_names.csv`: handle -> sound name map
- `subtitles_by_handle.txt`: canonical handle -> subtitle entries
- `subtitles_by_name.txt`: name -> subtitle entries
- `subtitles_ko*.json`, `subtitle_timeline.json`: subtitle source data
- `mobile_subs/`: mobile subtitle timeline fragments
- `hash_only.txt`: baseline runtime config marker

## Current Stable State
- Normal, non-stream dialogue is matched reliably by PCM hash.
- Streamed dialogue is not fully solved yet; broad 1:1 matching is still blocked by runtime identity capture.

## Build
- Requirement: Visual Studio 2022 Build Tools with x86 toolchain
- Command: `patch_src\\build_dsound.cmd`

## Scope
- Large game binaries and runtime logs are intentionally excluded.
- This repository keeps the patch source, mapping data, and concise handoff information.
