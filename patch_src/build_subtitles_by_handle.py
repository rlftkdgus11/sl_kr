import csv
import os
import re
import shutil

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SD_PATH = os.path.join(ROOT, "sd_kr.txt")
SOUND_NAMES = os.path.join(ROOT, "sound_names.csv")
DUR_PATH = os.path.join(ROOT, "audio_durations.csv")
OUT_PATH = os.environ.get("SUB_OUT", os.path.join(ROOT, "subtitles_by_handle.txt"))
REPORT_PATH = os.environ.get(
    "SUB_REPORT", os.path.join(ROOT, "subtitles_by_handle_mfa_report.tsv")
)

SOUND_TAGS = {"효과음", "음향효과", "음악", "배경음", "소리", "SFX", "SE"}
HANGUL_RE = re.compile(r"[가-힣]")


def load_sound_names():
    rows = {}
    if not os.path.exists(SOUND_NAMES):
        return rows
    with open(SOUND_NAMES, "r", encoding="utf-8") as f:
        reader = csv.reader(f)
        for i, row in enumerate(reader):
            if i == 0 and row and row[0] == "handle":
                continue
            if len(row) < 2:
                continue
            try:
                handle = int(row[0])
            except ValueError:
                continue
            rows[handle] = row[1]
    return rows


def load_durations():
    rows = {}
    if not os.path.exists(DUR_PATH):
        return rows
    with open(DUR_PATH, "r", encoding="utf-8") as f:
        reader = csv.reader(f)
        for i, row in enumerate(reader):
            if i == 0 and row and row[0] == "base":
                continue
            if len(row) < 4:
                continue
            base = row[0].strip().strip('"')
            secs = row[3].strip().strip('"')
            try:
                handle = int(base)
                dur = float(secs)
            except ValueError:
                continue
            if dur > 0:
                rows[handle] = dur
    return rows


def parse_blocks(lines):
    blocks = []
    cur = None
    for line in lines:
        if line.startswith("[") and "]" in line:
            inner = line[1 : line.find("]")]
            if re.match(r"\d{4}", inner):
                if cur:
                    blocks.append(cur)
                handle = inner.split("_", 1)[0]
                cur = {"handle": handle, "lines": []}
                continue
        if cur is not None:
            cur["lines"].append(line)
    if cur:
        blocks.append(cur)
    return blocks


def extract_tag(lines):
    for line in lines:
        if line.startswith("[") and "]" in line:
            return line[1 : line.find("]")].strip()
    return ""


def _prefer_parenthetical_korean(line):
    m = re.search(r"\(([^)]*[가-힣][^)]*)\)", line)
    if not m:
        return ""
    alt = m.group(1).strip()
    sm = re.search(r"'([^']+)'", alt)
    if sm:
        alt = sm.group(1).strip()
    if not alt:
        return ""
    if alt.startswith("주:") or "텍스트 파일" in alt or "번역" in alt:
        return ""
    return alt


def extract_quotes(lines):
    out = []
    for line in lines:
        # Inline text: [대사: 베이비] 숫자 1
        if line.startswith("[") and "]" in line and "\"" not in line:
            tail = line.split("]", 1)[1].strip()
            if tail:
                out.append(tail)
            continue
        if "\"" not in line:
            continue
        if "원문" in line:
            first = line.find("\"")
            second = line.find("\"", first + 1)
            if second > first:
                chunk = line[first + 1 : second].strip()
                if chunk:
                    out.append(chunk)
            continue
        parts = line.split("\"")
        quoted = []
        for i in range(1, len(parts), 2):
            chunk = parts[i].strip()
            if chunk:
                quoted.append(chunk)
        if not quoted:
            continue
        if any(HANGUL_RE.search(q) for q in quoted):
            out.extend(quoted)
            continue
        alt = _prefer_parenthetical_korean(line)
        if alt:
            out.append(alt)
            continue
        out.extend(quoted)
    return out


def normalize_speaker(tag):
    if not tag:
        return ""
    if tag.startswith("대사"):
        if ":" in tag:
            speaker = tag.split(":", 1)[1].strip()
            speaker = re.sub(r"\s*\(.*?\)\s*", "", speaker).strip()
            return speaker or "대사"
        return "대사"
    if tag == "안내음":
        return "안내음"
    return ""


def pick_color(tag, speaker, sentences):
    if tag == "안내음":
        return "blue"
    if speaker and "에너드" in speaker:
        return "red"
    for s in sentences:
        if "에너드" in s:
            return "red"
    return "white"


def main():
    if not os.path.exists(SD_PATH):
        raise SystemExit("sd_kr.txt not found")
    with open(SD_PATH, "r", encoding="utf-8") as f:
        lines = [l.rstrip("\n") for l in f]

    blocks = parse_blocks(lines)
    sound_names = load_sound_names()
    durations = load_durations()

    mapped = []
    skipped = []
    report_rows = []
    for b in blocks:
        tag = extract_tag(b["lines"])
        handle = b["handle"]
        if not tag:
            skipped.append((handle, "no_tag"))
            continue
        if tag in SOUND_TAGS:
            skipped.append((handle, tag))
            continue
        quotes = extract_quotes(b["lines"])
        if not quotes:
            skipped.append((handle, "no_text"))
            continue
        speaker = normalize_speaker(tag)
        sentences = quotes[:]
        if speaker:
            sentences[0] = f"{speaker}: {sentences[0]}"
        color = pick_color(tag, speaker, sentences)
        dur = 0.0
        if handle.isdigit():
            dur = durations.get(int(handle), 0.0)
        mapped.append((handle, color, f"{dur:.3f}" if dur > 0 else "0", "|".join(sentences), tag, speaker, len(quotes)))
        sname = sound_names.get(int(handle)) if handle.isdigit() else ""
        report_rows.append((handle, sname or "", tag, speaker, len(quotes)))

    if os.path.exists(OUT_PATH):
        try:
            shutil.copy2(OUT_PATH, OUT_PATH + ".bak")
        except OSError:
            pass

    with open(OUT_PATH, "w", encoding="utf-8", newline="") as f:
        for handle, color, dur, text, _tag, _speaker, _qcount in mapped:
            f.write(f"{handle}\t{color}\t{dur}\t{text}\n")

    with open(REPORT_PATH, "w", encoding="utf-8", newline="") as f:
        f.write("handle\tsound_name\ttag\tspeaker\tquote_count\n")
        for row in report_rows:
            f.write("\t".join(map(str, row)) + "\n")

    print(f"mapped: {len(mapped)}")
    print(f"skipped: {len(skipped)}")
    print(f"report: {REPORT_PATH}")


if __name__ == "__main__":
    main()
