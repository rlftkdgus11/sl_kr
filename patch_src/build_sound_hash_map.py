import os, sys, wave, struct, re

FNV_INIT = 14695981039346656037
FNV_PRIME = 1099511628211


def fnv_update(h, data):
    if not data:
        return h
    # data is bytes
    for b in data:
        h ^= b
        h = (h * FNV_PRIME) & 0xFFFFFFFFFFFFFFFF
    return h


def hash_wav(path):
    with wave.open(path, 'rb') as wf:
        ch = wf.getnchannels()
        rate = wf.getframerate()
        sampwidth = wf.getsampwidth()
        bits = sampwidth * 8
        frames = wf.getnframes()
        bytes_len = frames * ch * sampwidth
        h = FNV_INIT
        h = fnv_update(h, struct.pack('<H', 1))  # PCM format tag
        h = fnv_update(h, struct.pack('<H', ch))
        h = fnv_update(h, struct.pack('<I', rate))
        h = fnv_update(h, struct.pack('<H', bits))
        h = fnv_update(h, struct.pack('<I', bytes_len))
        while True:
            data = wf.readframes(4096)
            if not data:
                break
            h = fnv_update(h, data)
        return h, bytes_len, rate, ch, bits


def main():
    if len(sys.argv) < 3:
        print('usage: build_sound_hash_map.py <sounds_dir> <out_tsv>')
        return 1
    sounds_dir = sys.argv[1]
    out_tsv = sys.argv[2]
    if not os.path.isdir(sounds_dir):
        print('missing dir:', sounds_dir)
        return 1
    rows = []
    for name in sorted(os.listdir(sounds_dir)):
        if not name.lower().endswith('.wav'):
            continue
        m = re.match(r'^(\d+)', name)
        if not m:
            continue
        handle = int(m.group(1))
        path = os.path.join(sounds_dir, name)
        try:
            h, bytes_len, rate, ch, bits = hash_wav(path)
        except Exception:
            continue
        rows.append((h, bytes_len, rate, ch, bits, handle, name))
    with open(out_tsv, 'w', encoding='ascii', newline='') as f:
        f.write('#hash\tbytes\trate\tch\tbits\thandle\tname\n')
        for h, bytes_len, rate, ch, bits, handle, name in rows:
            f.write('%016x\t%d\t%d\t%d\t%d\t%d\t%s\n' % (h, bytes_len, rate, ch, bits, handle, name))
    print('wrote', out_tsv, 'rows', len(rows))
    return 0

if __name__ == '__main__':
    raise SystemExit(main())
