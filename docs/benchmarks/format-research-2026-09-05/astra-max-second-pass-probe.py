"""Source-derived byte accounting only: no database, build, or timing load.

Baseline append_route is the grammar at seglog/sealed_index.go:536-580.
Run-mode encoding is a proposal, not implemented VibeDB behavior.
"""
import json
from pathlib import Path


def uv(x):
    out = bytearray()
    while x >= 128:
        out.append((x & 127) | 128)
        x >>= 7
    out.append(x)
    return bytes(out)


def append_route(entries):
    out = bytearray()
    previous_term = previous_extent = 0
    for i, e in enumerate(entries):
        new_extent = i == 0 or e['off'] != previous_extent
        out.append(e['type'] | (128 if new_extent else 0))
        delta = e['term'] - previous_term
        out.extend(uv(2 * abs(delta) + (delta < 0)))
        if new_extent:
            out.extend(uv(e['off'] - previous_extent))
            out.extend(uv(e['length']))
            out.extend(uv(e['id']))
            out.extend(e['wave'])
            previous_extent = e['off']
        out.extend(uv(e['data_off']))
        out.extend(uv(e['data_len']))
        previous_term = e['term']
    return bytes(out)


def run_route(entries):
    runs = []
    for e in entries:
        if (e['term'], e['type']) != (entries[0]['term'], entries[0]['type']):
            return None, None
        if runs and e['off'] == runs[-1][0]['off']:
            base, count = runs[-1]
            if any(e[k] != base[k] for k in ('length', 'id', 'wave', 'data_len')):
                return None, None
            if e['data_off'] != base['data_off'] + count * base['data_len']:
                return None, None
            runs[-1] = (base, count + 1)
        else:
            runs.append((e, 1))
    if len(runs) > 16:
        return None, len(runs)
    out = bytearray(uv(entries[0]['term']))
    out.append(entries[0]['type'])
    out.extend(uv(len(runs)))
    previous_extent = 0
    for base, count in runs:
        out.extend(uv(count))
        out.extend(uv(base['off'] - previous_extent))
        out.extend(uv(base['length']))
        out.extend(uv(base['id']))
        out.extend(base['wave'])
        out.extend(uv(base['data_off']))
        out.extend(uv(base['data_len']))
        previous_extent = base['off']
    return bytes(out), len(runs)


def fixed_entries(count, data_len):
    entries = []
    offset, extent_id = 4096, 1
    packed = max(1, 32768 // data_len)
    while len(entries) < count:
        n = min(packed, count - len(entries))
        extent_bytes = n * data_len + 16
        for i in range(n):
            entries.append(dict(term=7, type=0, off=offset, length=extent_bytes,
                                id=extent_id, wave=bytes(range(16)),
                                data_off=i*data_len, data_len=data_len))
        offset += extent_bytes
        extent_id += 1
    return entries


def overflow_extents(total):
    result = []
    while total:
        body = min(total, 65536 - 132)
        extent = ((body + 132 + 4095) // 4096) * 4096
        result.append(dict(body=body, extent=extent))
        total -= body
    return result


routes = []
for count, data_len in [(64, 512), (256, 300), (256, 512), (256, 1024),
                        (256, 65536)]:
    entries = fixed_entries(count, data_len)
    old = append_route(entries)
    new, run_count = run_route(entries)
    routes.append(dict(entries=count, command_bytes=data_len, extents=run_count,
                       old_payload=len(old), proposed_payload=None if new is None else len(new),
                       payload_saved=None if new is None else len(old)-len(new),
                       old_metadata_read=40+len(old),
                       proposed_metadata_read=None if new is None else 40+len(new),
                       old_varints=3*count+3*run_count,
                       proposed_varints=None if new is None else 2+6*run_count))

overflow = []
for total in [65536, 131072, 1048576, 4194304]:
    extents = overflow_extents(total)
    physical = sum(e['extent'] for e in extents)
    saved = physical - extents[0]['extent']
    overflow.append(dict(document_bytes=total, pieces=len(extents),
                         last_body=extents[-1]['body'], last_extent=extents[-1]['extent'],
                         full_chain=physical, first_chunk_only_new_bytes=extents[0]['extent'],
                         first_chunk_only_saved=saved, rf3_saved_per_checkpointed_version=3*saved,
                         last_chunk_changed_saved=0))

result = dict(note='Offline source-derived grammar model; no database/timing measurement.',
              routes=routes, overflow=overflow)
target = Path('/private/tmp/vibedb-astra-max-second-pass-probe.json')
target.write_text(json.dumps(result, indent=2) + '\n')
print(json.dumps(result, indent=2))
