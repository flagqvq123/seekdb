"""Build a small Parquet fixture without a query engine or third-party packages."""
import base64
import struct
from pathlib import Path


def varint(value):
    result = bytearray()
    while value >= 128:
        result.append((value & 127) | 128)
        value >>= 7
    result.append(value)
    return bytes(result)


def integer(value):
    return varint((value << 1) ^ (value >> 63))


def value(kind, item):
    if kind in (4, 5, 6):
        return integer(item)
    if kind == 8:
        data = item.encode() if isinstance(item, str) else item
        return varint(len(data)) + data
    if kind == 12:
        return compact(item)
    if kind == 9:
        element, values = item
        length = len(values)
        header = bytes([(min(length, 15) << 4) | element])
        return header + (varint(length) if length >= 15 else b'') + b''.join(value(element, x) for x in values)
    raise ValueError(kind)


def compact(fields):
    result = bytearray()
    previous = 0
    for field, kind, item in fields:
        delta = field - previous
        result.append((delta << 4) | kind if 0 < delta < 16 else kind)
        if not 0 < delta < 16:
            result.extend(integer(field))
        if kind not in (1, 2):
            result.extend(value(kind, item))
        previous = field
    result.append(0)
    return bytes(result)


def snappy_literal(data):
    length = len(data) - 1
    if length < 60:
        header = bytes([length << 2])
    else:
        size = max(1, (length.bit_length() + 7) // 8)
        header = bytes([(59 + size) << 2]) + length.to_bytes(size, 'little')
    return varint(len(data)) + header + data


def levels(items):
    return bytes([3, sum((item is not None) << i for i, item in enumerate(items))])


def plain(physical, items):
    present = [item for item in items if item is not None]
    if physical == 0:
        return bytes([sum(bool(item) << i for i, item in enumerate(present))])
    if physical == 1:
        return b''.join(struct.pack('<i', item) for item in present)
    if physical == 2:
        return b''.join(struct.pack('<q', item) for item in present)
    if physical == 5:
        return b''.join(struct.pack('<d', item) for item in present)
    return b''.join(struct.pack('<I', len(item.encode())) + item.encode() for item in present)


def page(physical, items, optional, version2, dictionary):
    prefix = b''
    encoding = 0
    data = plain(physical, items)
    if dictionary:
        unique = list(dict.fromkeys(item for item in items if item is not None))
        raw = plain(physical, unique)
        compressed = snappy_literal(raw)
        header = compact([(1, 5, 2), (2, 5, len(raw)), (3, 5, len(compressed)),
                          (7, 12, [(1, 5, len(unique)), (2, 5, 0)])])
        prefix = header + compressed
        ids = [unique.index(item) for item in items if item is not None]
        data = bytes([1, 3, sum(index << i for i, index in enumerate(ids))])
        encoding = 8
    definition = levels(items) if optional else b''
    if version2:
        compressed = definition + snappy_literal(data)
        details = [(1, 5, len(items)), (2, 5, items.count(None)), (3, 5, len(items)),
                   (4, 5, encoding), (5, 5, len(definition)), (6, 5, 0), (7, 1, True)]
        header = compact([(1, 5, 3), (2, 5, len(definition) + len(data)),
                          (3, 5, len(compressed)), (8, 12, details)])
    else:
        raw = (struct.pack('<I', len(definition)) + definition if optional else b'') + data
        compressed = snappy_literal(raw)
        details = [(1, 5, len(items)), (2, 5, encoding), (3, 5, 3), (4, 5, 3)]
        header = compact([(1, 5, 0), (2, 5, len(raw)), (3, 5, len(compressed)), (5, 12, details)])
    return prefix + header + compressed, len(prefix)


def build():
    columns = [('id', 2, True, None), ('name', 6, True, 0), ('active', 0, False, None),
               ('amount', 5, False, None), ('day', 1, True, 6), ('created', 2, True, 10)]
    groups = [([1, None, 3], ['甲', '乙', '甲'], [True, False, True], [1.25, 2.5, 3.75],
               [0, None, 19782], [0, None, 1709208000123456]),
              ([4, 5, 6], ['乙', None, '甲'], [False, True, False], [4.5, 5.25, 6.0],
               [19783, 19784, None], [-1, 1709251200000000, None])]
    output = bytearray(b'PAR1')
    rowgroups = []
    for group_index, group in enumerate(groups):
        chunks = []
        group_bytes = 0
        for (name, physical, optional, _), items in zip(columns, group):
            encoded, dictionary_size = page(physical, list(items), optional, bool(group_index), name == 'name')
            offset = len(output)
            output.extend(encoded)
            uncompressed = len(encoded) - 2 * (1 + bool(dictionary_size))
            group_bytes += uncompressed
            meta = [(1, 5, physical), (2, 9, (5, [0, 3, 8])), (3, 9, (8, [name])),
                    (4, 5, 1), (5, 6, 3), (6, 6, uncompressed), (7, 6, len(encoded)),
                    (9, 6, offset + dictionary_size)]
            if dictionary_size:
                meta.append((11, 6, offset))
            chunks.append([(2, 6, offset), (3, 12, meta)])
        rowgroups.append([(1, 9, (12, chunks)), (2, 6, group_bytes), (3, 6, 3)])
    schema = [[(4, 8, 'schema'), (5, 5, len(columns))]]
    for name, physical, optional, converted in columns:
        fields = [(1, 5, physical), (3, 5, int(optional)), (4, 8, name)]
        if converted is not None:
            fields.append((6, 5, converted))
        schema.append(fields)
    footer = compact([(1, 5, 2), (2, 9, (12, schema)), (3, 6, 6),
                      (4, 9, (12, rowgroups)), (6, 8, 'seekdb file SQL regression fixture')])
    output.extend(footer)
    output.extend(struct.pack('<I', len(footer)))
    output.extend(b'PAR1')
    assert output[:4] == output[-4:] == b'PAR1'
    assert len(output) < 4096
    return bytes(output)


if __name__ == '__main__':
    destination = Path(__file__).with_name('file_sql_multigroup.parquet.b64')
    destination.write_text(base64.b64encode(build()).decode() + '\n', encoding='ascii')
