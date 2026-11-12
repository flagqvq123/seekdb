#!/usr/bin/env python3
"""Check file SQL path boundaries and cached-plan feature gating on a test instance."""
import argparse
import subprocess
import tempfile
from pathlib import Path

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument('--client', required=True)
parser.add_argument('--port', required=True, type=int)
parser.add_argument('--allowed-directory', required=True, type=Path)
parser.add_argument('--outside-directory', required=True, type=Path)
options = parser.parse_args()
allowed = options.allowed_directory.resolve(strict=True)
outside = options.outside_directory.resolve(strict=True)
if allowed == outside or allowed in outside.parents:
    parser.error('outside-directory must be outside the configured secure_file_priv directory')

def sql(text):
    return subprocess.run([options.client, '-h127.0.0.1', '-P%d' % options.port,
                           '-uroot', '-Dtest', '-N', '-B', '-e', text],
                          text=True, capture_output=True)

def literal(path):
    return "'" + str(path).replace('\\', '\\\\').replace("'", "''") + "'"

with tempfile.TemporaryDirectory(prefix='file-sql-allowed-', dir=allowed) as a, \
     tempfile.TemporaryDirectory(prefix='file-sql-outside-', dir=outside) as b:
    a, b = Path(a), Path(b)
    good, secret = a / 'good.csv', b / 'outside.csv'
    good.write_text('id\n1\n2\n', encoding='utf-8')
    secret.write_text('id\n9\n', encoding='utf-8')
    (a / 'escape.csv').symlink_to(secret)
    result = sql('SET enable_file_sql=ON; SELECT COUNT(*) FROM read_csv(%s);' % literal(good))
    assert result.returncode == 0 and result.stdout.strip() == '2', result.stderr
    result = sql('SET enable_file_sql=ON; SELECT COUNT(*) FROM read_csv(%s); '
                 'SET enable_file_sql=OFF; SELECT COUNT(*) FROM read_csv(%s);' %
                 (literal(good), literal(good)))
    assert result.returncode != 0 and '1235' in result.stderr, (result.stdout, result.stderr)
    for query in [
        'SELECT COUNT(*) FROM read_csv(%s)' % literal(secret),
        'SELECT COUNT(*) FROM read_csv(%s)' % literal(a / 'escape.csv'),
        'SELECT COUNT(*) FROM glob(%s)' % literal(a / '*.csv'),
        'COPY (SELECT 1 AS id) TO %s (FORMAT csv, HEADER)' % literal(b / 'blocked.csv'),
    ]:
        result = sql('SET enable_file_sql=ON; ' + query)
        assert result.returncode != 0, (query, result.stdout)
    assert not (b / 'blocked.csv').exists()
print('Verified disabled feature, outside paths, symlink/glob escape and outside COPY rejection')
