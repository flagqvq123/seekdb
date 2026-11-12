import argparse
import json
import subprocess
from pathlib import Path

parser = argparse.ArgumentParser(description="Check SQL embedding batches against the local enhancement mock log")
parser.add_argument('--client', required=True, type=Path)
parser.add_argument('--port', required=True, type=int)
parser.add_argument('--log', required=True, type=Path)
options = parser.parse_args()
client = options.client
log = options.log

def sql(query):
    return subprocess.check_output([str(client), '-h127.0.0.1', '-P%d' % options.port, '-uroot',
                                    '-Dtest', '-N', '-B', '-e', query], text=True)

def requests():
    return [json.loads(line) for line in log.read_text().splitlines()]

sql("CALL DBMS_AI_SERVICE.CREATE_AI_MODEL('audit_embed', "
    "'{\"type\":\"dense_embedding\",\"model_name\":\"audit-embedding\"}');"
    "CALL DBMS_AI_SERVICE.CREATE_AI_MODEL_ENDPOINT('audit_embed_ep', "
    "'{\"ai_model_name\":\"audit_embed\",\"provider\":\"openai\",\"access_key\":\"test\","
    "\"url\":\"http://127.0.0.1:8000/v1/embeddings/shuffle\"}');"
    "CREATE TABLE audit_embedding(id INT PRIMARY KEY, content TEXT, dim INT);")
try:
    values = ','.join("(%d,%s,%d)" % (i, 'NULL' if i % 11 == 0 else "'row-%d'" % i,
                                    2 if i % 2 == 0 else 3) for i in range(1, 46))
    sql('INSERT INTO audit_embedding VALUES ' + values)
    start = len(requests())
    rows = sql("SELECT id, AI_EMBED('audit_embed', content, dim) FROM audit_embedding ORDER BY id")
    parsed = [row.split('\t', 1) for row in rows.splitlines()]
    assert len(parsed) == 45
    for key, value in parsed:
        i = int(key)
        if i % 11 == 0:
            assert value == 'NULL', (i, value)
        else:
            vector = json.loads(value)
            assert len(vector) == (2 if i % 2 == 0 else 3), (i, vector)
            assert vector[0] == len(('row-%d' % i).encode()) / 10, (i, vector)
    sent = requests()[start:]
    sizes = [len(event['body']['input']) for event in sent]
    assert sum(sizes) == 41, sizes
    assert any(size > 1 for size in sizes), sizes
    assert max(sizes) <= 20, sizes
    print('45 SQL rows, 41 remote inputs, batch sizes:', sizes)
    start = len(requests())
    rows = sql("SELECT id, AI_EMBED('audit_embed', content, dim) FROM audit_embedding "
               "WHERE id % 2 = 1 ORDER BY id LIMIT 3")
    assert [int(row.split('\t')[0]) for row in rows.splitlines()] == [1, 3, 5], rows
    texts = [text for event in requests()[start:] for text in event['body']['input']]
    assert sorted(texts) == ['row-1', 'row-3', 'row-5'], texts
    print('WHERE/LIMIT sent exactly three selected inputs:', texts)
finally:
    sql("DROP TABLE audit_embedding;"
        "CALL DBMS_AI_SERVICE.DROP_AI_MODEL_ENDPOINT('audit_embed_ep');"
        "CALL DBMS_AI_SERVICE.DROP_AI_MODEL('audit_embed');")
