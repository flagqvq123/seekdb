#!/usr/bin/env python3
"""Local protocol mock; logs actual requests for SQL batch regression checks."""

import argparse
import json
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer


def serve(port, dimension, log_path):
    lock = threading.Lock()

    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *args):
            pass

        def send(self, status, value):
            data = value if isinstance(value, bytes) else json.dumps(value, ensure_ascii=False).encode()
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)

        def do_POST(self):
            raw = self.rfile.read(int(self.headers.get("Content-Length", "0")))
            try:
                body = json.loads(raw)
                with lock, open(log_path, "a", encoding="utf-8") as log:
                    log.write(json.dumps({"path": self.path, "body": body}, ensure_ascii=False) + "\n")
                path = self.path.removeprefix("/compatible-mode")
                if path.endswith("/http500"):
                    return self.send(500, {"error": "injected failure"})
                if path.endswith("/malformed"):
                    return self.send(200, b'{"data":[')
                if path.endswith("/root-array"):
                    return self.send(200, [])
                if path.startswith("/v1/chat/completions"):
                    text = body["messages"][-1]["content"]
                    return self.send(200, {"choices": [{"message": {"content": "" if path.endswith("/empty") else "echo:" + text}}]})
                if path.startswith("/api/generate"):
                    if body.get("stream") is not False:
                        return self.send(400, {"error": "non-streaming request required"})
                    return self.send(200, {"done": True, "response": "echo:" + body["prompt"]})
                if path.startswith(("/v1/embeddings", "/api/embed")):
                    texts = body["input"]
                    if not isinstance(texts, list):
                        texts = [texts]
                    dim = body.get("dimensions", dimension)
                    vectors = [[(len(text.encode()) + i) / 10 for i in range(dim)] for text in texts]
                    if path.endswith("/bad-vector"):
                        vectors[0][0] = "invalid"
                    if path.endswith("/bad-dimension"):
                        vectors[0].append(0.0)
                    if path.startswith("/api/embed"):
                        return self.send(200, {"embeddings": vectors})
                    data = [{"index": i, "embedding": vector} for i, vector in enumerate(vectors)]
                    if path.endswith("/bad-index"):
                        data[0]["index"] = len(data)
                    return self.send(200, {"data": data[::-1] if path.endswith("/shuffle") else data})
                if path.startswith("/v1/rerank"):
                    query = body["query"].casefold()
                    results = [{"index": i, "relevance_score": float(len(text) + (text.casefold().count(query) if query else 0) * 1000000)}
                               for i, text in enumerate(body["documents"])]
                    if path.endswith("/bad-index"):
                        results[0]["index"] = len(results)
                    return self.send(200, {"results": results[::-1] if path.endswith("/shuffle") else results})
                self.send(404, {"error": "unknown endpoint"})
            except (ValueError, KeyError, TypeError, IndexError) as error:
                self.send(400, {"error": str(error)})

    ThreadingHTTPServer(("127.0.0.1", port), Handler).serve_forever()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--port", type=int, default=8000)
    parser.add_argument("--dimension", type=int, default=768)
    parser.add_argument("--log", default="/tmp/seekdb-ai-enhancement-requests.jsonl")
    options = parser.parse_args()
    if not 0 < options.port < 65536 or options.dimension <= 0:
        parser.error("port and dimension must be positive and port must be below 65536")
    serve(options.port, options.dimension, options.log)
