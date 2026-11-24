import json
from http.server import BaseHTTPRequestHandler, HTTPServer

class CentralPrueba(BaseHTTPRequestHandler):
    def do_PUT(self):
        datos = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
        print("PUT   ", self.path, datos)
        self.responder()

    def do_DELETE(self):
        print("DELETE", self.path)
        self.responder()

    def responder(self):
        cuerpo = b'{"ok": true}'
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(cuerpo)))
        self.end_headers()
        self.wfile.write(cuerpo)

    def log_message(self, *args):
        pass

print("Central de prueba en http://127.0.0.1:8080")
HTTPServer(("0.0.0.0", 8080), CentralPrueba).serve_forever()