from http.server import BaseHTTPRequestHandler, HTTPServer

class MyHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path == "/" or self.path == "/index.html":
            self.send_response(200)
            self.send_header("Content-Type", "text/html")
            self.end_headers()
            with open("webpage/index.html", "rb") as f:
                self.wfile.write(f.read())

        elif self.path == "/styles.css":
            self.send_response(200)
            self.send_header("Content-Type", "text/css")
            self.end_headers()
            with open("webpage/styles.css", "rb") as f:
                self.wfile.write(f.read())

        elif self.path == "/main.js":
            self.send_response(200)
            self.send_header("Content-Type", "application/javascript")
            self.end_headers()
            with open("webpage/main.js", "rb") as f:
                self.wfile.write(f.read())

        elif self.path.startswith("/data"):
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(b'{"id": "abcd", "values": [1,2,3]}')

        else:
            self.send_response(404)
            self.end_headers()
            self.wfile.write(b"Not Found")

server = HTTPServer(("0.0.0.0", 8080), MyHandler)
print("Serving at http://localhost:8080")
server.serve_forever()
