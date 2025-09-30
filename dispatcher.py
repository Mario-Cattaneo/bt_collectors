#!/usr/bin/env python3
import sys
import os
import json
import socket

SOCKET_PATH = "/tmp/collection_manager.sock"

def send_method(method):
    try:
        client = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        client.settimeout(2)  # prevent hanging forever
        client.connect(SOCKET_PATH)

        client.sendall(json.dumps(method).encode("utf-8"))
        response = client.recv(65536).decode("utf-8")
        client.close()

        print(f"Server response: {response}")
        return response
    except Exception as e:
        print(f"Error sending method {method.get('type')}: {e}")
        return None

def main():
    if len(sys.argv) < 2:
        print(f"Usage: {sys.argv[0]} <json_name | ls>")
        sys.exit(1)

    cmd = sys.argv[1]

    if cmd == "ls":
        send_method({"type": "ls"})
        return

    json_file = cmd if cmd.endswith(".json") else f"{cmd}.json"
    if not os.path.exists(json_file):
        print(f"Error: JSON file {json_file} not found")
        sys.exit(1)

    with open(json_file, "r") as f:
        config = json.load(f)

    methods = config.get("methods", [])
    for method in methods:
        send_method(method)

if __name__ == "__main__":
    main()
