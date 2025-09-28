import argparse
import asyncio
import json
import os
import re
import websockets

DEFAULT_JSONS = {
    "market": "./market_collector.json",
    "rpc": "./rpc_collector.json"
}

SOCKET_PATH = "/tmp/collection_manager.sock"  # raw path, no unix://


def parse_imports_from_file(file_path):
    """Parse top-level imports from a Python file."""
    deps = set()
    import_pattern = re.compile(r'^\s*(?:from\s+([\w\.]+)\s+import|import\s+([\w\.]+))')
    with open(file_path, "r") as f:
        for line in f:
            match = import_pattern.match(line)
            if match:
                dep = match.group(1) or match.group(2)
                if dep:
                    deps.add(dep.split('.')[0])  # keep top-level only
    return list(deps)


async def send_message(message):
    """Send a JSON message to the collection manager over a Unix WebSocket."""
    async with websockets.unix_connect(SOCKET_PATH) as ws:
        await ws.send(json.dumps(message))
        response = await ws.recv()
        print("Server response:", response)
        return json.loads(response)


async def init_action(kind, json_path):
    """Initialize a collector (market or rpc)."""
    with open(json_path) as f:
        spec = json.load(f)

    # Validate JSON
    file_path = spec.get("file")
    if not file_path:
        print("Error: JSON must contain a 'file' key")
        return
    instance_name = spec.get("instance_name")
    if not instance_name:
        print("Error: JSON must contain an 'instance_name' key")
        return
    args = spec.get("args", {})

    # Read code and parse imports
    with open(file_path) as f:
        code = f.read()
    dependencies = parse_imports_from_file(file_path) + spec.get("dependencies", [])

    class_name = spec.get("class_name") or os.path.splitext(os.path.basename(file_path))[0]

    # 1️⃣ Add class
    add_resp = await send_message({
        "type": "add_class",
        "class_name": class_name,
        "code": code,
        "dependencies": dependencies
    })
    if add_resp.get("status") != "success":
        print("Init failed at add_class")
        return

    # 2️⃣ Create instance
    create_resp = await send_message({
        "type": "create_instance",
        "class_name": class_name,
        "instance_name": instance_name,
        "args": args
    })
    if create_resp.get("status") != "success":
        print("Init failed at create_instance")
        return

    # 3️⃣ Start instance
    start_resp = await send_message({
        "type": "start_instance",
        "instance_name": instance_name
    })
    if start_resp.get("status") != "success":
        print("Init failed at start_instance")
        return

    print("Init succeeded")


def main():
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(dest="command")

    p_init = subparsers.add_parser("init")
    p_init.add_argument("kind", choices=["market", "rpc"])
    p_init.add_argument("json_path", nargs="?", help="Path to JSON")

    args = parser.parse_args()

    if args.command == "init":
        json_path = args.json_path or DEFAULT_JSONS[args.kind]
        asyncio.run(init_action(args.kind, json_path))


if __name__ == "__main__":
    main()
