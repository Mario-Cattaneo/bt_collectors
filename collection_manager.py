import asyncio
import json
import importlib
import sys
import traceback
import websockets
import os

SOCKET_PATH = "/tmp/collection_manager.sock"

class collection_manager:
    def __init__(self):
        self.classes = {}         # class_name -> class object
        self.class_sources = {}   # class_name -> code
        self.instances = {}       # instance_name -> instance object
        self.running = set()      # running instance names

    async def handle_message(self, websocket, message):
        try:
            print(f"[Server] Received message: {message}")
            data = json.loads(message)
            msg_type = data.get("type")

            if msg_type == "add_class":
                await self._handle_add_class(websocket, data)
            elif msg_type == "remove_class":
                await self._handle_remove_class(websocket, data)
            elif msg_type == "create_instance":
                await self._handle_create_instance(websocket, data)
            elif msg_type == "destroy_instance":
                await self._handle_destroy_instance(websocket, data)
            elif msg_type == "start_instance":
                await self._handle_start_instance(websocket, data)
            elif msg_type == "stop_instance":
                await self._handle_stop_instance(websocket, data)
            else:
                await websocket.send(json.dumps({
                    "status": "failure",
                    "error": f"Unknown message type: {msg_type}"
                }))
                print(f"[Server] Unknown message type: {msg_type}")

        except Exception as e:
            await websocket.send(json.dumps({
                "status": "failure",
                "error": f"Exception in message handler: {e}",
                "traceback": traceback.format_exc()
            }))

    async def _handle_add_class(self, websocket, data):
        class_name = data.get("class_name")
        code = data.get("code")
        dependencies = data.get("dependencies", [])

        print(f"[Server] Handling add_class for: {class_name}")
        print(f"[Server] Dependencies: {dependencies}")

        if class_name in self.classes:
            await websocket.send(json.dumps({
                "status": "failure",
                "error": f"Class {class_name} already exists"
            }))
            return

        # Load dependencies
        try:
            for dep in dependencies:
                if dep not in sys.modules:
                    print(f"[Server] Importing dependency: {dep}")
                    importlib.import_module(dep)
        except ImportError as e:
            await websocket.send(json.dumps({
                "status": "failure",
                "error": f"Missing dependency: {e}"
            }))
            return

        # Execute class code
        try:
            namespace = {}
            exec(code, namespace)
            cls = next(obj for obj in namespace.values() if isinstance(obj, type))
        except Exception as e:
            await websocket.send(json.dumps({
                "status": "failure",
                "error": f"Failed to exec class: {e}",
                "traceback": traceback.format_exc()
            }))
            return

        self.classes[class_name] = cls
        self.class_sources[class_name] = code
        print(f"[Server] Class {class_name} added successfully")

        await websocket.send(json.dumps({
            "status": "success",
            "message": f"Class {class_name} added successfully"
        }))

    async def _handle_remove_class(self, websocket, data):
        class_name = data.get("class_name")
        print(f"[Server] Handling remove_class for: {class_name}")

        if class_name not in self.classes:
            await websocket.send(json.dumps({"status": "failure", "error": f"Class {class_name} does not exist"}))
            return

        # Only remove class if no instances exist
        if any(isinstance(inst, self.classes[class_name]) for inst in self.instances.values()):
            await websocket.send(json.dumps({"status": "failure", "error": f"Cannot remove class {class_name}, instances exist"}))
            return

        del self.classes[class_name]
        del self.class_sources[class_name]
        await websocket.send(json.dumps({"status": "success", "message": f"Class {class_name} removed"}))
        print(f"[Server] Class {class_name} removed")

    async def _handle_create_instance(self, websocket, data):
        class_name = data.get("class_name")
        instance_name = data.get("instance_name")
        args = data.get("args", {})

        print(f"[Server] Handling create_instance: {instance_name} of class {class_name}")

        if class_name not in self.classes:
            await websocket.send(json.dumps({"status": "failure", "error": f"Class {class_name} does not exist"}))
            return

        if instance_name in self.instances:
            await websocket.send(json.dumps({"status": "failure", "error": f"Instance {instance_name} already exists"}))
            return

        cls = self.classes[class_name]
        try:
            instance = cls(**args)
        except TypeError as e:
            await websocket.send(json.dumps({"status": "failure", "error": f"Invalid constructor args: {e}"}))
            return

        self.instances[instance_name] = instance
        await websocket.send(json.dumps({"status": "success", "message": f"Instance {instance_name} created"}))
        print(f"[Server] Instance {instance_name} created")

    async def _handle_destroy_instance(self, websocket, data):
        instance_name = data.get("instance_name")
        print(f"[Server] Handling destroy_instance: {instance_name}")

        if instance_name not in self.instances:
            await websocket.send(json.dumps({"status": "failure", "error": f"Instance {instance_name} does not exist"}))
            return

        if instance_name in self.running:
            await websocket.send(json.dumps({"status": "failure", "error": f"Instance {instance_name} is running"}))
            return

        del self.instances[instance_name]
        await websocket.send(json.dumps({"status": "success", "message": f"Instance {instance_name} destroyed"}))
        print(f"[Server] Instance {instance_name} destroyed")

    async def _handle_start_instance(self, websocket, data):
        instance_name = data.get("instance_name")
        print(f"[Server] Handling start_instance: {instance_name}")

        if instance_name not in self.instances:
            await websocket.send(json.dumps({"status": "failure", "error": f"Instance {instance_name} does not exist"}))
            return

        if instance_name in self.running:
            await websocket.send(json.dumps({"status": "failure", "error": f"Instance {instance_name} is already running"}))
            return

        instance = self.instances[instance_name]
        if not hasattr(instance, "start") or not asyncio.iscoroutinefunction(instance.start):
            await websocket.send(json.dumps({"status": "failure", "error": f"Instance {instance_name} has no async start() method"}))
            return

        asyncio.create_task(instance.start())
        self.running.add(instance_name)
        await websocket.send(json.dumps({"status": "success", "message": f"Instance {instance_name} started"}))
        print(f"[Server] Instance {instance_name} started")

    async def _handle_stop_instance(self, websocket, data):
        instance_name = data.get("instance_name")
        print(f"[Server] Handling stop_instance: {instance_name}")

        if instance_name not in self.instances:
            await websocket.send(json.dumps({"status": "failure", "error": f"Instance {instance_name} does not exist"}))
            return

        if instance_name not in self.running:
            await websocket.send(json.dumps({"status": "failure", "error": f"Instance {instance_name} is not running"}))
            return

        instance = self.instances[instance_name]
        if not hasattr(instance, "stop") or not asyncio.iscoroutinefunction(instance.stop):
            await websocket.send(json.dumps({"status": "failure", "error": f"Instance {instance_name} has no async stop() method"}))
            return

        await instance.stop()
        self.running.remove(instance_name)
        await websocket.send(json.dumps({"status": "success", "message": f"Instance {instance_name} stopped"}))
        print(f"[Server] Instance {instance_name} stopped")

async def server_loop():
    if os.path.exists(SOCKET_PATH):
        os.remove(SOCKET_PATH)

    cm = collection_manager()

    async def handler(websocket):
        async for message in websocket:
            await cm.handle_message(websocket, message)

    print(f"[Server] collection_manager listening on {SOCKET_PATH}")
    async with websockets.unix_serve(handler, SOCKET_PATH):
        await asyncio.Future()  # run forever

def main():
    try:
        asyncio.run(server_loop())
    except KeyboardInterrupt:
        print("[Server] collection_manager shutting down")

if __name__ == "__main__":
    main()
