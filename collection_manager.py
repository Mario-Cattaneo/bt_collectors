import asyncio
import json
import importlib
import sys
import traceback
import os

SOCKET_PATH = "/tmp/collection_manager.sock"

class collection_manager:
    def __init__(self):
        self.classes = {}
        self.class_sources = {}
        self.instances = {}
        self.running = set()

    async def handle_message(self, writer, message):
        try:
            print(f"[Server] Received message: {message}")
            data = json.loads(message)
            msg_type = data.get("type")

            if msg_type == "add_class":
                await self._handle_add_class(writer, data)
            elif msg_type == "remove_class":
                await self._handle_remove_class(writer, data)
            elif msg_type == "create_instance":
                await self._handle_create_instance(writer, data)
            elif msg_type == "destroy_instance":
                await self._handle_destroy_instance(writer, data)
            elif msg_type == "invoke_function":
                await self._handle_invoke_function(writer, data)
            elif msg_type == "ls":
                await self._handle_ls(writer)
            else:
                await self._send(writer, {
                    "status": "failure",
                    "error": f"Unknown message type: {msg_type}"
                })
                print(f"[Server] Unknown message type: {msg_type}")

        except Exception as e:
            await self._send(writer, {
                "status": "failure",
                "error": f"Exception in message handler: {e}",
                "traceback": traceback.format_exc()
            })

    async def _send(self, writer, obj):
        msg = json.dumps(obj).encode("utf-8")
        writer.write(msg)
        await writer.drain()

    async def _handle_add_class(self, writer, data):
        class_name = data.get("class_name")
        code = data.get("code")
        file = data.get("file")
        dependencies = data.get("dependencies", [])

        if class_name in self.classes:
            await self._send(writer, {"status": "failure", "error": f"Class {class_name} already exists"})
            return

        # Load dependencies
        try:
            for dep in dependencies:
                if dep not in sys.modules:
                    importlib.import_module(dep)
        except ImportError as e:
            await self._send(writer, {"status": "failure", "error": f"Missing dependency: {e}"})
            return

        # Load class code (either inline or from file)
        try:
            if file:
                with open(file, "r") as f:
                    code = f.read()
            if not code:
                await self._send(writer, {"status": "failure", "error": "No code or file provided"})
                return

            namespace = {}
            exec(code, namespace)
            cls = namespace.get(class_name)
            if cls is None or not isinstance(cls, type):
                await self._send(writer, {"status": "failure", "error": f"Class {class_name} not found"})
                return
        except Exception as e:
            await self._send(writer, {
                "status": "failure",
                "error": f"Failed to exec class: {e}",
                "traceback": traceback.format_exc()
            })
            return

        self.classes[class_name] = cls
        self.class_sources[class_name] = code
        await self._send(writer, {"status": "success", "message": f"Class {class_name} added successfully"})

    
    async def _handle_remove_class(self, writer, data):
        class_name = data.get("class_name")
        if class_name not in self.classes:
            await self._send(writer, {"status": "failure", "error": f"Class {class_name} does not exist"})
            return
        if any(isinstance(inst, self.classes[class_name]) for inst in self.instances.values()):
            await self._send(writer, {"status": "failure", "error": f"Cannot remove class {class_name}, instances exist"})
            return
        del self.classes[class_name]
        del self.class_sources[class_name]
        await self._send(writer, {"status": "success", "message": f"Class {class_name} removed"})

    async def _handle_create_instance(self, writer, data):
        class_name = data.get("class_name")
        instance_name = data.get("instance_name")
        args = data.get("args", {})
        if class_name not in self.classes:
            await self._send(writer, {"status": "failure", "error": f"Class {class_name} does not exist"})
            return
        if instance_name in self.instances:
            await self._send(writer, {"status": "failure", "error": f"Instance {instance_name} already exists"})
            return
        cls = self.classes[class_name]
        try:
            instance = cls(**args)
        except TypeError as e:
            await self._send(writer, {"status": "failure", "error": f"Invalid constructor args: {e}"})
            return
        self.instances[instance_name] = instance
        await self._send(writer, {"status": "success", "message": f"Instance {instance_name} created"})

    async def _handle_destroy_instance(self, writer, data):
        instance_name = data.get("instance_name")
        if instance_name not in self.instances:
            await self._send(writer, {"status": "failure", "error": f"Instance {instance_name} does not exist"})
            return
        if instance_name in self.running:
            await self._send(writer, {"status": "failure", "error": f"Instance {instance_name} is running"})
            return
        del self.instances[instance_name]
        await self._send(writer, {"status": "success", "message": f"Instance {instance_name} destroyed"})

    async def _handle_invoke_function(self, writer, data):
        instance_name = data.get("instance_name")
        func_name = data.get("function_name")
        args = data.get("args", {})
        kwargs = data.get("kwargs", {})

        if instance_name not in self.instances:
            await self._send(writer, {"status": "failure", "error": f"Instance {instance_name} does not exist"})
            return

        instance = self.instances[instance_name]
        if not hasattr(instance, func_name):
            await self._send(writer, {"status": "failure", "error": f"Instance {instance_name} has no function {func_name}"})
            return

        func = getattr(instance, func_name)
        try:
            if asyncio.iscoroutinefunction(func):
                result = await func(**args)
            else:
                result = func(**args)
            await self._send(writer, {"status": "success", "message": f"Invoked {func_name} on {instance_name}", "result": result})
        except Exception as e:
            await self._send(writer, {"status": "failure", "error": f"Failed to invoke {func_name}: {e}", "traceback": traceback.format_exc()})

    async def _handle_ls(self, writer):
        overview = {cls_name: [inst_name for inst_name, obj in self.instances.items() if isinstance(obj, cls)]
                    for cls_name, cls in self.classes.items()}
        await self._send(writer, {"status": "success", "overview": overview})


async def server_loop():
    if os.path.exists(SOCKET_PATH):
        os.remove(SOCKET_PATH)

    cm = collection_manager()

    async def handler(reader, writer):
        data = await reader.read(65536)
        message = data.decode()
        await cm.handle_message(writer, message)
        writer.close()
        await writer.wait_closed()

    server = await asyncio.start_unix_server(handler, path=SOCKET_PATH)
    print(f"[Server] collection_manager listening on {SOCKET_PATH}")
    async with server:
        await server.serve_forever()

def main():
    try:
        asyncio.run(server_loop())
    except KeyboardInterrupt:
        print("[Server] collection_manager shutting down")

if __name__ == "__main__":
    main()
