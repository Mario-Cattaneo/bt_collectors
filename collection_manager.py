#!/usr/bin/env python3
import asyncio
import json
import types
import traceback
from typing import Any, Dict


class DynamicManager:
    def __init__(self):
        self.classes: Dict[str, type] = {}        # class_name -> class
        self.instances: Dict[str, Any] = {}       # instance_name -> instance

    def add_class(self, class_name: str, code: str):
        """Define a new class from code string"""
        if class_name in self.classes:
            raise ValueError(f"Class {class_name} already exists")
        namespace = {}
        exec(code, namespace)
        if class_name not in namespace:
            raise ValueError(f"Class {class_name} not found in code")
        cls = namespace[class_name]
        self.classes[class_name] = cls
        return {"status": "ok"}

    def remove_class(self, class_name: str):
        if class_name not in self.classes:
            raise ValueError(f"No such class {class_name}")
        # Remove instances of this class
        for inst_name in list(self.instances.keys()):
            if type(self.instances[inst_name]).__name__ == class_name:
                del self.instances[inst_name]
        del self.classes[class_name]
        return {"status": "ok"}

    def instantiate(self, class_name: str, inst_name: str, args: dict):
        if inst_name in self.instances:
            raise ValueError(f"Instance {inst_name} already exists")
        if class_name not in self.classes:
            raise ValueError(f"No such class {class_name}")
        cls = self.classes[class_name]
        instance = cls(**args)
        self.instances[inst_name] = instance
        return {"status": "ok"}

    def remove_instance(self, inst_name: str):
        if inst_name not in self.instances:
            raise ValueError(f"No such instance {inst_name}")
        del self.instances[inst_name]
        return {"status": "ok"}

    async def dispatch(self, inst_name: str, func: str, args: dict):
        if inst_name not in self.instances:
            raise ValueError(f"No such instance {inst_name}")
        instance = self.instances[inst_name]
        if not hasattr(instance, func):
            raise ValueError(f"{inst_name} has no method {func}")
        method = getattr(instance, func)
        if asyncio.iscoroutinefunction(method):
            result = await method(**args)
        else:
            result = method(**args)
        return {"status": "ok", "result": result}


async def handle_client(reader: asyncio.StreamReader, writer: asyncio.StreamWriter, manager: DynamicManager):
    try:
        while not reader.at_eof():
            data = await reader.readline()
            if not data:
                break
            try:
                msg = json.loads(data.decode())
                action = msg.get("action")
                if action == "add_class":
                    result = manager.add_class(msg["name"], msg["code"])
                elif action == "remove_class":
                    result = manager.remove_class(msg["name"])
                elif action == "instantiate":
                    result = manager.instantiate(msg["class"], msg["name"], msg.get("args", {}))
                elif action == "remove_instance":
                    result = manager.remove_instance(msg["name"])
                elif action == "dispatch":
                    result = await manager.dispatch(msg["name"], msg["func"], msg.get("args", {}))
                else:
                    result = {"status": "error", "error": f"Unknown action {action}"}
            except Exception as e:
                result = {"status": "error", "error": str(e), "traceback": traceback.format_exc()}
            writer.write((json.dumps(result) + "\n").encode())
            await writer.drain()
    finally:
        writer.close()
        await writer.wait_closed()


async def main(socket_path="/tmp/dyn_manager.sock"):
    import os
    try:
        os.unlink(socket_path)
    except FileNotFoundError:
        pass

    manager = DynamicManager()
    server = await asyncio.start_unix_server(
        lambda r, w: handle_client(r, w, manager), path=socket_path
    )
    print(f"Dynamic manager listening on {socket_path}")
    async with server:
        await server.serve_forever()


if __name__ == "__main__":
    asyncio.run(main())
