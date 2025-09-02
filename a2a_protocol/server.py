"""
A2A Protocol JSON-RPC Server

This module implements an asynchronous JSON-RPC 2.0 server for A2A communication,
supporting the enhanced message types and providing extensible method handling.
"""

import asyncio
import json
import logging
import os
from typing import Any, Callable, Dict, Optional
from aiohttp import web
from aiohttp.web import Request, Response

from .validation import MessageValidator
from .schemas import JSONRPCRequest, JSONRPCNotification, JSONRPCErrorResponse

logger = logging.getLogger(__name__)


class A2AServer:
    """Asynchronous JSON-RPC server for A2A protocol"""

    def __init__(self, host: str = None, port: int = None):
        self.host = host or os.getenv("A2A_HOST", "0.0.0.0")
        self.port = port or int(os.getenv("A2A_PORT", "8080"))
        self.host = host
        self.port = port
        self.app = web.Application()
        self.methods: Dict[str, Callable] = {}
        self._setup_routes()

    def _setup_routes(self):
        """Setup HTTP routes"""
        self.app.router.add_post("/jsonrpc", self._handle_jsonrpc)
        self.app.router.add_get("/health", self._handle_health)

    async def _handle_jsonrpc(self, request: Request) -> Response:
        """Handle JSON-RPC requests"""
        try:
            raw_data = await request.text()
            logger.debug(f"Received raw message: {raw_data}")

            # Validate and parse the message
            parsed_message = MessageValidator.validate_jsonrpc_message(raw_data)

            if isinstance(parsed_message, JSONRPCErrorResponse):
                return self._json_response(parsed_message.dict())

            # Handle notification (no response expected)
            if isinstance(parsed_message, JSONRPCNotification):
                asyncio.create_task(self._handle_notification(parsed_message))
                return Response(status=204)  # No Content

            # Handle request
            if isinstance(parsed_message, JSONRPCRequest):
                response = await self._handle_request(parsed_message)
                return self._json_response(response.dict())

        except Exception as e:
            logger.error(f"Error handling request: {e}")
            error_response = MessageValidator._create_error_response(
                -32603, "Internal error", str(e), None
            )
            return self._json_response(error_response.dict())

    async def _handle_request(self, rpc_request: JSONRPCRequest) -> Any:
        """Handle JSON-RPC request and return response"""
        method = rpc_request.method
        params = rpc_request.params
        msg_id = rpc_request.id

        if method not in self.methods:
            return MessageValidator._create_error_response(
                -32601, "Method not found", f"Method '{method}' not found", msg_id
            )

        try:
            # Validate A2A message if it's a known type
            a2a_message = MessageValidator.validate_a2a_message(method, params)
            if a2a_message is None:
                return MessageValidator._create_error_response(
                    -32602, "Invalid params", "Invalid A2A message parameters", msg_id
                )

            # Validate business rules
            if not MessageValidator.validate_business_rules(a2a_message):
                return MessageValidator._create_error_response(
                    -32602, "Invalid params", "Business rule validation failed", msg_id
                )

            # Call the method handler
            handler = self.methods[method]
            result = await handler(a2a_message)

            return MessageValidator.create_success_response(result, msg_id)

        except Exception as e:
            logger.error(f"Error executing method {method}: {e}")
            return MessageValidator._create_error_response(
                -32603, "Internal error", str(e), msg_id
            )

    async def _handle_notification(self, notification: JSONRPCNotification):
        """Handle JSON-RPC notification asynchronously"""
        method = notification.method
        params = notification.params

        if method not in self.methods:
            logger.warning(f"Notification for unknown method: {method}")
            return

        try:
            a2a_message = MessageValidator.validate_a2a_message(method, params)
            if a2a_message and MessageValidator.validate_business_rules(a2a_message):
                handler = self.methods[method]
                await handler(a2a_message)
            else:
                logger.warning(f"Invalid notification parameters for {method}")
        except Exception as e:
            logger.error(f"Error handling notification {method}: {e}")

    async def _handle_health(self, request: Request) -> Response:
        """Health check endpoint"""
        return self._json_response({"status": "healthy", "methods": list(self.methods.keys())})

    def _json_response(self, data: Dict[str, Any]) -> Response:
        """Create JSON response"""
        return Response(
            text=json.dumps(data),
            content_type="application/json",
            headers={"Access-Control-Allow-Origin": "*"}
        )

    def register_method(self, method_name: str, handler: Callable):
        """
        Register a method handler.

        Args:
            method_name: The JSON-RPC method name
            handler: Async callable that takes an A2AMessage and returns a result
        """
        self.methods[method_name] = handler
        logger.info(f"Registered method: {method_name}")

    async def start(self):
        """Start the server"""
        logger.info(f"Starting A2A server on {self.host}:{self.port}")
        runner = web.AppRunner(self.app)
        await runner.setup()
        site = web.TCPSite(runner, self.host, self.port)
        await site.start()
        logger.info("A2A server started successfully")

    def run_forever(self):
        """Run the server indefinitely"""
        asyncio.run(self.start())
        try:
            asyncio.get_event_loop().run_forever()
        except KeyboardInterrupt:
            logger.info("Server stopped by user")


# Example usage
if __name__ == "__main__":
    server = A2AServer()

    # Register example handlers
    async def handle_geospatial_anomaly(message):
        logger.info(f"Handling geospatial anomaly: {message.description}")
        return {"status": "processed", "anomaly_id": message.message_id}

    async def handle_inspiration_request(message):
        logger.info(f"Handling inspiration request: {message.context}")
        return {"inspiration": "Generated creative content"}

    server.register_method("GEOSPATIAL_ANOMALY_IDENTIFIED", handle_geospatial_anomaly)
    server.register_method("INSPIRATION_REQUEST", handle_inspiration_request)

    server.run_forever()