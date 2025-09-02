"""
Error tracking and monitoring for Terra Constellata.
Integrates Sentry for comprehensive error reporting and monitoring.
"""

import os
import sentry_sdk
from sentry_sdk.integrations.fastapi import FastApiIntegration
from sentry_sdk.integrations.sqlalchemy import SqlAlchemyIntegration
from sentry_sdk.integrations.logging import LoggingIntegration
from .logging_config import app_logger

def init_sentry():
    """Initialize Sentry error tracking."""
    sentry_dsn = os.getenv("SENTRY_DSN")
    environment = os.getenv("ENVIRONMENT", "development")

    if sentry_dsn:
        sentry_sdk.init(
            dsn=sentry_dsn,
            environment=environment,
            integrations=[
                FastApiIntegration(),
                SqlAlchemyIntegration(),
                LoggingIntegration(
                    level=None,  # Capture all log levels
                    event_level=None  # Send all events to Sentry
                ),
            ],
            # Performance monitoring
            traces_sample_rate=1.0 if environment == "production" else 0.1,
            # Release tracking
            release=os.getenv("RELEASE_VERSION", "1.0.0"),
            # Error filtering
            before_send=before_send,
            # User feedback
            send_default_pii=True,
        )

        app_logger.info("Sentry error tracking initialized", extra={
            "environment": environment,
            "release": os.getenv("RELEASE_VERSION", "1.0.0")
        })
    else:
        app_logger.warning("SENTRY_DSN not configured, error tracking disabled")

def before_send(event, hint):
    """Filter and modify events before sending to Sentry."""
    # Don't send events in development for certain log levels
    if os.getenv("ENVIRONMENT") == "development":
        if event.get("level") == "info":
            return None

    # Add custom tags
    if "tags" not in event:
        event["tags"] = {}

    event["tags"]["service"] = "terra-constellata-backend"
    event["tags"]["component"] = "api"

    return event

def capture_exception(exc, **kwargs):
    """Capture an exception with additional context."""
    with sentry_sdk.configure_scope() as scope:
        for key, value in kwargs.items():
            scope.set_tag(key, value)

        sentry_sdk.capture_exception(exc)

def capture_message(message, level="info", **kwargs):
    """Capture a message with additional context."""
    with sentry_sdk.configure_scope() as scope:
        for key, value in kwargs.items():
            scope.set_tag(key, value)

        sentry_sdk.capture_message(message, level=level)

def set_user_context(user_id=None, email=None, username=None):
    """Set user context for error tracking."""
    with sentry_sdk.configure_scope() as scope:
        if user_id:
            scope.user = {
                "id": user_id,
                "email": email,
                "username": username
            }

def set_request_context(request_id=None, method=None, path=None):
    """Set request context for error tracking."""
    with sentry_sdk.configure_scope() as scope:
        if request_id:
            scope.set_tag("request_id", request_id)
        if method:
            scope.set_tag("method", method)
        if path:
            scope.set_tag("path", path)

def track_performance(operation_name, **kwargs):
    """Track performance of operations."""
    def decorator(func):
        def wrapper(*args, **kwargs_inner):
            with sentry_sdk.start_transaction(op=operation_name, name=operation_name):
                return func(*args, **kwargs_inner)
        return wrapper
    return decorator

# Initialize Sentry on module import
init_sentry()