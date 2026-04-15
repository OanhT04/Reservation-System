"""
config.py - Central configuration for the distributed restaurant reservation system.

All network addresses, ports, timeouts, and shared settings live here.
Every component imports from this file so changes propagate everywhere.
"""

# ─── Gateway (REST API that clients talk to) ───────────────────────────
GATEWAY_HOST = "127.0.0.1"
GATEWAY_PORT = 5000

# ─── Service Instances (TCP servers) ───────────────────────────────────
# Each restaurant is handled by its own service instance (sharding).
# Format: restaurant_id -> (host, port)
# where we can add more restauraunts
RESTAURANT_SERVICE_MAP = {
    "restaurant1": ("127.0.0.1", 6001),
    "restaurant2": ("127.0.0.1", 6002),
}

# User service
USER_SERVICE_HOST = "127.0.0.1"
USER_SERVICE_PORT = 6100

# ─── Replication ───────────────────────────────────────────────────────
# Each primary has a backup. Format: primary_port -> backup_port
BACKUP_MAP = {
    6001: 7001,  # restaurant_1 primary -> backup
    6002: 7002,  # restaurant_2 primary -> backup
}
REPLICATION_HOST = "127.0.0.1"

# ─── Heartbeat ─────────────────────────────────────────────────────────
HEARTBEAT_INTERVAL = 2.0      # seconds between heartbeats
HEARTBEAT_TIMEOUT = 6.0       # seconds before declaring primary dead
HEARTBEAT_PORT_OFFSET = 1000  # backup heartbeat port = primary_port + offset

# ─── Concurrency ──────────────────────────────────────────────────────
LOCK_TIMEOUT = 5.0  # seconds to wait for a table lock before giving up

# ─── General ──────────────────────────────────────────────────────────
BUFFER_SIZE = 4096
ENCODING = "utf-8"
