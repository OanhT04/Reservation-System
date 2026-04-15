"""
reservation_service.py - Core reservation management service.

This is the HEART of the distributed system. It demonstrates:

1. CONCURRENCY CONTROL (Mutual Exclusion)
   ───────────────────────────────────────
   Multiple clients may try to book the same table at the same time.
   We use per-table locks (threading.Lock) to ensure only one booking
   can proceed at a time for any given table+timeslot combination.
   
   Without locking, two concurrent requests could both check availability,
   both see the table is free, and both confirm — resulting in a double
   booking (a classic race condition).

2. REPLICATION (Primary-Backup with Synchronous Writes)
   ────────────────────────────────────────────────────
   After a booking is confirmed locally, we replicate the write to the
   backup BEFORE responding to the client. This ensures no data loss
   if the primary crashes.

3. SCALABILITY (Sharding by Restaurant)
   ────────────────────────────────────
   Each restaurant runs its own instance of this service on a different
   port. This means Restaurant A's traffic doesn't affect Restaurant B.
   Adding a new restaurant = starting a new service instance.

TCP Server Architecture:
   The service listens on a TCP port for requests from the gateway.
   Each incoming connection is handled in a separate thread (thread-per-client).
"""

import json
import os
import socket
import threading
import time
import logging
from common.config import (
    REPLICATION_HOST,
    LOCK_TIMEOUT,
    HEARTBEAT_PORT_OFFSET,
)
from common.protocol import send_message, receive_message
from replication.primary import PrimaryReplicator
from replication.heartbeat import HeartbeatSender

logger = logging.getLogger(__name__)


class ReservationService:
    """
    TCP server that manages reservations for a single restaurant.
    
    Each instance handles one restaurant (sharding). It maintains:
    - An in-memory reservation store: {(restaurant_id, table_id, timeslot): reservation_data}
    - A lock per table to prevent concurrent booking conflicts
    - A replicator that syncs writes to the backup server
    - A heartbeat sender so the backup knows we're alive
    """

    def __init__(self, restaurant_id: str, host: str, port: int, data_path: str):
        """
        Args:
            restaurant_id: Which restaurant this instance serves
            host: IP address to bind to
            port: TCP port to listen on
            data_path: Path to restaurants.json for initial data
        """
        self.restaurant_id = restaurant_id
        self.host = host
        self.port = port

        # ─── Load restaurant data ──────────────────────────────────
        with open(data_path, "r") as f:
            all_data = json.load(f)
        self.restaurant_info = all_data[restaurant_id]

        # ─── In-memory reservation store ───────────────────────────
        self.reservations = {}

        # ─── Concurrency: one lock per table ───────────────────────
        # This prevents two threads from booking the same table simultaneously
        self.table_locks = {}
        for table_id in self.restaurant_info["tables"]:
            self.table_locks[table_id] = threading.Lock()

        # ─── Replication: send writes to backup ────────────────────
        self.replicator = PrimaryReplicator(primary_port=port)

        # ─── Heartbeat: tell backup we're alive ────────────────────
        backup_heartbeat_port = port + HEARTBEAT_PORT_OFFSET
        self.heartbeat_sender = HeartbeatSender(
            backup_heartbeat_port=backup_heartbeat_port
        )

        # ─── Logical timestamp counter ─────────────────────────────
        # Incremented on every write operation.
        # Used to order events and included in reservation records.
        self.logical_clock = 0
        self.clock_lock = threading.Lock()

        self.running = False

    def _tick_clock(self) -> int:
        """
        Increment and return the logical clock.
        
        DISTRIBUTED SYSTEMS CONCEPT: Logical Time (Lamport Timestamps)
        ──────────────────────────────────────────────────────────────
        In a distributed system, physical clocks on different machines
        may not be synchronized. Logical clocks provide a consistent
        ordering of events: if event A happened before event B,
        then clock(A) < clock(B).
        
        We increment the clock on every write (booking/cancellation).
        """
        with self.clock_lock:
            self.logical_clock += 1
            return self.logical_clock

    def start(self):
        """Start the TCP server, replicator, and heartbeat sender."""
        self.running = True
        self.heartbeat_sender.start()

        server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        server.bind((self.host, self.port))
        server.listen(20)
        server.settimeout(1.0)

        logger.info(
            f"ReservationService [{self.restaurant_info['name']}] "
            f"listening on {self.host}:{self.port}"
        )

        while self.running:
            try:
                conn, addr = server.accept()
                # Each client connection gets its own thread
                threading.Thread(
                    target=self._handle_client,
                    args=(conn, addr),
                    daemon=True,
                ).start()
            except socket.timeout:
                continue

        server.close()
        self.heartbeat_sender.stop()

    def stop(self):
        self.running = False

    def _handle_client(self, conn: socket.socket, addr: tuple):
        """
        Handle a single request from the gateway.
        
        Reads the action from the message and dispatches to the
        appropriate handler method.
        """
        try:
            msg = receive_message(conn)
            action = msg.get("action")

            if action == "check_availability":
                response = self._check_availability(msg)
            elif action == "book":
                response = self._book_table(msg)
            elif action == "cancel":
                response = self._cancel_reservation(msg)
            elif action == "list_reservations":
                response = self._list_reservations(msg)
            elif action == "get_info":
                response = self._get_restaurant_info()
            else:
                response = {"status": "error", "message": f"Unknown action: {action}"}

            send_message(conn, response)

        except Exception as e:
            logger.error(f"Error handling client {addr}: {e}")
            try:
                send_message(conn, {"status": "error", "message": str(e)})
            except Exception:
                pass
        finally:
            conn.close()

    def _get_restaurant_info(self) -> dict:
        """Return restaurant metadata (name, tables, timeslots)."""
        return {
            "status": "ok",
            "restaurant_id": self.restaurant_id,
            "name": self.restaurant_info["name"],
            "tables": self.restaurant_info["tables"],
            "timeslots": self.restaurant_info["timeslots"],
        }

    def _check_availability(self, msg: dict) -> dict:
        """
        Check which tables are available for a given date and timeslot.
        
        This is a READ operation — no locking needed because we're just
        reading the current state. (In a stricter system, you'd use
        read locks, but for a demo this is fine.)
        
        Args (in msg):
            date: The date to check (e.g., "2025-05-15")
            timeslot: The timeslot to check (e.g., "19:00")
            party_size: Number of guests (filters by table capacity)
        """
        date = msg.get("date")
        timeslot = msg.get("timeslot")
        party_size = msg.get("party_size", 1)

        available = []
        for table_id, table_info in self.restaurant_info["tables"].items():
            if table_info["capacity"] < party_size:
                continue  # Table too small

            key = (self.restaurant_id, table_id, f"{date}_{timeslot}")
            if key not in self.reservations:
                available.append({
                    "table_id": table_id,
                    "capacity": table_info["capacity"],
                    "location": table_info["location"],
                })

        return {
            "status": "ok",
            "restaurant_id": self.restaurant_id,
            "date": date,
            "timeslot": timeslot,
            "available_tables": available,
        }

    def _book_table(self, msg: dict) -> dict:
        """
        Book a specific table at a specific timeslot.
        
        CONCURRENCY CONTROL IN ACTION:
        ──────────────────────────────
        1. Acquire the lock for this table (with timeout)
        2. Check if the table is still available (it may have been
           booked by another thread while we were waiting for the lock)
        3. Create the reservation record
        4. Replicate to backup (synchronous — blocks until ACK)
        5. Release the lock
        6. Return success to client
        
        If the lock times out, we return an error — the table is busy.
        This is PESSIMISTIC concurrency control.
        
        Args (in msg):
            table_id: Which table to book
            date: Reservation date
            timeslot: Reservation time
            customer_name: Who's booking
            party_size: Number of guests
            contact: Phone or email
        """
        table_id = msg.get("table_id")
        date = msg.get("date")
        timeslot = msg.get("timeslot")
        customer_name = msg.get("customer_name")
        party_size = msg.get("party_size", 1)
        contact = msg.get("contact", "")

        # Validate table exists
        if table_id not in self.table_locks:
            return {"status": "error", "message": f"Table {table_id} does not exist"}

        # ─── Step 1: Acquire lock with timeout ─────────────────────
        lock = self.table_locks[table_id]
        acquired = lock.acquire(timeout=LOCK_TIMEOUT)

        if not acquired:
            logger.warning(f"Lock timeout for table {table_id} at {timeslot}")
            return {
                "status": "error",
                "message": f"Table {table_id} is currently being booked by another customer. Try again.",
            }

        try:
            # ─── Step 2: Double-check availability (under lock) ────
            key = (self.restaurant_id, table_id, f"{date}_{timeslot}")
            if key in self.reservations:
                return {
                    "status": "error",
                    "message": f"Table {table_id} at {timeslot} is already booked",
                }

            # ─── Step 3: Create reservation record ────────────────
            timestamp = self._tick_clock()
            reservation = {
                "restaurant_id": self.restaurant_id,
                "table_id": table_id,
                "date": date,
                "timeslot": timeslot,
                "customer_name": customer_name,
                "party_size": party_size,
                "contact": contact,
                "logical_timestamp": timestamp,
                "created_at": time.time(),
            }

            # ─── Step 4: Replicate to backup (synchronous) ────────
            repl_success = self.replicator.replicate("book", reservation)
            if not repl_success:
                logger.warning("Replication failed — proceeding anyway (backup may be down)")

            # ─── Step 5: Commit locally ────────────────────────────
            self.reservations[key] = reservation

            logger.info(
                f"BOOKED: {customer_name} → {table_id} at {date} {timeslot} "
                f"(logical_ts={timestamp})"
            )

            return {
                "status": "ok",
                "message": "Reservation confirmed!",
                "reservation": reservation,
            }

        finally:
            # ─── Step 6: Release lock ──────────────────────────────
            lock.release()

    def _cancel_reservation(self, msg: dict) -> dict:
        """
        Cancel an existing reservation.
        
        Also uses locking and replication — cancellation is a write operation.
        """
        table_id = msg.get("table_id")
        date = msg.get("date")
        timeslot = msg.get("timeslot")

        if table_id not in self.table_locks:
            return {"status": "error", "message": f"Table {table_id} does not exist"}

        lock = self.table_locks[table_id]
        acquired = lock.acquire(timeout=LOCK_TIMEOUT)

        if not acquired:
            return {"status": "error", "message": "Could not acquire lock. Try again."}

        try:
            key = (self.restaurant_id, table_id, f"{date}_{timeslot}")
            if key not in self.reservations:
                return {"status": "error", "message": "No reservation found to cancel"}

            reservation = self.reservations[key]

            # Replicate cancellation to backup
            self.replicator.replicate("cancel", {
                "restaurant_id": self.restaurant_id,
                "table_id": table_id,
                "timeslot": f"{date}_{timeslot}",
            })

            # Remove locally
            del self.reservations[key]
            timestamp = self._tick_clock()

            logger.info(f"CANCELLED: {table_id} at {date} {timeslot} (logical_ts={timestamp})")

            return {
                "status": "ok",
                "message": "Reservation cancelled",
                "cancelled": reservation,
            }

        finally:
            lock.release()

    def _list_reservations(self, msg: dict) -> dict:
        """List all reservations, optionally filtered by date."""
        date = msg.get("date")
        results = []
        for key, reservation in self.reservations.items():
            if date and reservation.get("date") != date:
                continue
            results.append(reservation)

        return {
            "status": "ok",
            "reservations": results,
            "count": len(results),
        }
