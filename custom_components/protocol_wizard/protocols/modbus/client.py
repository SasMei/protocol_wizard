#------------------------------------------
#-- protocol client.py protocol wizard
#------------------------------------------
"""Modbus protocol client wrapper."""
from __future__ import annotations

import logging
from typing import Any

from pymodbus.exceptions import ModbusIOException

from ..base import BaseProtocolClient

_LOGGER = logging.getLogger(__name__)
# Reduce noise from pymodbus
# Setting parent logger to CRITICAL to catch all sub-loggers
logging.getLogger("pymodbus").setLevel(logging.CRITICAL)
logging.getLogger("pymodbus.logging").setLevel(logging.CRITICAL)

class ModbusClient(BaseProtocolClient):
    """Wrapper for pymodbus clients to match BaseProtocolClient interface."""

    def __init__(self, pymodbus_client, slave_id: int):
        """
        Initialize Modbus client wrapper.

        Args:
            pymodbus_client: The underlying pymodbus AsyncModbus*Client
            slave_id: Modbus slave/device ID
        """
        self._client = pymodbus_client
        self.slave_id = int(slave_id)
        self._connection_failed = False  # Track connection health

    async def connect(self) -> bool:
        """Establish connection."""
        try:
            await self._client.connect()
            self._connection_failed = False
            return self._client.connected
        except Exception as err:
            _LOGGER.error("Modbus connection failed: %s", err)
            self._connection_failed = True
            return False

    async def disconnect(self) -> None:
        """Close connection."""
        try:
            if self._client.connected:
                self._client.close()
            self._connection_failed = False
        except Exception as err:
            _LOGGER.debug("Error closing Modbus client: %s", err)

    async def reconnect(self) -> bool:
        """Force reconnection - close and reopen."""
        _LOGGER.info("Forcing Modbus reconnection...")
        try:
            # Force close regardless of state
            try:
                self._client.close()
            except Exception:
                pass

            # Small delay before reconnecting
            import asyncio
            await asyncio.sleep(0.5)

            # Reconnect
            await self._client.connect()
            self._connection_failed = False
            _LOGGER.info("Modbus reconnection successful")
            return self._client.connected
        except Exception as err:
            _LOGGER.error("Modbus reconnection failed: %s", err)
            self._connection_failed = True
            return False

    async def read(self, address: str, **kwargs) -> Any:
        """
        Read Modbus register(s).

        Kwargs:
            count: Number of registers to read
            register_type: "holding", "input", "coil", "discrete"
        """
        addr = int(address)
        count = int(kwargs.get("count", 1))
        reg_type = kwargs.get("register_type", "holding")

        method_map = {
            "holding": self._client.read_holding_registers,
            "input": self._client.read_input_registers,
            "coil": self._client.read_coils,
            "discrete": self._client.read_discrete_inputs,
        }

        method = method_map.get(reg_type)
        if not method:
            raise ValueError(f"Invalid register type: {reg_type}")

        try:
            result = await method(
                address=addr,
                count=count,
                device_id=int(self.slave_id),
            )

            if result.isError():
                return None

            # Success - mark connection as healthy
            self._connection_failed = False

            # Return registers or bits depending on type
            if reg_type in ("coil", "discrete"):
                return result.bits[:count]
            return result.registers[:count]

        except ModbusIOException as err:
            _LOGGER.warning("Modbus I/O error reading address %s: %s", address, err)
            self._connection_failed = True  # Mark for reconnection
            raise  # Re-raise so coordinator can handle
        except Exception as err:
            _LOGGER.error("Modbus read error at %s: %s", address, err)
            self._connection_failed = True
            raise

    async def write(self, address: str, value: Any, **kwargs) -> bool:
        addr = int(address)
        reg_type = kwargs.get("register_type", "holding").lower()
     #   _LOGGER.debug("write called: addr=%s, value=%r (type=%s), reg_type=%s", addr, value, type(value).__name__, reg_type)
        try:
            if reg_type == "coil":
                if isinstance(value, list):
                    result = await self._client.write_coils(
                        address=addr,
                        values=[bool(v) for v in value],
                        device_id=self.slave_id,
                    )
                else:
                    result = await self._client.write_coil(
                        address=addr,
                        value=bool(value),
                        device_id=self.slave_id,
                    )
            elif reg_type == "holding":
                if isinstance(value, list):
                    result = await self._client.write_registers(
                        address=addr,
                        values=[int(v) for v in value],
                        device_id=self.slave_id,
                    )
                else:
                    result = await self._client.write_register(
                        address=addr,
                        value=int(value),
                        device_id=self.slave_id,
                    )
            elif reg_type in ("input", "discrete"):
                _LOGGER.error("Cannot write to read-only %s registers", reg_type)
                return False
            else:
                _LOGGER.error("Unsupported register_type '%s'", reg_type)
                return False

            success = not result.isError()
            if success:
                self._connection_failed = False
            return success

        except ModbusIOException as err:
            _LOGGER.warning("Modbus I/O error writing to %s: %s", address, err)
            self._connection_failed = True
            return False
        except Exception as err:
            _LOGGER.error("Modbus write failed at %s: %s", address, err)
            self._connection_failed = True
            return False

    @property
    def is_connected(self) -> bool:
        """Check if connected and healthy."""
        # Consider connection unhealthy if we've had I/O errors
        if self._connection_failed:
            return False
        return self._client.connected

    @property
    def needs_reconnect(self) -> bool:
        """Check if reconnection is needed due to errors."""
        return self._connection_failed

    # Expose underlying client for protocol-specific methods
    @property
    def raw_client(self):
        """Get the underlying pymodbus client for advanced operations."""
        return self._client
