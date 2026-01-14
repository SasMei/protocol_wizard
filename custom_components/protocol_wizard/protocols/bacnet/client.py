# protocols/bacnet/client.py
"""BACnet/IP client for Protocol Wizard - Based on BAC0 official docs."""

import logging
import asyncio
from typing import Any, Optional

_LOGGER = logging.getLogger(__name__)

try:
    import BAC0
    HAS_BAC0 = True
except ImportError:
    HAS_BAC0 = False
    _LOGGER.error("BAC0 library not installed")


class BACnetClient:
    """BACnet/IP client wrapper for BAC0 2025+."""
    
    def __init__(
        self, 
        host: str, 
        device_id: Optional[int] = None,
        port: int = 47808,
        network_number: Optional[int] = None
    ):
        """
        Initialize BACnet client.
        
        Args:
            host: IP address or "0.0.0.0" for discovery
            device_id: BACnet device instance (None for discovery)
            port: UDP port (default 47808)
            network_number: BACnet network number (None for local)
        """
        if not HAS_BAC0:
            raise ImportError("BAC0 library is required for BACnet support")
        
        self.host = host
        self.device_id = device_id
        self.port = port
        self.network_number = network_number
        self.bacnet = None
        self._connected = False
    
    
    async def connect(self) -> bool:
        """
        Connect to BACnet network asynchronously.
        """
        try:
            _LOGGER.info("Connecting to BACnet network on %s:%s", self.host, self.port)
    
            # Use BAC0.start() as async context manager (recommended in 2025+ docs)
            self.bacnet = await BAC0.start(
                ip=self.host,          # or '0.0.0.0' for broadcast
                port=self.port,
                device_id=self.device_id if self.device_id else None,
                # Optional: bbmdAddress if needed, etc.
            )
    
            # Wait a moment for initialization
            await asyncio.sleep(0.5)
    
            _LOGGER.info("BACnet connection established")
            self._connected = True
            return True
    
        except Exception as err:
            _LOGGER.error("BACnet connection failed: %s", err)
            import traceback
            traceback.print_exc()
            self._connected = False
            return False
    
    
    async def discover_devices(self, timeout: int = 10) -> list[dict]:
        try:
            _LOGGER.info("Starting BACnet device discovery (timeout: %ds)", timeout)
    
            if not self._connected:
                if not await self.connect():
                    return []
    
            # Use async discover if available, or run sync in executor as fallback
            _LOGGER.info("Sending Who-Is broadcast...")
            await asyncio.get_event_loop().run_in_executor(
                None,
                self.bacnet.discover
            )
    
            _LOGGER.info("Waiting %ds for I-Am responses...", timeout)
            await asyncio.sleep(timeout)
    
            # Collect discovered devices (sync part)
            devices = await asyncio.get_event_loop().run_in_executor(
                None,
                self._collect_discovered_devices
            )
    
            _LOGGER.info("Discovered %d BACnet devices", len(devices))
            return devices
    
        except Exception as err:
            _LOGGER.error("BACnet discovery failed: %s", err)
            return []
    
    
    def _collect_discovered_devices(self) -> list[dict]:
        """Collect devices from registered_devices (runs in executor)."""
        devices = []
        
        try:
            # Get registered devices
            registered = getattr(self.bacnet, 'registered_devices', None)
            
            if not registered:
                _LOGGER.warning("No registered_devices attribute found")
                return []
            
            _LOGGER.info("Found registered_devices: %s (type: %s)", 
                        registered, type(registered))
            
            # Handle dict format
            if isinstance(registered, dict):
                _LOGGER.info("Processing %d devices from dict", len(registered))
                for device_id, device_obj in registered.items():
                    try:
                        device_dict = self._parse_device(device_id, device_obj)
                        if device_dict:
                            devices.append(device_dict)
                            _LOGGER.info("Added device: %s", device_dict)
                    except Exception as err:
                        _LOGGER.warning("Error parsing device %s: %s", device_id, err)
            
            # Handle list/iterable format
            elif hasattr(registered, '__iter__'):
                _LOGGER.info("Processing devices from iterable")
                for device_obj in registered:
                    try:
                        # Extract device ID
                        device_id = getattr(device_obj, 'device_id', None)
                        if not device_id:
                            obj_id = getattr(device_obj, 'objectIdentifier', None)
                            if obj_id and len(obj_id) > 1:
                                device_id = obj_id[1]
                        
                        if device_id:
                            device_dict = self._parse_device(device_id, device_obj)
                            if device_dict:
                                devices.append(device_dict)
                    except Exception as err:
                        _LOGGER.warning("Error parsing device: %s", err)
            
            else:
                _LOGGER.warning("Unknown registered_devices format: %s", type(registered))
        
        except Exception as err:
            _LOGGER.error("Error collecting devices: %s", err)
            import traceback
            traceback.print_exc()
        
        return devices
    
    
    def _parse_device(self, device_id, device_obj) -> Optional[dict]:
        """Parse device object into dict."""
        try:
            # Extract address
            address = getattr(device_obj, 'address', None)
            if address:
                address = str(address).split(':')[0]
            else:
                address = self.host
            
            # Extract name
            name = getattr(device_obj, 'objectName', None)
            if not name:
                name = getattr(device_obj, 'name', f"Device {device_id}")
            
            # Extract vendor
            vendor = getattr(device_obj, 'vendorName', 'Unknown')
            
            return {
                'device_id': int(device_id),
                'address': address,
                'port': self.port,
                'name': name,
                'vendor': vendor,
            }
        
        except Exception as err:
            _LOGGER.warning("Error parsing device %s: %s", device_id, err)
            return None
    
    
    async def get_device_name(self) -> Optional[str]:
        """
        Get device name from BACnet device.
        
        Returns:
            Device name or None if not available
        """
        try:
            if not self._connected or not self.device_id:
                return None
            
            # Try to read device object name
            name = await self.read_property("device", self.device_id, "objectName")
            return name
        
        except Exception as err:
            _LOGGER.warning("Could not read device name: %s", err)
            return None
    
    
    async def read_property(
        self, 
        object_type: str, 
        object_instance: int, 
        property_name: str
    ) -> Optional[Any]:
        """
        Read BACnet property.
        
        Args:
            object_type: BACnet object type (e.g., 'analogInput', 'binaryValue')
            object_instance: Object instance number
            property_name: Property name (e.g., 'presentValue')
        
        Returns:
            Property value or None if read failed
        """
        if not self._connected:
            _LOGGER.error("Not connected to BACnet network")
            return None
        
        try:
            # Format: "address object_type instance property"
            # Example: "192.168.1.10 analogInput 0 presentValue"
            read_string = f"{self.host} {object_type} {object_instance} {property_name}"
            
            _LOGGER.debug("Reading: %s", read_string)
            
            # Execute read in executor
            result = await asyncio.get_event_loop().run_in_executor(
                None,
                self.bacnet.read,
                read_string
            )
            
            _LOGGER.debug("Read result: %s (type: %s)", result, type(result))
            return result
        
        except Exception as err:
            _LOGGER.error("Read failed for %s:%s.%s: %s", 
                         object_type, object_instance, property_name, err)
            import traceback
            traceback.print_exc()
            return None
    
    
    async def write_property(
        self,
        object_type: str,
        object_instance: int,
        property_name: str,
        value: Any,
        priority: int = 8
    ) -> bool:
        """
        Write BACnet property.
        
        Args:
            object_type: BACnet object type
            object_instance: Object instance number
            property_name: Property name
            value: Value to write
            priority: Write priority (1-16, default 8)
        
        Returns:
            True if write successful, False otherwise
        """
        if not self._connected:
            _LOGGER.error("Not connected to BACnet network")
            return False
        
        try:
            # Format: "address object_type instance property value - priority"
            # Example: "192.168.1.10 analogOutput 0 presentValue 75.0 - 8"
            write_string = f"{self.host} {object_type} {object_instance} {property_name} {value} - {priority}"
            
            _LOGGER.debug("Writing: %s", write_string)
            
            # Execute write in executor
            await asyncio.get_event_loop().run_in_executor(
                None,
                self.bacnet.write,
                write_string
            )
            
            _LOGGER.info("Wrote %s to %s:%s.%s (priority %d)", 
                         value, object_type, object_instance, property_name, priority)
            return True
        
        except Exception as err:
            _LOGGER.error("Write failed for %s:%s.%s: %s", 
                         object_type, object_instance, property_name, err)
            import traceback
            traceback.print_exc()
            return False
    
    
    async def disconnect(self):
        """Disconnect from BACnet network."""
        if self.bacnet:
            try:
                # Disconnect in executor
                await asyncio.get_event_loop().run_in_executor(
                    None,
                    self.bacnet.disconnect
                )
                _LOGGER.info("Disconnected from BACnet network")
            except Exception as err:
                _LOGGER.error("Error disconnecting: %s", err)
            finally:
                self.bacnet = None
                self._connected = False
    
    
    @property
    def connected(self) -> bool:
        """Return connection status."""
        return self._connected
