"""BACnet/IP client for Protocol Wizard."""

import logging
import asyncio
from typing import Any, Optional

_LOGGER = logging.getLogger(__name__)

try:
    import BAC0
 #   from BAC0.core.devices.Device import Device
    HAS_BAC0 = True
except ImportError:
    HAS_BAC0 = False
    _LOGGER.error("BAC0 library not installed")


class BACnetClient:
    """BACnet/IP client wrapper for BAC0."""
    
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
        self.device = None
        self._connected = False
    
    
    async def connect(self) -> bool:
        """
        Connect to BACnet device.
        
        Returns:
            True if connection successful, False otherwise
        """
        try:
            # BAC0.connect is blocking, run in executor
            _LOGGER.info("Connecting to BACnet device %s:%s (ID: %s)", 
                        self.host, self.port, self.device_id)
            
            # Create BAC0 connection (now async)
            self.bacnet = await self._create_bacnet_connection()
            
            if not self.bacnet:
                _LOGGER.error("Failed to create BACnet connection")
                return False
            
            # Connect to specific device if device_id provided
            if self.device_id is not None:
                device_address = f"{self.host}:{self.port}"
                
                # Discover device (blocking call, use executor)
                self.device = await asyncio.get_event_loop().run_in_executor(
                    None,
                    self.bacnet.discover,
                    device_address,
                    self.device_id
                )
                
                if not self.device:
                    _LOGGER.error("Device ID %s not found at %s", 
                                self.device_id, device_address)
                    return False
                
                _LOGGER.info("Connected to BACnet device: %s", 
                           self.device.properties.name)
            
            self._connected = True
            return True
        
        except Exception as err:
            _LOGGER.error("BACnet connection failed: %s", err)
            self._connected = False
            return False
    
    
    async def _create_bacnet_connection(self):
        """Create BAC0 connection (async)."""
        try:
            # BAC0 2025+ is async, so we can call it directly
            # Using specific IP if provided, or 0.0.0.0 for discovery
            bacnet = BAC0.connect(ip=self.host, port=self.port)
            
            # Give it a moment to initialize
            await asyncio.sleep(0.5)
            
            return bacnet
        except Exception as err:
            _LOGGER.error("Failed to create BAC0 connection: %s", err)
            return None
    
    
    async def discover_devices(self, timeout: int = 10) -> list[dict]:
        """
        Discover BACnet devices on the network using Who-Is.
        
        Args:
            timeout: Discovery timeout in seconds
        
        Returns:
            List of discovered devices with their properties
        """
        try:
            _LOGGER.info("Starting BACnet device discovery (timeout: %ds)", timeout)
            
            # Create temporary connection for discovery
            if not self.bacnet:
                await self.connect()
            
            if not self.bacnet:
                _LOGGER.error("Cannot discover without BACnet connection")
                return []
            
            # Perform Who-Is discovery
            discovered = await asyncio.get_event_loop().run_in_executor(
                None,
                self._discover_devices_blocking,
                timeout
            )
            
            _LOGGER.info("Discovered %d BACnet devices", len(discovered))
            return discovered
        
        except Exception as err:
            _LOGGER.error("BACnet discovery failed: %s", err)
            return []
    
    
    def _discover_devices_blocking(self, timeout: int) -> list[dict]:
        """
        Blocking discovery function.
        
        Returns:
            List of dicts with device info: {
                'device_id': int,
                'address': str,
                'port': int,
                'name': str,
                'vendor': str
            }
        """
        import time
        
        devices = []
        
        try:
            # Send Who-Is broadcast
            self.bacnet.whois()
            
            # Wait for I-Am responses
            time.sleep(timeout)
            
            # Collect discovered devices
            for device_id, device_info in self.bacnet.devices.items():
                try:
                    # Parse device address
                    address_parts = device_info[2].split(':')
                    ip = address_parts[0]
                    port = int(address_parts[1]) if len(address_parts) > 1 else 47808
                    
                    # Try to get device name
                    device_name = "Unknown"
                    vendor_name = "Unknown"
                    
                    try:
                        # Some devices provide name in discovery
                        device_name = device_info[1] if len(device_info) > 1 else "Unknown"
                    except:
                        pass
                    
                    devices.append({
                        'device_id': device_id,
                        'address': ip,
                        'port': port,
                        'name': device_name,
                        'vendor': vendor_name,
                    })
                
                except Exception as err:
                    _LOGGER.warning("Error parsing device %s: %s", device_id, err)
                    continue
        
        except Exception as err:
            _LOGGER.error("Error during Who-Is discovery: %s", err)
        
        return devices
    
    
    async def get_device_name(self) -> Optional[str]:
        """
        Get device name from BACnet device.
        
        Returns:
            Device name or None if not available
        """
        try:
            if not self._connected or not self.device:
                return None
            
            # Get device name property
            name = await asyncio.get_event_loop().run_in_executor(
                None,
                lambda: self.device.properties.name
            )
            
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
        if not self._connected or not self.device:
            _LOGGER.error("Not connected to BACnet device")
            return None
        
        try:
            # Build BACnet address string
            obj_id = f"{object_type}:{object_instance}"
            
            # Read property
            result = await asyncio.get_event_loop().run_in_executor(
                None,
                self.device.read_property,
                obj_id,
                property_name
            )
            
            return result
        
        except Exception as err:
            _LOGGER.error("Read failed for %s:%s.%s: %s", 
                         object_type, object_instance, property_name, err)
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
        if not self._connected or not self.device:
            _LOGGER.error("Not connected to BACnet device")
            return False
        
        try:
            # Build BACnet address string
            obj_id = f"{object_type}:{object_instance}"
            
            # Write property
            await asyncio.get_event_loop().run_in_executor(
                None,
                self.device.write_property,
                obj_id,
                property_name,
                value,
                priority
            )
            
            _LOGGER.debug("Wrote %s to %s:%s.%s", 
                         value, object_type, object_instance, property_name)
            return True
        
        except Exception as err:
            _LOGGER.error("Write failed for %s:%s.%s: %s", 
                         object_type, object_instance, property_name, err)
            return False
    
    
    async def disconnect(self):
        """Disconnect from BACnet device."""
        if self.bacnet:
            try:
                # BAC0 disconnect might be async or sync, handle both
                if asyncio.iscoroutinefunction(self.bacnet.disconnect):
                    await self.bacnet.disconnect()
                else:
                    await asyncio.get_event_loop().run_in_executor(
                        None,
                        self.bacnet.disconnect
                    )
                _LOGGER.info("Disconnected from BACnet device")
            except Exception as err:
                _LOGGER.error("Error disconnecting: %s", err)
            finally:
                self.bacnet = None
                self.device = None
                self._connected = False
    
    
    @property
    def connected(self) -> bool:
        """Return connection status."""
        return self._connected
