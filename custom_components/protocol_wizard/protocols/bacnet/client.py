# protocols/bacnet/client.py
"""BACnet/IP client for Protocol Wizard - Updated for BAC0 2025+."""

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
        self.device = None
        self._connected = False
    
    
    async def connect(self) -> bool:
        """
        Connect to BACnet device.
        
        Returns:
            True if connection successful, False otherwise
        """
        try:
            _LOGGER.info("Connecting to BACnet device %s:%s (ID: %s)", 
                        self.host, self.port, self.device_id)
            
            # Create BAC0 connection
            self.bacnet = await self._create_bacnet_connection()
            
            if not self.bacnet:
                _LOGGER.error("Failed to create BACnet connection")
                return False
            
            # Connect to specific device if device_id provided
            if self.device_id is not None:
                # In BAC0 2025+, devices are registered after discovery
                # First trigger discovery to populate registered_devices
                await self._trigger_discovery_if_needed()
                
                # Look for device in registered_devices
                self.device = self._find_device_by_id(self.device_id)
                
                if not self.device:
                    _LOGGER.error("Device ID %s not found at %s", 
                                self.device_id, self.host)
                    return False
                
                device_name = self._get_device_property(self.device, 'objectName', 'Unknown')
                _LOGGER.info("Connected to BACnet device: %s", device_name)
            
            self._connected = True
            return True
        
        except Exception as err:
            _LOGGER.error("BACnet connection failed: %s", err)
            import traceback
            traceback.print_exc()
            self._connected = False
            return False
    
    
    async def _create_bacnet_connection(self):
        """Create BAC0 connection (async)."""
        try:
            # BAC0 2025+ is async-aware
            bacnet = BAC0.connect(ip=self.host, port=self.port)
            
            # Give it a moment to initialize
            await asyncio.sleep(0.5)
            
            return bacnet
        except Exception as err:
            _LOGGER.error("Failed to create BAC0 connection: %s", err)
            return None
    
    
    async def _trigger_discovery_if_needed(self):
        """Trigger discovery to populate registered_devices."""
        try:
            # Send Who-Is to discover devices
            await asyncio.get_event_loop().run_in_executor(
                None,
                self.bacnet.discover
            )
            
            # Wait for I-Am responses
            await asyncio.sleep(2)
        except Exception as err:
            _LOGGER.warning("Discovery trigger failed: %s", err)
    
    
    def _find_device_by_id(self, device_id: int):
        """Find device in registered_devices by instance ID."""
        try:
            # Try new API: registered_devices (dict or list)
            registered = getattr(self.bacnet, 'registered_devices', None)
            
            if isinstance(registered, dict):
                # Dict: device_id -> device_object
                return registered.get(device_id)
            
            elif hasattr(registered, '__iter__'):
                # List of device objects
                for device in registered:
                    dev_id = self._get_device_property(device, 'device_id', None)
                    if dev_id == device_id:
                        return device
            
            # Try old API: devices
            devices = getattr(self.bacnet, 'devices', {})
            if device_id in devices:
                return devices[device_id]
            
            return None
        
        except Exception as err:
            _LOGGER.error("Error finding device: %s", err)
            return None
    
    
    def _get_device_property(self, device, prop_name: str, default=None):
        """Safely get property from device object."""
        try:
            # Try direct attribute
            if hasattr(device, prop_name):
                return getattr(device, prop_name)
            
            # Try properties dict
            if hasattr(device, 'properties') and hasattr(device.properties, prop_name):
                return getattr(device.properties, prop_name)
            
            # Try dict access
            if isinstance(device, dict):
                return device.get(prop_name, default)
            
            return default
        except Exception:
            return default
    
    
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
            
            # BAC0 2025+ discover() is async, call it directly
            _LOGGER.info("Sending Who-Is broadcast...")
            
            try:
                # Try calling discover directly (it's async in 2025+)
                self.bacnet.discover()
            except RuntimeError as err:
                if "no running event loop" in str(err):
                    _LOGGER.warning("discover() is async but called from sync context")
                    # discover() creates async tasks internally, we can't await it
                    # Just let it run
                    pass
                else:
                    raise
            except AttributeError:
                # Fall back to old API
                try:
                    self.bacnet.whois()
                except AttributeError:
                    _LOGGER.error("Cannot find discover() or whois() method")
                    return []
            
            # Wait for I-Am responses
            _LOGGER.info("Waiting %ds for I-Am responses...", timeout)
            await asyncio.sleep(timeout)
            
            # Collect discovered devices
            devices = self._collect_discovered_devices()
            
            _LOGGER.info("Discovered %d BACnet devices", len(devices))
            return devices
        
        except Exception as err:
            _LOGGER.error("BACnet discovery failed: %s", err)
            import traceback
            traceback.print_exc()
            return []
    
    
    def _collect_discovered_devices(self) -> list[dict]:
        """Collect devices from registered_devices or devices."""
        devices = []
        
        try:
            # Get device list
            device_list = getattr(self.bacnet, 'registered_devices', None)
            if device_list is None:
                device_list = getattr(self.bacnet, 'devices', {})
            
            _LOGGER.info("Processing discovered devices (type: %s, count: %s)", 
                        type(device_list), len(device_list) if hasattr(device_list, '__len__') else '?')
            
            # Handle dict of devices
            if isinstance(device_list, dict):
                for device_id, device_info in device_list.items():
                    try:
                        device_dict = self._parse_device_info(device_id, device_info)
                        if device_dict:
                            devices.append(device_dict)
                    except Exception as err:
                        _LOGGER.warning("Error parsing device %s: %s", device_id, err)
            
            # Handle list/iterable of devices
            elif hasattr(device_list, '__iter__'):
                for device in device_list:
                    try:
                        device_id = self._get_device_property(device, 'device_id', None)
                        if not device_id:
                            # Try objectIdentifier tuple
                            obj_id = self._get_device_property(device, 'objectIdentifier', (None, None))
                            device_id = obj_id[1] if isinstance(obj_id, tuple) and len(obj_id) > 1 else None
                        
                        if device_id:
                            device_dict = self._parse_device_info(device_id, device)
                            if device_dict:
                                devices.append(device_dict)
                    except Exception as err:
                        _LOGGER.warning("Error parsing device: %s", err)
        
        except Exception as err:
            _LOGGER.error("Error collecting devices: %s", err)
            import traceback
            traceback.print_exc()
        
        return devices
    
    
    def _parse_device_info(self, device_id, device_info) -> Optional[dict]:
        """Parse device info into standardized dict."""
        try:
            # Try to extract info from device object
            if hasattr(device_info, '__dict__') or hasattr(device_info, 'address'):
                # It's a device object
                address = str(self._get_device_property(device_info, 'address', '127.0.0.1'))
                address = address.split(':')[0] if ':' in address else address
                
                name = self._get_device_property(device_info, 'objectName', None)
                if not name:
                    name = self._get_device_property(device_info, 'name', 'Unknown')
                
                vendor = self._get_device_property(device_info, 'vendorName', 'Unknown')
                port = 47808
            
            # Handle tuple/list format (old API)
            elif isinstance(device_info, (tuple, list)) and len(device_info) >= 3:
                address_str = str(device_info[2])
                address = address_str.split(':')[0]
                port = int(address_str.split(':')[1]) if ':' in address_str else 47808
                name = device_info[1] if len(device_info) > 1 else "Unknown"
                vendor = "Unknown"
            
            else:
                _LOGGER.warning("Unknown device info format: %s", type(device_info))
                return None
            
            return {
                'device_id': int(device_id),
                'address': address,
                'port': port,
                'name': name,
                'vendor': vendor,
            }
        
        except Exception as err:
            _LOGGER.warning("Error parsing device info: %s", err)
            return None
    
    
    async def get_device_name(self) -> Optional[str]:
        """
        Get device name from BACnet device.
        
        Returns:
            Device name or None if not available
        """
        try:
            if not self._connected or not self.device:
                return None
            
            # Try to get name from device object
            name = self._get_device_property(self.device, 'objectName', None)
            if not name:
                name = self._get_device_property(self.device, 'name', None)
            
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
            _LOGGER.error("Not connected to BACnet device")
            return None
        
        try:
            # BAC0 2025+ uses bacnet.read() with full address string
            # Format: "device_address object_type object_instance property_name"
            
            device_address = f"{self.host}:{self.port}"
            
            # If we have device instance, include it
            if self.device_id:
                device_address = f"{self.host}:{self.port}@{self.device_id}"
            
            read_string = f"{device_address} {object_type} {object_instance} {property_name}"
            
            _LOGGER.debug("Reading: %s", read_string)
            
            # Read property using BAC0's read method
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
            _LOGGER.error("Not connected to BACnet device")
            return False
        
        try:
            # BAC0 2025+ uses bacnet.write() with full address string
            # Format: "device_address object_type object_instance property_name value - priority"
            
            device_address = f"{self.host}:{self.port}"
            
            # If we have device instance, include it
            if self.device_id:
                device_address = f"{self.host}:{self.port}@{self.device_id}"
            
            write_string = f"{device_address} {object_type} {object_instance} {property_name} {value} - {priority}"
            
            _LOGGER.debug("Writing: %s", write_string)
            
            # Write property using BAC0's write method
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
        """Disconnect from BACnet device."""
        if self.bacnet:
            try:
                # BAC0 disconnect might be async or sync
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
