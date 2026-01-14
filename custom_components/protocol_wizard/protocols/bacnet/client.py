# protocols/bacnet/client.py
"""BACnet/IP client for Protocol Wizard using bacpypes3."""

import logging
import asyncio
from typing import Any, Optional

_LOGGER = logging.getLogger(__name__)

try:
    from bacpypes3.apdu import ErrorRejectAbortNack
    from bacpypes3.app import Application
    from bacpypes3.pdu import Address
    from bacpypes3.primitivedata import ObjectIdentifier
    from bacpypes3.basetypes import PropertyIdentifier
    HAS_BACPYPES3 = True
except ImportError:
    HAS_BACPYPES3 = False
    _LOGGER.error("bacpypes3 library not installed")


class BACnetClient:
    """BACnet/IP client wrapper for bacpypes3."""
    
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
        if not HAS_BACPYPES3:
            raise ImportError("bacpypes3 library is required for BACnet support")
        
        self.host = host
        self.device_id = device_id
        self.port = port
        self.network_number = network_number
        self.app: Optional[Application] = None
        self._connected = False
        self._discovered_devices = {}
    
    
    async def connect(self) -> bool:
        """
        Connect to BACnet network.
        
        Returns:
            True if connection successful, False otherwise
        """
        try:
            _LOGGER.info("Connecting to BACnet network on %s:%s", self.host, self.port)
            
            # Import additional classes needed for setup
            from bacpypes3.local.device import DeviceObject
            from bacpypes3.local.networkport import NetworkPortObject
            
            # Create device object for this client
            # Use a random device instance for the client
            import random
            client_device_id = random.randint(100000, 999999)
            
            # Create BACnet application (like your simulator does)
            self.app = Application()
            
            # Add device object
            device = DeviceObject(
                objectIdentifier=("device", client_device_id),
                objectName="Protocol Wizard Client",
                vendorName="Protocol Wizard",
                vendorIdentifier=999,
            )
            self.app.add_object(device)
            
            # Add network port
            net_port = NetworkPortObject(
                objectIdentifier=("networkPort", 1),
                objectName="BACnet/IP",
                networkType="ipv4",
                ipAddress=self.host if self.host != "0.0.0.0" else "127.0.0.1",
                ipSubnetMask="255.255.255.0",
                ipDefaultGateway="0.0.0.0",
                bacnetIPUDPPort=self.port,
            )
            self.app.add_object(net_port)
            
            _LOGGER.info("BACnet application created with device ID %s", client_device_id)
            self._connected = True
            return True
        
        except Exception as err:
            _LOGGER.error("BACnet connection failed: %s", err)
            import traceback
            traceback.print_exc()
            self._connected = False
            return False
    
    
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
            
            # Ensure connection
            if not self.app:
                await self.connect()
            
            if not self.app:
                _LOGGER.error("Cannot discover without BACnet connection")
                return []
            
            # Clear previous discoveries
            self._discovered_devices = {}
            
            # Send Who-Is broadcast
            _LOGGER.info("Sending Who-Is broadcast...")
            
            # bacpypes3 Who-Is: send to broadcast address
            await self.app.who_is()
            
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
        """Collect devices from discovery results."""
        devices = []
        
        try:
            # In bacpypes3, discovered devices are in app's device info cache
            if hasattr(self.app, 'device_info_cache'):
                device_cache = self.app.device_info_cache.instance_cache
                
                _LOGGER.info("Found %d devices in cache", len(device_cache))
                
                for device_id, device_info in device_cache.items():
                    try:
                        # Extract device information
                        address = str(device_info.device_address)
                        if ':' in address:
                            ip, port_str = address.rsplit(':', 1)
                            port = int(port_str)
                        else:
                            ip = address
                            port = 47808
                        
                        # Get device name
                        name = getattr(device_info, 'device_name', f"Device {device_id}")
                        
                        # Get vendor
                        vendor = getattr(device_info, 'vendor_name', 'Unknown')
                        
                        devices.append({
                            'device_id': device_id,
                            'address': ip,
                            'port': port,
                            'name': name,
                            'vendor': vendor,
                        })
                        
                        _LOGGER.info("Found device: %s (%s) at %s", name, device_id, ip)
                    
                    except Exception as err:
                        _LOGGER.warning("Error parsing device %s: %s", device_id, err)
            else:
                _LOGGER.warning("No device_info_cache found in app")
        
        except Exception as err:
            _LOGGER.error("Error collecting devices: %s", err)
            import traceback
            traceback.print_exc()
        
        return devices
    
    
    async def get_device_name(self) -> Optional[str]:
        """
        Get device name from BACnet device.
        
        Returns:
            Device name or None if not available
        """
        try:
            if not self._connected or not self.device_id:
                return None
            
            # Read device object name
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
        if not self._connected or not self.app:
            _LOGGER.error("Not connected to BACnet network")
            return None
        
        try:
            # Create object identifier
            object_id = ObjectIdentifier(f"{object_type},{object_instance}")
            
            # Create device address
            device_address = Address(f"{self.host}:{self.port}")
            
            # Create property identifier
            prop_id = PropertyIdentifier(property_name)
            
            _LOGGER.debug("Reading %s from %s at %s", 
                         property_name, object_id, device_address)
            
            # Read property
            result = await self.app.read_property(
                address=device_address,
                objid=object_id,
                prop=prop_id
            )
            
            # Handle ErrorRejectAbortNack
            if isinstance(result, ErrorRejectAbortNack):
                _LOGGER.error("Read error: %s", result)
                return None
            
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
        if not self._connected or not self.app:
            _LOGGER.error("Not connected to BACnet network")
            return False
        
        try:
            # Create object identifier
            object_id = ObjectIdentifier(f"{object_type},{object_instance}")
            
            # Create device address
            device_address = Address(f"{self.host}:{self.port}")
            
            # Create property identifier
            prop_id = PropertyIdentifier(property_name)
            
            _LOGGER.debug("Writing %s to %s.%s at %s (priority %d)", 
                         value, object_id, property_name, device_address, priority)
            
            # Write property
            result = await self.app.write_property(
                address=device_address,
                objid=object_id,
                prop=prop_id,
                value=value,
                priority=priority
            )
            
            # Handle ErrorRejectAbortNack
            if isinstance(result, ErrorRejectAbortNack):
                _LOGGER.error("Write error: %s", result)
                return False
            
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
        if self.app:
            try:
                # bacpypes3 cleanup
                await self.app.close()
                _LOGGER.info("Disconnected from BACnet network")
            except Exception as err:
                _LOGGER.error("Error disconnecting: %s", err)
            finally:
                self.app = None
                self._connected = False
    
    
    @property
    def connected(self) -> bool:
        """Return connection status."""
        return self._connected
