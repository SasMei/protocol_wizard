# protocols/bacnet/client.py
"""BACnet/IP client for Protocol Wizard using bacpypes3 - proper initialization."""

import logging
import asyncio
from typing import Any, Optional
import sys

_LOGGER = logging.getLogger(__name__)

try:
    from bacpypes3.settings import settings
    from bacpypes3.app import Application
    from bacpypes3.local.device import DeviceObject
    from bacpypes3.primitivedata import ObjectIdentifier
    from bacpypes3.basetypes import PropertyIdentifier
    from bacpypes3.pdu import Address
    from bacpypes3.argparse import SimpleArgumentParser, create_log_handler
    HAS_BACPYPES3 = True
except ImportError:
    HAS_BACPYPES3 = False
    _LOGGER.error("bacpypes3 library not installed")


# Global application instance
_global_app = None
_app_initialized = False


async def _initialize_bacpypes3():
    """Initialize bacpypes3 properly using from_args pattern."""
    global _global_app, _app_initialized
    
    if _app_initialized:
        return _global_app
    
    try:
        from argparse import Namespace
        import random
        
        _LOGGER.info("Initializing bacpypes3 Application")
        
        # Create a proper Namespace with required arguments
        # Based on SimpleArgumentParser defaults
        args = Namespace(
            # Required
            name="Protocol Wizard Client",
            instance=random.randint(100000, 999999),
            vendoridentifier=999,
            
            # Network - use None to auto-detect, or specific interface
            # Using None lets bacpypes3 auto-detect the network interface
            address=None,  # Let bacpypes3 auto-detect
            network=0,
            
            # Optional
            foreign=None,
            ttl=30,
            bbmd=None,
            
            # Logging (set to None/False to avoid log handler issues)
            loggers=None,
            debug=None,
            color=None,
            route_aware=None,
        )
        
        _LOGGER.info("Calling Application.from_args() with instance=%s", args.instance)
        
        # from_args is synchronous, not async!
        _global_app = Application.from_args(args)
        
        _LOGGER.info("BACnet application initialized successfully!")
        _LOGGER.info("Has elementService: %s", hasattr(_global_app, 'elementService'))
        _app_initialized = True
        
        return _global_app
        
    except Exception as err:
        _LOGGER.error("Failed to initialize bacpypes3: %s", err)
        import traceback
        traceback.print_exc()
    
    return None


class BACnetClientApp(Application):
    """BACnet client application following bacpypes3 patterns."""
    
    def __init__(self):
        """Initialize with proper setup."""
        # Call parent init
        Application.__init__(self)
        
        _LOGGER.info("BACnet client app initialized")


class BACnetClient:
    """BACnet/IP client using bacpypes3."""
    
    def __init__(
        self, 
        host: str, 
        device_id: Optional[int] = None,
        port: int = 47808,
        network_number: Optional[int] = None
    ):
        """Initialize BACnet client."""
        if not HAS_BACPYPES3:
            raise ImportError("bacpypes3 library is required for BACnet support")
        
        self.host = host
        self.device_id = device_id
        self.port = port
        self.network_number = network_number
        self.app: Optional[Application] = None
        self._connected = False
    
    
    async def connect(self) -> bool:
        """Connect to BACnet network."""
        try:
            _LOGGER.info("Connecting to BACnet network")
            
            # Initialize bacpypes3 using from_args
            await _initialize_bacpypes3()
            
            # Get global app
            global _global_app
            
            if _global_app is None:
                _LOGGER.error("Failed to create BACnet application")
                return False
            
            self.app = _global_app
            
            # Check if initialized properly
            has_element_service = hasattr(self.app, 'elementService')
            _LOGGER.info("App ready (has elementService: %s)", has_element_service)
            
            # List available methods for debugging
            methods = [m for m in dir(self.app) if not m.startswith('_')]
            _LOGGER.info("App has %d methods including: who_is=%s, read_property=%s, write_property=%s", 
                        len(methods),
                        'who_is' in methods,
                        'read_property' in methods, 
                        'write_property' in methods)
            
            self._connected = True
            return True
        
        except Exception as err:
            _LOGGER.error("BACnet connection failed: %s", err)
            import traceback
            traceback.print_exc()
            return False
    
    
    async def discover_devices(self, timeout: int = 10) -> list[dict]:
        """Discover BACnet devices using Who-Is."""
        try:
            _LOGGER.info("Starting BACnet device discovery (timeout: %ds)", timeout)
            
            if not self.app:
                await self.connect()
            
            if not self.app:
                _LOGGER.error("Cannot discover without BACnet connection")
                return []
            
            # Send Who-Is
            _LOGGER.info("Sending Who-Is broadcast...")
            
            try:
                await self.app.who_is()
                _LOGGER.info("Who-Is sent successfully")
            except Exception as err:
                _LOGGER.error("Who-Is failed: %s", err)
                import traceback
                traceback.print_exc()
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
        """Collect devices from app's device info cache."""
        devices = []
        
        try:
            # Check for device_info_cache (standard bacpypes3 location)
            if hasattr(self.app, 'device_info_cache'):
                cache = self.app.device_info_cache
                _LOGGER.info("Found device_info_cache: %s", cache)
                
                if hasattr(cache, 'instance_cache'):
                    _LOGGER.info("Found instance_cache with %d devices", 
                               len(cache.instance_cache))
                    
                    for device_id, device_info in cache.instance_cache.items():
                        try:
                            # Extract device information
                            address = getattr(device_info, 'device_address', None)
                            if address:
                                addr_str = str(address)
                                if ':' in addr_str:
                                    ip, port_str = addr_str.rsplit(':', 1)
                                    port = int(port_str)
                                else:
                                    ip = addr_str
                                    port = 47808
                            else:
                                ip = self.host
                                port = self.port
                            
                            name = getattr(device_info, 'device_name', f"Device {device_id}")
                            vendor = getattr(device_info, 'vendor_name', 'Unknown')
                            
                            devices.append({
                                'device_id': int(device_id),
                                'address': ip,
                                'port': port,
                                'name': name,
                                'vendor': vendor,
                            })
                            
                            _LOGGER.info("Found device: %s (%s) at %s:%s", 
                                       name, device_id, ip, port)
                        
                        except Exception as err:
                            _LOGGER.warning("Error parsing device %s: %s", device_id, err)
            else:
                _LOGGER.warning("No device_info_cache found")
                # List cache-related attributes
                attrs = [a for a in dir(self.app) if 'cache' in a.lower()]
                _LOGGER.info("Cache attributes: %s", attrs)
        
        except Exception as err:
            _LOGGER.error("Error collecting devices: %s", err)
            import traceback
            traceback.print_exc()
        
        return devices
    
    
    async def get_device_name(self) -> Optional[str]:
        """Get device name."""
        try:
            if not self._connected or not self.device_id:
                return None
            
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
        """Read BACnet property."""
        if not self._connected or not self.app:
            _LOGGER.error("Not connected to BACnet network")
            return None
        
        try:
            object_id = ObjectIdentifier(f"{object_type},{object_instance}")
            device_address = Address(f"{self.host}:{self.port}")
            prop_id = PropertyIdentifier(property_name)
            
            _LOGGER.debug("Reading %s from %s at %s", 
                         property_name, object_id, device_address)
            
            result = await self.app.read_property(
                address=device_address,
                objid=object_id,
                prop=prop_id
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
        """Write BACnet property."""
        if not self._connected or not self.app:
            _LOGGER.error("Not connected to BACnet network")
            return False
        
        try:
            object_id = ObjectIdentifier(f"{object_type},{object_instance}")
            device_address = Address(f"{self.host}:{self.port}")
            prop_id = PropertyIdentifier(property_name)
            
            _LOGGER.debug("Writing %s to %s.%s at %s (priority %d)", 
                         value, object_id, property_name, device_address, priority)
            
            result = await self.app.write_property(
                address=device_address,
                objid=object_id,
                prop=prop_id,
                value=value,
                priority=priority
            )
            
            _LOGGER.info("Wrote %s to %s:%s.%s", 
                         value, object_type, object_instance, property_name)
            return True
        
        except Exception as err:
            _LOGGER.error("Write failed for %s:%s.%s: %s", 
                         object_type, object_instance, property_name, err)
            import traceback
            traceback.print_exc()
            return False
    
    
    async def disconnect(self):
        """Disconnect from BACnet network."""
        # Don't disconnect global app, just clear reference
        self.app = None
        self._connected = False
    
    
    @property
    def connected(self) -> bool:
        """Return connection status."""
        return self._connected
