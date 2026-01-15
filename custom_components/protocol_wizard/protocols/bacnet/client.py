# protocols/bacnet/client.py
"""BACnet/IP client for Protocol Wizard using bacpypes3 - proper initialization."""

import logging
import asyncio
from typing import Any, Optional
from homeassistant.core import HomeAssistant
#import sys

_LOGGER = logging.getLogger(__name__)

try:
#    from bacpypes3.settings import settings
    from bacpypes3.app import Application
#    from bacpypes3.local.device import DeviceObject
    from bacpypes3.primitivedata import ObjectIdentifier
    from bacpypes3.basetypes import PropertyIdentifier
    from bacpypes3.pdu import Address
#    from bacpypes3.argparse import SimpleArgumentParser, create_log_handler
    HAS_BACPYPES3 = True
except ImportError:
    HAS_BACPYPES3 = False
    _LOGGER.error("bacpypes3 library not installed")


# Global application instance
_global_app = None
_app_initialized = False


async def _initialize_bacpypes3(hass: HomeAssistant):
    """Initialize bacpypes3 properly using from_args pattern."""
    global _global_app, _app_initialized
    
    if _app_initialized:
        return _global_app
    
    try:
        from argparse import Namespace
        import random
        from homeassistant.components import network


        try:
            source_ip = await network.async_get_source_ip(hass)
            return source_ip
        except Exception:
            return "0.0.0.0" # Fallback
        # Create a proper Namespace with required arguments
        # CRITICAL: Specify the correct network address to use
        # Use the actual HA IP address on the correct subnet
        args = Namespace(
            # Required
            name="Protocol Wizard Client",
            instance=random.randint(100000, 999999),
            vendoridentifier=999,
            
            # Network - SPECIFY THE CORRECT INTERFACE/ADDRESS
            # This should be HA's IP on the correct subnet where devices are
            # /22 means 192.168.0.0-192.168.3.255 (netmask 255.255.252.0)
            address=f"{local_ip}/24",
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
        
        _LOGGER.info("Calling Application.from_args() with instance=%s, address=%s", 
                    args.instance, args.address)
        
        # from_args is synchronous, not async!
        _global_app = Application.from_args(args)
        
        _LOGGER.info("BACnet application initialized successfully!")
        _LOGGER.info("Has elementService: %s", hasattr(_global_app, 'elementService'))
        
        # Log network configuration
        if hasattr(_global_app, 'link_layers'):
            _LOGGER.info("Link layers: %s", _global_app.link_layers)
            for port_id, link_layer in _global_app.link_layers.items():
                if hasattr(link_layer, 'address'):
                    _LOGGER.info("  Link layer %s address: %s", port_id, link_layer.address)
                if hasattr(link_layer, 'broadcast'):
                    _LOGGER.info("  Link layer %s broadcast: %s", port_id, link_layer.broadcast)
        
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
        hass: HomeAssistant
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
        self.hass = hass
    
    
    async def connect(self) -> bool:
        """Connect to BACnet network."""
        try:
            _LOGGER.info("Connecting to BACnet network")
            
            # Initialize bacpypes3 using from_args
            await _initialize_bacpypes3(self.hass)
            
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
            
            # Log app info
            _LOGGER.info("App local device ID: %s", 
                        getattr(self.app, 'objectIdentifier', 'unknown'))
            
            # Check if we have link layers
            if hasattr(self.app, 'link_layers'):
                _LOGGER.info("Link layers: %s", self.app.link_layers)
            
            # Send Who-Is
            _LOGGER.info("Sending Who-Is broadcast...")
            _LOGGER.info("Target host: %s, device_id: %s", self.host, self.device_id)
            
            try:
                # Check cache before Who-Is
                if hasattr(self.app, 'device_info_cache'):
                    cache_before = len(self.app.device_info_cache.instance_cache) if hasattr(self.app.device_info_cache, 'instance_cache') else 0
                    _LOGGER.info("Cache has %d devices BEFORE Who-Is", cache_before)
                
                # Log what address we're using
                if hasattr(self.app, 'link_layers'):
                    for port_id, link_layer in self.app.link_layers.items():
                        _LOGGER.info("Link layer %s:", port_id)
                        if hasattr(link_layer, 'address'):
                            _LOGGER.info("  Local address: %s", link_layer.address)
                        if hasattr(link_layer, 'broadcast'):
                            _LOGGER.info("  Broadcast address: %s", link_layer.broadcast)
                        if hasattr(link_layer, 'addrBroadcastTuple'):
                            _LOGGER.info("  Broadcast tuple: %s", link_layer.addrBroadcastTuple)
                
                # If we're looking for a specific device (not 0.0.0.0), try directed Who-Is first
                if self.host != "0.0.0.0" and self.device_id:
                    _LOGGER.info("Trying directed Who-Is to %s:%s for device %s", 
                                self.host, self.port, self.device_id)
                    # Send Who-Is directly to the device's IP
                    target_address = Address(f"{self.host}:{self.port}")
                    _LOGGER.info("Target address object: %s", target_address)
                    
                    await self.app.who_is(
                        device_instance_range_low_limit=self.device_id,
                        device_instance_range_high_limit=self.device_id,
                        address=target_address
                    )
                    _LOGGER.info("Directed Who-Is sent to %s", target_address)
                else:
                    # Broadcast Who-Is
                    _LOGGER.info("Sending broadcast Who-Is (no specific target)")
                    await self.app.who_is()
                    _LOGGER.info("Broadcast Who-Is sent")
                
                _LOGGER.info("Who-Is sent successfully")
                
                # Give a moment for immediate responses
                await asyncio.sleep(0.5)
                
                # Check cache immediately after
                if hasattr(self.app, 'device_info_cache'):
                    cache_after = len(self.app.device_info_cache.instance_cache) if hasattr(self.app.device_info_cache, 'instance_cache') else 0
                    _LOGGER.info("Cache has %d devices immediately after Who-Is", cache_after)
                
            except Exception as err:
                _LOGGER.error("Who-Is failed: %s", err)
                import traceback
                traceback.print_exc()
                return []
            
            # Wait for I-Am responses with periodic checks
            _LOGGER.info("Waiting %ds for I-Am responses...", timeout)
            
            for i in range(timeout):
                await asyncio.sleep(1)
                
                if hasattr(self.app, 'device_info_cache') and hasattr(self.app.device_info_cache, 'instance_cache'):
                    cache_count = len(self.app.device_info_cache.instance_cache)
                    if cache_count > 0:
                        _LOGGER.info("After %ds: %d devices in cache", i+1, cache_count)
            
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
            _LOGGER.info("=" * 60)
            _LOGGER.info("COLLECTING DISCOVERED DEVICES")
            _LOGGER.info("=" * 60)
            
            # Check what attributes the app has
            _LOGGER.info("App type: %s", type(self.app))
            _LOGGER.info("App class: %s", self.app.__class__.__name__)
            
            # List all attributes
            all_attrs = dir(self.app)
            cache_attrs = [a for a in all_attrs if 'cache' in a.lower()]
            device_attrs = [a for a in all_attrs if 'device' in a.lower()]
            
            _LOGGER.info("Cache-related attributes (%d): %s", len(cache_attrs), cache_attrs)
            _LOGGER.info("Device-related attributes (%d): %s", len(device_attrs), device_attrs)
            
            # Check for device_info_cache
            has_device_info_cache = hasattr(self.app, 'device_info_cache')
            _LOGGER.info("Has device_info_cache: %s", has_device_info_cache)
            
            if has_device_info_cache:
                cache = self.app.device_info_cache
                _LOGGER.info("device_info_cache type: %s", type(cache))
                _LOGGER.info("device_info_cache class: %s", cache.__class__.__name__)
                _LOGGER.info("device_info_cache attributes: %s", [a for a in dir(cache) if not a.startswith('_')])
                
                # Check for instance_cache
                has_instance_cache = hasattr(cache, 'instance_cache')
                _LOGGER.info("Has instance_cache: %s", has_instance_cache)
                
                if has_instance_cache:
                    instance_cache = cache.instance_cache
                    _LOGGER.info("instance_cache type: %s", type(instance_cache))
                    _LOGGER.info("instance_cache length: %d", len(instance_cache))
                    
                    if len(instance_cache) == 0:
                        _LOGGER.warning("!!! instance_cache is EMPTY - no devices found !!!")
                        _LOGGER.info("This means either:")
                        _LOGGER.info("  1. No I-Am responses were received")
                        _LOGGER.info("  2. I-Am responses went to a different cache")
                        _LOGGER.info("  3. Network/firewall is blocking responses")
                    else:
                        _LOGGER.info("!!! instance_cache has %d entries !!!", len(instance_cache))
                    
                    # Log all entries in detail
                    for idx, (device_id, device_info) in enumerate(instance_cache.items()):
                        _LOGGER.info("-" * 60)
                        _LOGGER.info("Device %d/%d:", idx+1, len(instance_cache))
                        _LOGGER.info("  device_id: %s (type: %s)", device_id, type(device_id))
                        _LOGGER.info("  device_info type: %s", type(device_info))
                        _LOGGER.info("  device_info class: %s", device_info.__class__.__name__)
                        _LOGGER.info("  device_info attributes: %s", [a for a in dir(device_info) if not a.startswith('_')])
                        
                        try:
                            # Try to extract all possible attributes
                            for attr_name in ['device_address', 'address', 'device_name', 'name', 
                                            'objectName', 'vendor_name', 'vendorName', 'vendorIdentifier']:
                                if hasattr(device_info, attr_name):
                                    attr_value = getattr(device_info, attr_name)
                                    _LOGGER.info("    %s: %s", attr_name, attr_value)
                            
                            # Extract device information for our list
                            address = getattr(device_info, 'device_address', None)
                            _LOGGER.info("  Extracted address: %s", address)
                            
                            if address:
                                addr_str = str(address)
                                if ':' in addr_str:
                                    ip, port_str = addr_str.rsplit(':', 1)
                                    port = int(port_str)
                                else:
                                    ip = addr_str
                                    port = 47808
                            else:
                                ip = self.host if self.host != "0.0.0.0" else "127.0.0.1"
                                port = self.port
                            
                            name = getattr(device_info, 'device_name', None)
                            if not name:
                                name = getattr(device_info, 'objectName', f"Device {device_id}")
                            
                            vendor = getattr(device_info, 'vendor_name', 'Unknown')
                            
                            device_dict = {
                                'device_id': int(device_id),
                                'address': ip,
                                'port': port,
                                'name': name,
                                'vendor': vendor,
                            }
                            
                            devices.append(device_dict)
                            
                            _LOGGER.info("  ✓ Added device: %s (%s) at %s:%s", 
                                       name, device_id, ip, port)
                        
                        except Exception as err:
                            _LOGGER.error("  ✗ Error parsing device %s: %s", device_id, err)
                            import traceback
                            traceback.print_exc()
                else:
                    _LOGGER.error("device_info_cache exists but has NO instance_cache!")
                    # Try to access cache directly as dict
                    if isinstance(cache, dict):
                        _LOGGER.info("Cache is a dict with %d items", len(cache))
                        for key, value in cache.items():
                            _LOGGER.info("  Cache[%s] = %s (type: %s)", key, value, type(value))
            else:
                _LOGGER.error("App has NO device_info_cache attribute!")
                _LOGGER.info("Trying alternative cache locations...")
                
                # Try other possible cache locations
                for attr_name in ['i_am_cache', 'iAmCache', 'devices', '_devices']:
                    if hasattr(self.app, attr_name):
                        alt_cache = getattr(self.app, attr_name)
                        _LOGGER.info("Found %s: %s (type: %s)", attr_name, alt_cache, type(alt_cache))
                        if isinstance(alt_cache, dict):
                            _LOGGER.info("  Has %d items", len(alt_cache))
        
        except Exception as err:
            _LOGGER.error("CRITICAL ERROR in _collect_discovered_devices: %s", err)
            import traceback
            traceback.print_exc()
        
        _LOGGER.info("=" * 60)
        _LOGGER.info("COLLECTION COMPLETE: Found %d devices", len(devices))
        _LOGGER.info("=" * 60)
        
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
            
            # Add timeout to prevent hanging forever
            try:
                result = await asyncio.wait_for(
                    self.app.read_property(
                        address=device_address,
                        objid=object_id,
                        prop=prop_id
                    ),
                    timeout=5.0  # 5 second timeout
                )
                
                _LOGGER.debug("Read result: %s (type: %s)", result, type(result))
                return result
                
            except asyncio.TimeoutError:
                _LOGGER.error("Read timed out after 5 seconds - no response from %s", device_address)
                _LOGGER.error("This means:")
                _LOGGER.error("  1. Device is not responding")
                _LOGGER.error("  2. Network/firewall is blocking BACnet traffic")
                _LOGGER.error("  3. Device is on different subnet")
                return None
        
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
            
            _LOGGER.info("Wrote %s to %s:%s.%s. Result:%s", 
                         value, object_type, object_instance, property_name,result)
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
    
    @property
    def is_connected(self) -> bool:
        """Alias for connected property (for compatibility)."""
        return self._connected
