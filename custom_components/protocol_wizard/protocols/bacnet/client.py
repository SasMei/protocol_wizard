# protocols/bacnet/client.py
"""BACnet client wrapper."""
import logging
from BAC0 import connect

_LOGGER = logging.getLogger(__name__)

class BACnetClient:
    """BACnet client for Protocol Wizard."""
    
    def __init__(self, host: str, device_id: int, port: int=47808, network_number: int=0):
        """Initialize BACnet client."""
        self.host = host
        self.device_id = device_id
        self.port = port
        self.network_number = network_number
        self.bacnet = None
        
    async def connect(self) -> bool:
        """Connect to BACnet device."""
        try:
            # BAC0 is blocking, so use executor
            self.bacnet = await asyncio.get_event_loop().run_in_executor(
                None, connect, self.host
            )
            _LOGGER.info("Connected to BACnet device %s", self.host)
            return True
        except Exception as err:
            _LOGGER.error("BACnet connection failed: %s", err)
            return False
    
    async def read_property(
        self, 
        object_type: str, 
        object_instance: int, 
        property_name: str
    ) -> any:
        """Read BACnet property."""
        try:
            address = f"{self.host}:{self.device_id}"
            result = await asyncio.get_event_loop().run_in_executor(
                None,
                self.bacnet.read,
                f"{address} {object_type} {object_instance} {property_name}"
            )
            return result
        except Exception as err:
            _LOGGER.error("Read failed: %s", err)
            return None
    
    async def write_property(
        self,
        object_type: str,
        object_instance: int,
        property_name: str,
        value: any
    ) -> bool:
        """Write BACnet property."""
        try:
            address = f"{self.host}:{self.device_id}"
            await asyncio.get_event_loop().run_in_executor(
                None,
                self.bacnet.write,
                f"{address} {object_type} {object_instance} {property_name} {value}"
            )
            return True
        except Exception as err:
            _LOGGER.error("Write failed: %s", err)
            return False
    
    async def disconnect(self):
        """Disconnect from BACnet device."""
        if self.bacnet:
            await asyncio.get_event_loop().run_in_executor(
                None, self.bacnet.disconnect
            )
