# protocols/bacnet/coordinator.py
"""BACnet coordinator for Protocol Wizard."""
from ...protocols.base import BaseProtocolCoordinator
from .const import CONF_REGISTERS

class BACnetCoordinator(BaseProtocolCoordinator):
    """BACnet data coordinator."""
    
    async def _async_update_data(self):
        """Fetch data from BACnet device."""
        if not await self._async_connect():
            return {}
        
        entities = self.my_config_entry.options.get(CONF_REGISTERS, [])
        if not entities:
            return {}
        
        new_data = {}
        
        async with self._lock:
            for entity in entities:
                # Parse BACnet address: "analogInput:0:presentValue"
                address = entity["address"]
                obj_type, obj_instance, prop_name = address.split(":")
                
                result = await self.client.read_property(
                    obj_type, 
                    int(obj_instance), 
                    prop_name
                )
                
                if result is not None:
                    key = self._entity_key(entity["name"])
                    decoded = self._decode_value(result, entity)
                    formatted = self._format_value(decoded, entity)
                    new_data[key] = formatted
        
        return new_data
