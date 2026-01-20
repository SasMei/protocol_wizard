#------------------------------------------
#-- base init.py protocol wizard - CORRECTED HUB/DEVICE LOGIC
#------------------------------------------
"""The Protocol Wizard integration."""
import shutil
import logging
import os

from homeassistant.helpers import device_registry as dr, entity_registry as er
from homeassistant.config_entries import ConfigEntry
from homeassistant.const import Platform
from homeassistant.core import HomeAssistant, ServiceCall
from pymodbus.client import AsyncModbusSerialClient, AsyncModbusTcpClient, AsyncModbusUdpClient
from homeassistant.exceptions import HomeAssistantError
from homeassistant.helpers.service import SupportsResponse
from datetime import timedelta
# Import protocol registry and plugins
from .protocols import ProtocolRegistry
from .protocols.modbus import ModbusClient
from .protocols.snmp import SNMPClient
from .protocols.mqtt import MQTTClient
from .protocols.bacnet.client import BACnetClient
from .template_utils import ensure_user_template_dirs, load_template

from .const import (
    CONF_BAUDRATE,
    CONF_BYTESIZE,
    CONF_CONNECTION_TYPE,
    CONF_HOST,
    CONF_PARITY,
    CONF_PORT,
    CONF_SERIAL_PORT,
    CONF_SLAVE_ID,
    CONF_STOPBITS,
    CONF_UPDATE_INTERVAL,
    CONF_NAME,
    CONNECTION_TYPE_SERIAL,
    CONNECTION_TYPE_IP,
    CONNECTION_TYPE_UDP,
    CONNECTION_TYPE_TCP,
    DEFAULT_BAUDRATE,
    DEFAULT_BYTESIZE,
    DEFAULT_PARITY,
    DEFAULT_STOPBITS,
    DOMAIN,
    CONF_PROTOCOL_MODBUS,
    CONF_PROTOCOL_SNMP,
    CONF_PROTOCOL_MQTT,
    CONF_PROTOCOL_BACNET,
    CONF_PROTOCOL,
    CONF_TEMPLATE,
    CONF_TEMPLATE_APPLIED,
    CONF_ENTITIES,
    CONF_REGISTERS,
    CONF_IS_HUB,
    CONF_HUB_ID,
    HUB_CLIENTS,
)


_LOGGER = logging.getLogger(__name__)

PLATFORMS = [Platform.SENSOR, Platform.NUMBER, Platform.SELECT, Platform.SWITCH]

async def async_install_frontend_resource(hass: HomeAssistant):
    """Ensure the frontend JS file is copied to the www/community folder."""
    
    def install():
        source_path = hass.config.path("custom_components", DOMAIN, "frontend", "protocol_wizard.js")
        target_dir = hass.config.path("www", "community", DOMAIN)
        target_path = os.path.join(target_dir, "protocol_wizard.js")

        try:
            if not os.path.exists(target_dir):
                _LOGGER.debug("Creating directory: %s", target_dir)
                os.makedirs(target_dir, exist_ok=True)

            if os.path.exists(source_path):
                shutil.copy2(source_path, target_path)
                _LOGGER.info("Updated frontend resource: %s", target_path)
            else:
                _LOGGER.warning("Frontend source file missing at %s", source_path)
                
        except Exception as err:
            _LOGGER.error("Failed to install frontend resource: %s", err)

    await hass.async_add_executor_job(install)

async def async_register_card(hass: HomeAssistant, entry: ConfigEntry):
    """Register the custom card as a Lovelace resource."""
    lovelace_data = hass.data.get("lovelace")
    if not lovelace_data:
        _LOGGER.debug("Unable to get lovelace data")
        return

    resources = lovelace_data.resources
    if not resources:
        _LOGGER.debug("Unable to get resources")
        return

    if not resources.loaded:
        await resources.async_load()

    card_url = f"/hacsfiles/{DOMAIN}/{DOMAIN}.js"

    for item in resources.async_items():
        if item["url"] == card_url:
            _LOGGER.debug("Card already registered: %s", card_url)
            return

    await resources.async_create_item({
        "res_type": "module",
        "url": card_url,
    })
    _LOGGER.debug("Card registered: %s", card_url)

async def async_setup_entry(hass: HomeAssistant, entry: ConfigEntry) -> bool:
    """Set up Protocol Wizard from a config entry."""

    hass.data.setdefault(DOMAIN, {})
    hass.data[DOMAIN].setdefault("connections", {})
    hass.data[DOMAIN].setdefault("coordinators", {})
    hass.data[DOMAIN].setdefault(HUB_CLIENTS, {})  # NEW: Hub client registry

    config = entry.data
    ensure_user_template_dirs(hass)
    
    # Determine protocol
    protocol_name = config.get(CONF_PROTOCOL)
    if protocol_name is None:
        connection_type = config.get(CONF_CONNECTION_TYPE)
        if connection_type in (CONNECTION_TYPE_SERIAL, CONNECTION_TYPE_IP):
            protocol_name = CONF_PROTOCOL_MODBUS
        else:
            protocol_name = CONF_PROTOCOL_MODBUS
    
    # CORRECTED: Check if this is a hub or device
    is_hub = entry.data.get(CONF_IS_HUB, False)
    
    # Handle Modbus Hub differently
    if protocol_name == CONF_PROTOCOL_MODBUS and is_hub:
        return await _setup_modbus_hub(hass, entry, config)
    
    # Get protocol-specific coordinator class
    CoordinatorClass = ProtocolRegistry.get_coordinator_class(protocol_name)
    if not CoordinatorClass:
        _LOGGER.error("Unknown protocol: %s", protocol_name)
        return False
    
    # Create protocol-specific client
    try:
        if protocol_name == CONF_PROTOCOL_MODBUS:
            # This is a device (slave) - get or create client
            client = await _create_modbus_device_client(hass, config, entry)
        elif protocol_name == CONF_PROTOCOL_SNMP:
            client = _create_snmp_client(config)
        elif protocol_name == CONF_PROTOCOL_MQTT:
            client = _create_mqtt_client(config)
        elif protocol_name == CONF_PROTOCOL_BACNET:
            client = _create_bacnet_client(config, hass)
        else:
            _LOGGER.error("Protocol %s not yet implemented", protocol_name)
            return False
    except Exception as err:
        _LOGGER.error("Failed to create client for %s: %s", protocol_name, err)
        return False
    
    # Create coordinator
    update_interval = entry.options.get(CONF_UPDATE_INTERVAL, 10)
    
    coordinator = CoordinatorClass(
        hass=hass,
        client=client,
        config_entry=entry,
        update_interval=timedelta(seconds=update_interval),
    )
    
    template_name = entry.options.get(CONF_TEMPLATE)
    
    if (template_name and not entry.options.get(CONF_TEMPLATE_APPLIED)):
        _LOGGER.info("Loading template '%s' for new device", template_name)
        await _load_template_into_options(hass, entry, protocol_name, template_name)
    
        # mark as applied
        options = dict(entry.options)
        options[CONF_TEMPLATE_APPLIED] = True
        hass.config_entries.async_update_entry(entry, options=options)
    
    
    await coordinator.async_config_entry_first_refresh()
    
    hass.data[DOMAIN]["coordinators"][entry.entry_id] = coordinator
    devicename = entry.title or entry.data.get(CONF_NAME) or f"{protocol_name.title()} Device"
    
    # CREATE DEVICE REGISTRY ENTRY
    device_registry = dr.async_get(hass)
    device_registry.async_get_or_create(
        config_entry_id=entry.entry_id,
        identifiers={(DOMAIN, entry.entry_id)},
        name=devicename,
        manufacturer=protocol_name.title(),
        model="Protocol Wizard",
        configuration_url=f"homeassistant://config/integrations/integration/{entry.entry_id}",
    )
    
    # Platforms
    await hass.config_entries.async_forward_entry_setups(entry, PLATFORMS)
    
    # Services (register once)
    if not hass.data[DOMAIN].get("services_registered"):
        await async_setup_services(hass)
        hass.data[DOMAIN]["services_registered"] = True
    
    # Frontend
    await async_install_frontend_resource(hass)
    await async_register_card(hass, entry)
    
    return True


# ============================================================================
# NEW: MODBUS HUB SETUP
# ============================================================================

async def _setup_modbus_hub(hass: HomeAssistant, entry: ConfigEntry, config: dict) -> bool:
    """Set up a Modbus Hub (shared connection only, no entities)."""
    _LOGGER.info("Setting up Modbus Hub: %s", entry.title)
    
    # Create the shared pymodbus client
    pymodbus_client = await _create_pymodbus_client(config)
    
    # Test connection
    try:
        await pymodbus_client.connect()
        if not pymodbus_client.connected:
            _LOGGER.error("Hub connection failed: %s", entry.title)
            return False
    except Exception as err:
        _LOGGER.error("Hub connection error: %s", err)
        return False
    
    # Store the shared client in hub registry
    hass.data[DOMAIN][HUB_CLIENTS][entry.entry_id] = {
        "client": pymodbus_client,
        "entry": entry,
    }
    
    _LOGGER.info("Modbus Hub '%s' ready - devices can now use this connection", entry.title)
    
    # Hubs don't create platforms - they just provide the connection
    # No coordinator, no entities, no platforms for hubs!
    
    # Still register services and frontend
    if not hass.data[DOMAIN].get("services_registered"):
        await async_setup_services(hass)
        hass.data[DOMAIN]["services_registered"] = True
    
    await async_install_frontend_resource(hass)
    
    return True


async def _create_pymodbus_client(config: dict):
    """Create raw pymodbus client from config."""
    conn_type = config.get(CONF_CONNECTION_TYPE)
    
    if conn_type == CONNECTION_TYPE_SERIAL:
        return AsyncModbusSerialClient(
            port=config[CONF_SERIAL_PORT],
            baudrate=int(config.get(CONF_BAUDRATE, DEFAULT_BAUDRATE)),
            parity=config.get(CONF_PARITY, DEFAULT_PARITY),
            stopbits=int(config.get(CONF_STOPBITS, DEFAULT_STOPBITS)),
            bytesize=int(config.get(CONF_BYTESIZE, DEFAULT_BYTESIZE)),
        )
    elif conn_type == CONNECTION_TYPE_TCP:
        return AsyncModbusTcpClient(
            host=config[CONF_HOST],
            port=int(config.get(CONF_PORT, 502)),
        )
    elif conn_type == CONNECTION_TYPE_UDP:
        return AsyncModbusUdpClient(
            host=config[CONF_HOST],
            port=int(config.get(CONF_PORT, 502)),
        )
    else:
        raise ValueError(f"Unknown connection type: {conn_type}")


async def _create_modbus_device_client(hass: HomeAssistant, config: dict, entry: ConfigEntry):
    """
    Create ModbusClient for a device (slave).
    
    If device has hub_id, uses shared hub client.
    Otherwise, creates standalone client (backward compatibility).
    """
    hub_id = config.get(CONF_HUB_ID)
    slave_id = config.get(CONF_SLAVE_ID, 1)
    
    # NEW: Device references a hub
    if hub_id:
        _LOGGER.info("Creating device client for slave %d on hub %s", slave_id, hub_id)
        
        # Get hub's shared client
        hub_data = hass.data[DOMAIN][HUB_CLIENTS].get(hub_id)
        
        if not hub_data:
            _LOGGER.error("Hub %s not found for device %s", hub_id, entry.title)
            raise ValueError(f"Hub {hub_id} not available")
        
        pymodbus_client = hub_data["client"]
        
        # Verify connection is still good
        if not pymodbus_client.connected:
            _LOGGER.info("Reconnecting hub for device %s", entry.title)
            await pymodbus_client.connect()
        
        # Wrap shared client with device-specific slave_id
        return ModbusClient(pymodbus_client, slave_id)
    
    # OLD: Standalone device (backward compatibility)
    else:
        _LOGGER.info("Creating standalone Modbus client for slave %d", slave_id)
        pymodbus_client = await _create_pymodbus_client(config)
        return ModbusClient(pymodbus_client, slave_id)


# ============================================================================
# EXISTING CLIENT CREATION FUNCTIONS (Keep for other protocols)
# ============================================================================

async def _create_modbus_client(hass, config, entry):
    """DEPRECATED: Old method - kept for backward compatibility."""
    # This is now handled by _create_modbus_device_client
    return await _create_modbus_device_client(hass, config, entry)


async def _create_modbus_hub(hass, config, entry):
    """DEPRECATED: This had the logic backwards - keeping for reference."""
    # The old code had this backwards - it was creating device clients in hub mode
    # Now properly handled by _setup_modbus_hub and _create_modbus_device_client
    _LOGGER.warning("_create_modbus_hub called - this should not happen with new logic")
    return await _create_modbus_device_client(hass, config, entry)


def _create_snmp_client(config):
    """Create SNMP client."""
    return SNMPClient(
        host=config[CONF_HOST],
        port=config.get(CONF_PORT, 161),
        community=config.get("community", "public"),
        version=config.get("version", "2c"),
    )

def _create_mqtt_client(config):
    """Create MQTT client."""
    return MQTTClient(
        broker=config.get("broker"),
        port=config.get(CONF_PORT, 1883),
        username=config.get("username"),
        password=config.get("password"),
    )

def _create_bacnet_client(config, hass):
    """Create BACnet client."""
    return BACnetClient(
        hass=hass,
        address=config.get("address"),
        object_identifier=config.get("object_identifier"),
        max_apdu_length=config.get("max_apdu_length", 1024),
    )


# ============================================================================
# TEMPLATE LOADING (UNCHANGED)
# ============================================================================

async def _load_template_into_options(
    hass: HomeAssistant,
    entry: ConfigEntry,
    protocol: str,
    template_name: str,
) -> None:
    """Load template entities into entry options."""
   
    try:
        
        template_data = await load_template(hass, protocol, template_name)
        
        if not template_data:
            _LOGGER.warning("Template %s is empty", template_name)
            return
        
        # Determine config key
        config_key = "registers" if protocol == CONF_PROTOCOL_MODBUS else "entities"
        
        # Update options with template entities
        new_options = dict(entry.options)
        new_options[config_key] = template_data
        new_options[CONF_TEMPLATE] = template_name
        
        hass.config_entries.async_update_entry(entry, options=new_options)
        _LOGGER.info("Loaded %d entities from template %s", len(template_data), template_name)
        
    except Exception as err:
        _LOGGER.error("Failed to load template %s: %s", template_name, err)


# ============================================================================
# SERVICES (UNCHANGED - keeping all existing service handlers)
# ============================================================================

async def async_setup_services(hass: HomeAssistant):
    """Register Protocol Wizard services."""
    
    def _get_coordinator(call: ServiceCall):
        """Get coordinator from service call."""
        entry_id = call.data.get("config_entry_id")
        if not entry_id:
            raise HomeAssistantError("config_entry_id is required")
        
        coordinator = hass.data[DOMAIN]["coordinators"].get(entry_id)
        if not coordinator:
            raise HomeAssistantError(f"No coordinator found for entry {entry_id}")
        
        return coordinator
    
    async def handle_write_register(call: ServiceCall):
        """Handle write_register service call."""
        coordinator = _get_coordinator(call)
        
        address = call.data["address"]
        value = call.data["value"]
        
        entity_config = {
            "register_type": call.data.get("register_type", "holding"),
            "data_type": call.data.get("data_type", "uint16"),
            "word_order": call.data.get("word_order", "big"),
        }
        
        _LOGGER.debug(
            "write_register service: addr=%s, value=%r, type=%s",
            address, value, entity_config["data_type"]
        )
        
        success = await coordinator.async_write_entity(
            address=str(address),
            value=value,
            entity_config=entity_config,
        )
        
        if not success:
            raise HomeAssistantError(f"Failed to write register at address {address}")
    
    async def handle_read_register(call: ServiceCall):
        """Handle read_register service call."""
        coordinator = _get_coordinator(call)
        
        address = call.data["address"]
        
        entity_config = {
            "register_type": call.data.get("register_type", "holding"),
            "data_type": call.data.get("data_type", "uint16"),
            "word_order": call.data.get("word_order", "big"),
        }
        
        kwargs = {
            "size": call.data.get("size", 1),
            "raw": call.data.get("raw", False),
        }
        
        value = await coordinator.async_read_entity(
            address=str(address),
            entity_config=entity_config,
            **kwargs
        )
        
        if value is None:
            raise HomeAssistantError(f"Failed to read register at address {address}")
        
        return {"value": value}
    
    async def handle_add_entity(call: ServiceCall):
        """Handle add_entity service call."""
        coordinator = _get_coordinator(call)
        
        entity_def = {
            "name": call.data["name"],
            "address": call.data["address"],
            "entity_type": call.data.get("entity_type", "sensor"),
            "register_type": call.data.get("register_type", "holding"),
            "data_type": call.data.get("data_type", "uint16"),
            "unit": call.data.get("unit"),
            "device_class": call.data.get("device_class"),
            "state_class": call.data.get("state_class"),
            "scale": call.data.get("scale", 1.0),
            "offset": call.data.get("offset", 0.0),
            "word_order": call.data.get("word_order", "big"),
        }
        
        protocol = coordinator.config_entry.data.get(CONF_PROTOCOL, CONF_PROTOCOL_MODBUS)
        config_key = "registers" if protocol == CONF_PROTOCOL_MODBUS else "entities"
        
        options = dict(coordinator.config_entry.options)
        entities = options.get(config_key, [])
        entities.append(entity_def)
        options[config_key] = entities
        
        hass.config_entries.async_update_entry(coordinator.config_entry, options=options)
        
        await hass.config_entries.async_reload(coordinator.config_entry.entry_id)
        
        _LOGGER.info("Added entity %s to %s", entity_def["name"], coordinator.config_entry.title)

    async def handle_read_snmp(call: ServiceCall):
        """SNMP read service."""
        coordinator = _get_coordinator(call)
        
        oid = call.data.get("oid")
        if not oid:
            raise HomeAssistantError("oid is required")
        
        entity_config = {
            "data_type": call.data.get("data_type", "string"),
            "device_id": call.data.get("device_id", None),
            "address": oid,
        }
        
        value = await coordinator.async_read_entity(
            address=oid,
            entity_config=entity_config,
        )
        
        if value is None:
            raise HomeAssistantError(f"Failed to read OID {oid}")
        
        return {"value": value}
    
    async def handle_write_snmp(call: ServiceCall):
        """SNMP write service."""
        coordinator = _get_coordinator(call)
        
        oid = call.data.get("oid")
        value = call.data.get("value")
        
        if not oid:
            raise HomeAssistantError("oid is required")
        if value is None:
            raise HomeAssistantError("value is required")
        
        entity_config = {
            "data_type": call.data.get("data_type", "string"),
            "device_id": call.data.get("device_id", None),
            "address": oid,
        }
        
        _LOGGER.debug(
            "write_snmp service: oid=%s, value=%r, data_type=%s",
            oid, value, entity_config["data_type"]
        )
        
        success = await coordinator.async_write_entity(
            address=oid,
            value=value,
            entity_config=entity_config,
        )
        
        if not success:
            raise HomeAssistantError(f"Failed to write to OID {oid}")

    async def handle_read_mqtt(call: ServiceCall):
        """MQTT read service."""
        coordinator = _get_coordinator(call)
        
        topic = call.data["topic"]
        wait_time = call.data.get("wait_time", 5.0)
        
        entity_config = {
            "data_type": "string",
        }
        
        value = await coordinator.async_read_entity(
            address=topic,
            entity_config=entity_config,
            wait_time=wait_time,
        )
        
        if value is None:
            raise HomeAssistantError(f"Failed to read MQTT topic {topic} (timeout or no data)")
        
        return {"value": value}
    
    async def handle_write_mqtt(call: ServiceCall):
        """MQTT write/publish service."""
        coordinator = _get_coordinator(call)
        
        topic = call.data["topic"]
        payload = call.data["payload"]
        qos = int(call.data.get("qos", 0))
        retain = call.data.get("retain", False)
        
        entity_config = {
            "data_type": "string",
        }
        
        success = await coordinator.async_write_entity(
            address=topic,
            value=payload,
            entity_config=entity_config,
            qos=qos,
            retain=retain,
        )
        
        if not success:
            raise HomeAssistantError(f"Failed to publish to MQTT topic {topic}")
        
        return {"success": True}
        
    async def handle_read_bacnet(call: ServiceCall):
        """BACnet read service."""
        coordinator = _get_coordinator(call)
        
        address = call.data.get("address")
        device_instance = call.data.get("device_instance")
        if not address:
           _LOGGER.error("address is required for bacnet read")
        
        entity_config = {
            "address": address,
            "data_type": call.data.get("data_type", "float"),
            "device_id": call.data.get("device_id", None),
            "device_instance" : device_instance
        }
        try:
            value = await coordinator.async_read_entity(
                address=address,
                entity_config=entity_config,
            )
        except Exception as err:
            _LOGGER.debug("BACnet Read failed returned %s with error: %s",value, err)
        if value is None:
           _LOGGER.error(f"Failed to read BACnet address {address}")
        
        return {"value": value}
    
    async def handle_write_bacnet(call: ServiceCall):
        """BACnet write service."""
        coordinator = _get_coordinator(call)
        
        address = call.data.get("address")
        value = call.data.get("value")
        device_instance = call.data.get("device_instance")
        
        if not address:
            _LOGGER.error("address is required writing to bacnet")
        if value is None:
            _LOGGER.error("value is required writing to bacnet")
        
        entity_config = {
            "address": address,
            "data_type": call.data.get("data_type", "float"),
            "device_id": call.data.get("device_id", None),
            "priority": call.data.get("priority", 8),
            "device_instance" : device_instance
        }
        
        _LOGGER.debug(
            "write_bacnet service: address=%s, value=%r, priority=%s",
            address, value, entity_config.get("priority")
        )
        try:
            success = await coordinator.async_write_entity(
                address=address,
                value=value,
                entity_config=entity_config,
            )
        except Exception as err:
            _LOGGER.debug("BACnet write failed, returned %s with error: %s",success, err)
        
        if not success:
            _LOGGER.error(f"Failed to write to BACnet address {address}")
        
        return {"success": True}
        
    # Register all services
    hass.services.async_register(DOMAIN, "write_register", handle_write_register)
    hass.services.async_register(
        DOMAIN,
        "read_register",
        handle_read_register,
        supports_response=SupportsResponse.ONLY,
    )
    hass.services.async_register(
        DOMAIN,
        "read_snmp",
        handle_read_snmp,
        supports_response=SupportsResponse.ONLY,
    )
    hass.services.async_register(DOMAIN, "write_snmp", handle_write_snmp)
    hass.services.async_register(
        DOMAIN,
        "add_entity",
        handle_add_entity,
    )
    hass.services.async_register(
        DOMAIN,
        "read_mqtt",
        handle_read_mqtt,
        supports_response=SupportsResponse.OPTIONAL,
    )
    hass.services.async_register(
        DOMAIN,
        "write_mqtt",
        handle_write_mqtt,
    )
    hass.services.async_register(
        DOMAIN,
        "read_bacnet",
        handle_read_bacnet,
        supports_response=SupportsResponse.ONLY,
    )
    hass.services.async_register(
        DOMAIN,
        "write_bacnet",
        handle_write_bacnet,
    )


# ============================================================================
# UNLOAD (MODIFIED to handle hubs)
# ============================================================================
    
async def async_unload_entry(hass: HomeAssistant, entry: ConfigEntry) -> bool:
    """Unload a config entry."""
    
    # Check if this is a hub
    is_hub = entry.data.get(CONF_IS_HUB, False)
    
    if is_hub:
        return await _unload_modbus_hub(hass, entry)
    
    # Regular device/protocol unload
    coordinator = hass.data[DOMAIN]["coordinators"].pop(entry.entry_id, None)
    
    unload_ok = await hass.config_entries.async_unload_platforms(entry, PLATFORMS)
    if not unload_ok:
        return False
    
    # Close connection if unused
    if coordinator:
        client = coordinator.client
        
        # Check if this device was using a hub
        hub_id = entry.data.get(CONF_HUB_ID)
        if hub_id:
            # Device was using shared hub - don't disconnect
            _LOGGER.info("Device %s unloaded (hub connection remains)", entry.title)
        else:
            # Standalone device - check if client is still used elsewhere
            still_used = any(
                c.client is client
                for c in hass.data[DOMAIN]["coordinators"].values()
            )
            
            if not still_used:
                try:
                    await client.disconnect()
                    _LOGGER.info("Closed standalone connection for %s", entry.title)
                except Exception as err:
                    _LOGGER.debug("Error closing client: %s", err)
    
    return True


async def _unload_modbus_hub(hass: HomeAssistant, entry: ConfigEntry) -> bool:
    """Unload a Modbus hub."""
    hub_id = entry.entry_id
    
    # Check if any devices are still using this hub
    devices_on_hub = [
        e for e in hass.config_entries.async_entries(DOMAIN)
        if e.data.get(CONF_HUB_ID) == hub_id
    ]
    
    if devices_on_hub:
        _LOGGER.error(
            "Cannot unload hub %s - %d device(s) still using it: %s",
            entry.title,
            len(devices_on_hub),
            [d.title for d in devices_on_hub]
        )
        return False
    
    # Close the shared client
    hub_data = hass.data[DOMAIN][HUB_CLIENTS].pop(hub_id, None)
    if hub_data:
        client = hub_data["client"]
        if client.connected:
            client.close()
            _LOGGER.info("Closed hub connection: %s", entry.title)
    
    return True
