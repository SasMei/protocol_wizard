"""Config flow for Protocol Wizard - MODIFIED for Hub + Device Architecture."""
import logging
from typing import Any
import serial.tools.list_ports
import voluptuous as vol
from homeassistant import config_entries
from homeassistant.helpers import selector
from homeassistant.data_entry_flow import FlowResult
from homeassistant.config_entries import ConfigEntry
from homeassistant.core import callback
from pymodbus.client import AsyncModbusSerialClient, AsyncModbusTcpClient, AsyncModbusUdpClient
from .protocols.mqtt import CONF_BROKER, DEFAULT_PORT, CONF_USERNAME, CONF_PASSWORD
from .const import (
    CONNECTION_TYPE_SERIAL,
    CONNECTION_TYPE_IP,
    CONNECTION_TYPE_TCP,
    CONNECTION_TYPE_UDP,
    CONF_CONNECTION_TYPE,
    CONF_HOST,
    CONF_PORT,
    CONF_SERIAL_PORT,
    CONF_SLAVE_ID,
    CONF_BAUDRATE,
    CONF_PARITY,
    CONF_NAME,
    CONF_STOPBITS,
    CONF_BYTESIZE,
    CONF_FIRST_REG,
    CONF_FIRST_REG_SIZE,
    CONF_UPDATE_INTERVAL,
    DEFAULT_SLAVE_ID,
    DEFAULT_BAUDRATE,
    DEFAULT_TCP_PORT,
    DEFAULT_PARITY,
    DEFAULT_STOPBITS,
    DEFAULT_BYTESIZE,
    DOMAIN,
    CONF_PROTOCOL_MODBUS,
    CONF_PROTOCOL_SNMP,
    CONF_PROTOCOL_MQTT,
    CONF_PROTOCOL,
    CONF_IP,
    CONF_TEMPLATE,
)
from .options_flow import ProtocolWizardOptionsFlow
from .protocols import ProtocolRegistry
from .template_utils import get_available_templates, get_template_dropdown_choices, load_template

_LOGGER = logging.getLogger(__name__)
# Reduce noise from pymodbus
logging.getLogger("pymodbus").setLevel(logging.CRITICAL)
logging.getLogger("pymodbus.logging").setLevel(logging.CRITICAL)

# NEW CONSTANTS for Hub/Device architecture
CONF_IS_HUB = "is_hub"
CONF_HUB_ID = "hub_id"

class ProtocolWizardConfigFlow(config_entries.ConfigFlow, domain=DOMAIN):
    """Handle config flow for Protocol Wizard."""

    VERSION = 1

    def __init__(self) -> None:
        """Initialize the config flow."""
        self._data: dict[str, Any] = {}
        self._protocol: str = CONF_PROTOCOL_MODBUS
        self._selected_template: str | None = None
        self._is_device_flow: bool = False  # NEW: Track if adding device to hub

    @staticmethod
    @callback
    def async_get_options_flow(config_entry: ConfigEntry):
        """Get the options flow for this handler."""
        return ProtocolWizardOptionsFlow(config_entry)

    async def async_step_user(self, user_input: dict[str, Any] | None = None) -> FlowResult:
        """First step: protocol selection OR device addition."""
        available_protocols = ProtocolRegistry.available_protocols()
        
        # NEW: Check if we have existing Modbus hubs
        existing_hubs = self._get_existing_modbus_hubs()
        
        if user_input is not None:
            # Check if user wants to add device to existing hub
            if user_input.get("flow_type") == "add_device":
                self._is_device_flow = True
                return await self.async_step_select_hub()
            
            self._protocol = user_input.get(CONF_PROTOCOL, CONF_PROTOCOL_MODBUS)
            
            if self._protocol == CONF_PROTOCOL_MODBUS:
                return await self.async_step_modbus_common()
            elif self._protocol == CONF_PROTOCOL_SNMP:
                return await self.async_step_snmp_common()
            elif self._protocol == CONF_PROTOCOL_MQTT:
                return await self.async_step_mqtt_common()
        
        # Build schema with option to add device if hubs exist
        schema_dict = {}
        
        if existing_hubs:
            schema_dict[vol.Required("flow_type", default="new_hub")] = selector.SelectSelector(
                selector.SelectSelectorConfig(
                    options=[
                        selector.SelectOptionDict(value="new_hub", label="Create New Hub"),
                        selector.SelectOptionDict(value="add_device", label="Add Device to Existing Hub"),
                    ],
                    mode=selector.SelectSelectorMode.DROPDOWN,
                )
            )
        
        schema_dict[vol.Required(CONF_PROTOCOL, default=CONF_PROTOCOL_MODBUS)] = selector.SelectSelector(
            selector.SelectSelectorConfig(
                options=[
                    selector.SelectOptionDict(
                        value=proto,
                        label=proto.upper() if proto in (CONF_PROTOCOL_SNMP, CONF_PROTOCOL_MQTT) else proto.title()
                    )
                    for proto in sorted(available_protocols)
                ],
                mode=selector.SelectSelectorMode.DROPDOWN,
            )
        )
        
        return self.async_show_form(
            step_id="user",
            data_schema=vol.Schema(schema_dict),
        )

    # ================================================================
    # NEW: HUB SELECTION STEP
    # ================================================================
    
    def _get_existing_modbus_hubs(self) -> list[ConfigEntry]:
        """Get all existing Modbus hub config entries."""
        return [
            entry for entry in self.hass.config_entries.async_entries(DOMAIN)
            if entry.data.get(CONF_PROTOCOL) == CONF_PROTOCOL_MODBUS
            and entry.data.get(CONF_IS_HUB, False)
        ]
    
    async def async_step_select_hub(self, user_input: dict[str, Any] | None = None) -> FlowResult:
        """Select which hub to add a device to."""
        existing_hubs = self._get_existing_modbus_hubs()
        
        if not existing_hubs:
            return self.async_abort(reason="no_hubs_available")
        
        if user_input is not None:
            hub_id = user_input["hub_id"]
            self._data[CONF_HUB_ID] = hub_id
            
            # Get hub entry to determine connection type
            hub_entry = next((e for e in existing_hubs if e.entry_id == hub_id), None)
            if hub_entry:
                self._data.update({
                    CONF_PROTOCOL: CONF_PROTOCOL_MODBUS,
                    CONF_IS_HUB: False,
                    CONF_CONNECTION_TYPE: hub_entry.data.get(CONF_CONNECTION_TYPE),
                })
                return await self.async_step_device_config()
        
        # Build hub selection
        hub_options = [
            selector.SelectOptionDict(
                value=entry.entry_id,
                label=f"{entry.title} ({entry.data.get(CONF_CONNECTION_TYPE, 'Unknown')})"
            )
            for entry in existing_hubs
        ]
        
        return self.async_show_form(
            step_id="select_hub",
            data_schema=vol.Schema({
                vol.Required("hub_id"): selector.SelectSelector(
                    selector.SelectSelectorConfig(
                        options=hub_options,
                        mode=selector.SelectSelectorMode.DROPDOWN,
                    )
                )
            }),
            description_placeholders={
                "info": "Select the hub (connection) to add a new device to"
            }
        )
    
    async def async_step_device_config(self, user_input: dict[str, Any] | None = None) -> FlowResult:
        """Configure device (slave) settings."""
        errors = {}
        
        if user_input is not None:
            slave_id = user_input[CONF_SLAVE_ID]
            
            # Check for duplicate slave_id on this hub
            if self._is_slave_id_duplicate(self._data[CONF_HUB_ID], slave_id):
                errors["base"] = "duplicate_slave_id"
            else:
                # Get available templates
                templates = await self._get_available_templates()
                template_options = get_template_dropdown_choices(templates)
                
                final_data = {
                    **self._data,
                    CONF_NAME: user_input[CONF_NAME],
                    CONF_SLAVE_ID: slave_id,
                    CONF_FIRST_REG: user_input.get(CONF_FIRST_REG, 0),
                    CONF_FIRST_REG_SIZE: user_input.get(CONF_FIRST_REG_SIZE, 1),
                }
                
                # Test connection through hub
                hub_entry = self.hass.config_entries.async_get_entry(self._data[CONF_HUB_ID])
                if hub_entry:
                    try:
                        await self._async_test_device_on_hub(hub_entry, slave_id, 
                                                             final_data[CONF_FIRST_REG],
                                                             final_data[CONF_FIRST_REG_SIZE])
                    except Exception as err:
                        _LOGGER.error("Device test failed: %s", err)
                        errors["base"] = "cannot_connect"
                
                if not errors:
                    # Handle template if selected
                    options = {}
                    use_template = user_input.get("use_template", False)
                    if use_template and user_input.get(CONF_TEMPLATE):
                        options[CONF_TEMPLATE] = user_input[CONF_TEMPLATE]
                    
                    return self.async_create_entry(
                        title=f"{user_input[CONF_NAME]} (Slave {slave_id})",
                        data=final_data,
                        options=options,
                    )
        
        # Get available templates
        templates = await self._get_available_templates()
        template_options = [
            selector.SelectOptionDict(value=t, label=t)
            for t in get_template_dropdown_choices(templates)
        ]
        
        schema_dict = {
            vol.Required(CONF_NAME, default=f"Modbus Device"): str,
            vol.Required(CONF_SLAVE_ID, default=DEFAULT_SLAVE_ID): selector.NumberSelector(
                selector.NumberSelectorConfig(
                    min=1,
                    max=255,
                    step=1,
                    mode=selector.NumberSelectorMode.BOX,
                )
            ),
        }
        
        # Add template option if templates exist
        if templates:
            schema_dict[vol.Optional("use_template", default=False)] = selector.BooleanSelector()
            schema_dict[vol.Optional(CONF_TEMPLATE)] = selector.SelectSelector(
                selector.SelectSelectorConfig(
                    options=template_options,
                    mode=selector.SelectSelectorMode.DROPDOWN,
                )
            )
        
        # Add test parameters
        schema_dict.update({
            vol.Required(CONF_FIRST_REG, default=0): selector.NumberSelector(
                selector.NumberSelectorConfig(
                    min=0,
                    max=65535,
                    step=1,
                    mode=selector.NumberSelectorMode.BOX,
                )
            ),
            vol.Required(CONF_FIRST_REG_SIZE, default=1): selector.NumberSelector(
                selector.NumberSelectorConfig(
                    min=1,
                    max=10,
                    step=1,
                    mode=selector.NumberSelectorMode.BOX,
                )
            ),
        })
        
        return self.async_show_form(
            step_id="device_config",
            data_schema=vol.Schema(schema_dict),
            errors=errors,
            description_placeholders={
                "info": "Configure the Modbus device (slave) on the selected hub"
            }
        )
    
    def _is_slave_id_duplicate(self, hub_id: str, slave_id: int) -> bool:
        """Check if slave_id already exists on this hub."""
        for entry in self.hass.config_entries.async_entries(DOMAIN):
            if (entry.data.get(CONF_HUB_ID) == hub_id and 
                entry.data.get(CONF_SLAVE_ID) == slave_id):
                return True
        return False
    
    async def _async_test_device_on_hub(self, hub_entry: ConfigEntry, slave_id: int, 
                                       test_addr: int, test_size: int) -> None:
        """Test device connectivity through the hub."""
        # Get the hub's coordinator or create a temporary client
        hub_data = hub_entry.data
        
        if hub_data.get(CONF_CONNECTION_TYPE) == CONNECTION_TYPE_SERIAL:
            client = AsyncModbusSerialClient(
                port=hub_data[CONF_SERIAL_PORT],
                baudrate=hub_data.get(CONF_BAUDRATE, DEFAULT_BAUDRATE),
                parity=hub_data.get(CONF_PARITY, DEFAULT_PARITY),
                stopbits=hub_data.get(CONF_STOPBITS, DEFAULT_STOPBITS),
                bytesize=hub_data.get(CONF_BYTESIZE, DEFAULT_BYTESIZE),
            )
        else:
            # TCP or UDP
            client_class = (AsyncModbusTcpClient if hub_data.get(CONF_CONNECTION_TYPE) == CONNECTION_TYPE_TCP 
                          else AsyncModbusUdpClient)
            client = client_class(
                host=hub_data[CONF_HOST],
                port=hub_data.get(CONF_PORT, DEFAULT_TCP_PORT),
            )
        
        try:
            await client.connect()
            if not client.connected:
                raise ConnectionError("Failed to connect to hub")
            
            # Try reading test register from device
            result = await client.read_holding_registers(
                address=test_addr,
                count=test_size,
                device_id=slave_id,
            )
            
            if result.isError():
                raise ConnectionError(f"Failed to read from device with slave_id {slave_id}")
            
        finally:
            client.close()

    # ================================================================
    # MODBUS HUB CONFIG FLOW (MODIFIED)
    # ================================================================
    
    async def _get_available_templates(self) -> dict[str, str]:
        """Get available templates for dropdown."""
        templates = await get_available_templates(self.hass, self._protocol)
        return get_template_dropdown_choices(templates)
    
    async def _load_template_params(self, template_id: str) -> tuple[int, int]:
        """Load first register address and size from template."""
        entities = await load_template(self.hass, self._protocol, template_id)
        
        if not entities or len(entities) == 0:
            return 0, 1
        
        first = entities[0]
        address = first.get("address", 0)
        size = first.get("size", 1)
        return address, size
    
    async def async_step_modbus_common(self, user_input: dict[str, Any] | None = None) -> FlowResult:
        """Modbus: Common settings - NOW CREATES HUB."""
        self._protocol = CONF_PROTOCOL_MODBUS
        errors = {}
        
        if user_input is not None:
            self._data.update(user_input)
            self._data[CONF_PROTOCOL] = CONF_PROTOCOL_MODBUS
            self._data[CONF_IS_HUB] = True  # NEW: Mark as hub
            
            # Handle template selection
            use_template = user_input.get("use_template", False)
            if use_template:
                template_name = user_input.get(CONF_TEMPLATE)
                if template_name:
                    self._selected_template = template_name
                    addr, size = await self._load_template_params(template_name)
                    self._data[CONF_FIRST_REG] = addr
                    self._data[CONF_FIRST_REG_SIZE] = size
            
            # Proceed to connection-specific settings
            if user_input[CONF_CONNECTION_TYPE] == CONNECTION_TYPE_SERIAL:
                return await self.async_step_modbus_serial()
            return await self.async_step_modbus_ip()
        
        # Get available templates
        templates = await self._get_available_templates()
        template_options = [
            selector.SelectOptionDict(value=t, label=t)
            for t in templates
        ]
        
        # Build schema - REMOVED SLAVE_ID (that's for devices)
        schema_dict = {
            vol.Required(CONF_NAME, default="Modbus Hub"): str,
            vol.Required(CONF_CONNECTION_TYPE, default=CONNECTION_TYPE_SERIAL): selector.SelectSelector(
                selector.SelectSelectorConfig(
                    options=[
                        selector.SelectOptionDict(value=CONNECTION_TYPE_SERIAL, label="Serial (RS485/RTU)"),
                        selector.SelectOptionDict(value=CONNECTION_TYPE_IP, label="IP (Modbus TCP/UDP)"),
                    ],
                    mode=selector.SelectSelectorMode.DROPDOWN,
                )
            ),
            # NOTE: We'll add slave_id in the next step for initial device
        }
        
        # Add template option if templates exist
        if templates:
            schema_dict[vol.Optional("use_template", default=False)] = selector.BooleanSelector()
            schema_dict[vol.Optional(CONF_TEMPLATE)] = selector.SelectSelector(
                selector.SelectSelectorConfig(
                    options=template_options,
                    mode=selector.SelectSelectorMode.DROPDOWN,
                )
            )
        
        # Add test parameters
        schema_dict.update({
            vol.Required(CONF_FIRST_REG, default=0): selector.NumberSelector(
                selector.NumberSelectorConfig(
                    min=0,
                    max=65535,
                    step=1,
                    mode=selector.NumberSelectorMode.BOX,
                )
            ),
            vol.Required(CONF_FIRST_REG_SIZE, default=1): selector.NumberSelector(
                selector.NumberSelectorConfig(
                    min=1,
                    max=10,
                    step=1,
                    mode=selector.NumberSelectorMode.BOX,
                )
            ),
        })
        
        return self.async_show_form(
            step_id="modbus_common",
            data_schema=vol.Schema(schema_dict),
            errors=errors,
            description_placeholders={
                "info": "Creating a Modbus Hub (connection). You'll add devices (slaves) afterward."
            }
        )

    async def async_step_modbus_serial(self, user_input: dict[str, Any] | None = None) -> FlowResult:
        """Modbus Serial (RTU) specific settings."""
        errors = {}
        
        if user_input is not None:
            self._data.update(user_input)
            
            try:
                # Test connection
                await self._async_test_modbus_serial(self._data)
                
                # Create hub entry
                final_data = {
                    **self._data,
                    CONF_CONNECTION_TYPE: CONNECTION_TYPE_SERIAL,
                }
                
                options = {}
                if self._selected_template:
                    options[CONF_TEMPLATE] = self._selected_template
                
                return self.async_create_entry(
                    title=f"Modbus Hub: {self._data[CONF_SERIAL_PORT]}",
                    data=final_data,
                    options=options,
                )
                
            except Exception as err:
                _LOGGER.exception("Modbus serial connection test failed: %s", err)
                errors["base"] = "cannot_connect"
        
        # Get available serial ports
        ports = await self.hass.async_add_executor_job(serial.tools.list_ports.comports)
        port_options = [
            selector.SelectOptionDict(value=p.device, label=f"{p.device} - {p.description}")
            for p in ports
        ]
        
        if not port_options:
            port_options = [selector.SelectOptionDict(value="/dev/ttyUSB0", label="Manual Entry")]
        
        return self.async_show_form(
            step_id="modbus_serial",
            data_schema=vol.Schema({
                vol.Required(CONF_SERIAL_PORT): selector.SelectSelector(
                    selector.SelectSelectorConfig(
                        options=port_options,
                        mode=selector.SelectSelectorMode.DROPDOWN,
                        custom_value=True,
                    )
                ),
                vol.Optional(CONF_BAUDRATE, default=DEFAULT_BAUDRATE): selector.SelectSelector(
                    selector.SelectSelectorConfig(
                        options=[
                            selector.SelectOptionDict(value=str(b), label=str(b))
                            for b in [1200, 2400, 4800, 9600, 19200, 38400, 57600, 115200]
                        ],
                        mode=selector.SelectSelectorMode.DROPDOWN,
                    )
                ),
                vol.Optional(CONF_PARITY, default=DEFAULT_PARITY): selector.SelectSelector(
                    selector.SelectSelectorConfig(
                        options=[
                            selector.SelectOptionDict(value="N", label="None"),
                            selector.SelectOptionDict(value="E", label="Even"),
                            selector.SelectOptionDict(value="O", label="Odd"),
                        ],
                        mode=selector.SelectSelectorMode.DROPDOWN,
                    )
                ),
                vol.Optional(CONF_STOPBITS, default=DEFAULT_STOPBITS): selector.NumberSelector(
                    selector.NumberSelectorConfig(
                        min=1,
                        max=2,
                        step=1,
                        mode=selector.NumberSelectorMode.BOX,
                    )
                ),
                vol.Optional(CONF_BYTESIZE, default=DEFAULT_BYTESIZE): selector.NumberSelector(
                    selector.NumberSelectorConfig(
                        min=5,
                        max=8,
                        step=1,
                        mode=selector.NumberSelectorMode.BOX,
                    )
                ),
                vol.Optional(CONF_UPDATE_INTERVAL, default=10): vol.All(
                    vol.Coerce(int),
                    vol.Range(min=1, max=300),
                ),
            }),
            errors=errors,
        )

    async def async_step_modbus_ip(self, user_input: dict[str, Any] | None = None) -> FlowResult:
        """Modbus TCP/UDP specific settings."""
        errors = {}
        
        if user_input is not None:
            self._data.update(user_input)
            
            try:
                # Determine if TCP or UDP
                conn_type = CONNECTION_TYPE_TCP if user_input.get("use_tcp", True) else CONNECTION_TYPE_UDP
                self._data[CONF_CONNECTION_TYPE] = conn_type
                
                # Test connection
                await self._async_test_modbus_ip(self._data)
                
                final_data = {
                    **self._data,
                }
                
                options = {}
                if self._selected_template:
                    options[CONF_TEMPLATE] = self._selected_template
                
                return self.async_create_entry(
                    title=f"Modbus Hub: {self._data[CONF_HOST]}:{self._data.get(CONF_PORT, DEFAULT_TCP_PORT)} ({conn_type.upper()})",
                    data=final_data,
                    options=options,
                )
                
            except Exception as err:
                _LOGGER.exception("Modbus IP connection test failed: %s", err)
                errors["base"] = "cannot_connect"
        
        return self.async_show_form(
            step_id="modbus_ip",
            data_schema=vol.Schema({
                vol.Required(CONF_HOST): str,
                vol.Optional(CONF_PORT, default=DEFAULT_TCP_PORT): vol.All(
                    vol.Coerce(int), vol.Range(min=1, max=65535)
                ),
                vol.Optional("use_tcp", default=True): selector.BooleanSelector(
                    selector.BooleanSelectorConfig()
                ),
                vol.Optional(CONF_UPDATE_INTERVAL, default=10): vol.All(
                    vol.Coerce(int),
                    vol.Range(min=1, max=300),
                ),
            }),
            errors=errors,
            description_placeholders={
                "tcp_info": "TCP is standard, UDP is rarely used"
            }
        )

    async def _async_test_modbus_serial(self, data: dict[str, Any]) -> None:
        """Test Modbus serial connection."""
        from .protocols.modbus import ModbusClient
        
        client = AsyncModbusSerialClient(
            port=data[CONF_SERIAL_PORT],
            baudrate=int(data.get(CONF_BAUDRATE, DEFAULT_BAUDRATE)),
            parity=data.get(CONF_PARITY, DEFAULT_PARITY),
            stopbits=int(data.get(CONF_STOPBITS, DEFAULT_STOPBITS)),
            bytesize=int(data.get(CONF_BYTESIZE, DEFAULT_BYTESIZE)),
        )
        
        # Use slave_id 1 for hub test (or first_reg if provided)
        test_slave_id = data.get(CONF_SLAVE_ID, 1)
        wrapper = ModbusClient(client, test_slave_id)
        
        try:
            if not await wrapper.connect():
                raise ConnectionError("Failed to connect to Modbus serial device")
            
            # Try reading test register
            result = await wrapper.read(
                address=str(data.get(CONF_FIRST_REG, 0)),
                count=data.get(CONF_FIRST_REG_SIZE, 1),
                register_type="holding"
            )
            
            if result is None:
                raise ConnectionError("Failed to read test register")
                
        finally:
            await wrapper.disconnect()

    async def _async_test_modbus_ip(self, data: dict[str, Any]) -> None:
        """Test Modbus TCP/UDP connection."""
        from .protocols.modbus import ModbusClient
        
        conn_type = data.get(CONF_CONNECTION_TYPE, CONNECTION_TYPE_TCP)
        client_class = AsyncModbusTcpClient if conn_type == CONNECTION_TYPE_TCP else AsyncModbusUdpClient
        
        client = client_class(
            host=data[CONF_HOST],
            port=int(data.get(CONF_PORT, DEFAULT_TCP_PORT)),
        )
        
        test_slave_id = data.get(CONF_SLAVE_ID, 1)
        wrapper = ModbusClient(client, test_slave_id)
        
        try:
            if not await wrapper.connect():
                raise ConnectionError(f"Failed to connect to Modbus {conn_type.upper()} device")
            
            result = await wrapper.read(
                address=str(data.get(CONF_FIRST_REG, 0)),
                count=data.get(CONF_FIRST_REG_SIZE, 1),
                register_type="holding"
            )
            
            if result is None:
                raise ConnectionError("Failed to read test register")
                
        finally:
            await wrapper.disconnect()

    # ================================================================
    # SNMP & MQTT CONFIG FLOWS (UNCHANGED)
    # ================================================================
    
    async def async_step_snmp_common(self, user_input: dict[str, Any] | None = None) -> FlowResult:
        """SNMP configuration (unchanged from original)."""
        # ... keep original implementation ...
        pass
    
    async def async_step_mqtt_common(self, user_input: dict[str, Any] | None = None) -> FlowResult:
        """MQTT configuration (unchanged from original)."""
        # ... keep original implementation ...
        pass
