#------------------------------------------
# options_flow.py – Protocol Wizard (protocol-agnostic)
#------------------------------------------
"""Options flow for Protocol Wizard – fully protocol-agnostic."""
from __future__ import annotations

import logging
import json
from datetime import timedelta
import voluptuous as vol
from .template_utils import ( 
    save_template, 
    get_available_templates, 
    get_template_dropdown_choices, 
    load_template,
    delete_template,
)
from homeassistant import config_entries
from homeassistant.helpers import selector

from .const import (
    DOMAIN,
    CONF_UPDATE_INTERVAL,
    CONF_ENTITIES,
    CONF_REGISTERS,
    CONF_PROTOCOL,
    CONF_PROTOCOL_MODBUS,
    CONF_PROTOCOL_SNMP,
    CONF_BYTE_ORDER,
    CONF_WORD_ORDER,
    CONF_REGISTER_TYPE,
)

_LOGGER = logging.getLogger(__name__)


# ============================================================================
# Options Flow
# ============================================================================

class ProtocolWizardOptionsFlow(config_entries.OptionsFlow):
    """Protocol-agnostic options flow for Protocol Wizard."""

    def __init__(self, config_entry: config_entries.ConfigEntry):
        self._config_entry = config_entry
        self.protocol = config_entry.data.get(CONF_PROTOCOL, CONF_PROTOCOL_MODBUS)

        self.schema_handler = self._get_schema_handler()

        # Determine the correct config key based on protocol
        if self.protocol == CONF_PROTOCOL_MODBUS:
            config_key = CONF_REGISTERS
        else:
            config_key = CONF_ENTITIES  # Future-proof for other protocols
            
        self._entities: list[dict] = list(config_entry.options.get(config_key, []))
        self._edit_index: int | None = None

    @property
    def config_entry(self) -> config_entries.ConfigEntry:
        return self._config_entry
        
    @staticmethod
    def _export_schema():
        return vol.Schema({
            vol.Required("name"): str
        })
        
    @staticmethod
    def _write_template(path: str, entities: list[dict]):
        with open(path, "w", encoding="utf-8") as f:
            json.dump(entities, f, indent=2)
    # ------------------------------------------------------------------
    # INIT
    # ------------------------------------------------------------------

    async def async_step_init(self, user_input=None):
        menu_options = {
            "settings": "Settings",
            "add_entity": "Add entity",
            "load_template": "Load template",
            "export_template": "Export template",
            "delete_template": "Delete user template",
        }
        if self._entities:
            menu_options["list_entities"] = f"Entities ({len(self._entities)})"
            menu_options["edit_entity"] = "Edit entity"

        return self.async_show_menu(step_id="init", menu_options=menu_options)


    # ------------------------------------------------------------------
    # SETTINGS
    # ------------------------------------------------------------------

    async def async_step_settings(self, user_input=None):
        if user_input:
            interval = user_input[CONF_UPDATE_INTERVAL]

            coordinator = (
                self.hass.data
                .get(DOMAIN, {})
                .get("coordinators", {})
                .get(self._config_entry.entry_id)
            )
            if coordinator:
                coordinator.update_interval = timedelta(seconds=interval)

            self._save_options({CONF_UPDATE_INTERVAL: interval})
            return self.async_abort(reason="settings_updated")

        current = self._config_entry.options.get(CONF_UPDATE_INTERVAL, 10)
        return self.async_show_form(
            step_id="settings",
            data_schema=vol.Schema({
                vol.Required(CONF_UPDATE_INTERVAL, default=current): vol.All(
                    vol.Coerce(int), vol.Range(min=5, max=300)
                )
            }),
        )

    # ------------------------------------------------------------------
    # ADD
    # ------------------------------------------------------------------

    async def async_step_add_entity(self, user_input=None):
        errors = {}

        if user_input:
            processed = self.schema_handler.process_input(user_input, errors, existing=None)
            if processed and not errors:
                self._entities.append(processed)
                await self._save_entities()
                return await self.async_step_init()

        return self.async_show_form(
            step_id="add_entity",
            data_schema=self.schema_handler.get_schema(),
            errors=errors,
        )

    # ------------------------------------------------------------------
    # EDIT SELECT
    # ------------------------------------------------------------------

    async def async_step_edit_entity(self, user_input=None):
        if user_input:
            self._edit_index = int(user_input["entity"])
            return await self.async_step_edit_entity_form()

        options = [
            selector.SelectOptionDict(
                value=str(i),
                label=self.schema_handler.format_label(e),
            )
            for i, e in enumerate(self._entities)
        ]

        return self.async_show_form(
            step_id="edit_entity",
            data_schema=vol.Schema({
                vol.Required("entity"): selector.SelectSelector(
                    selector.SelectSelectorConfig(
                        options=options,
                        mode=selector.SelectSelectorMode.DROPDOWN,
                    )
                )
            }),
        )

    # ------------------------------------------------------------------
    # EDIT FORM
    # ------------------------------------------------------------------

    async def async_step_edit_entity_form(self, user_input=None):
        entity = self._entities[self._edit_index]
        errors = {}

        if user_input:
            processed = self.schema_handler.process_input(user_input, errors, existing=entity)
            if processed and not errors:
                self._entities[self._edit_index] = processed
                await self._save_entities()
                return await self.async_step_init()

        defaults = self.schema_handler.get_defaults(entity)
        return self.async_show_form(
            step_id="edit_entity_form",
            data_schema=self.schema_handler.get_schema(defaults),
            errors=errors,
        )

    # ------------------------------------------------------------------
    # LIST / DELETE
    # ------------------------------------------------------------------

    async def async_step_list_entities(self, user_input=None):
        if user_input:
            if user_input.get("delete_all"):
                self._entities = []
            else:
                delete = set(user_input.get("delete", []))
                self._entities = [
                    e for i, e in enumerate(self._entities)
                    if str(i) not in delete
                ]
        
            await self._save_entities()
            return await self.async_step_init()

        options = [
            selector.SelectOptionDict(
                value=str(i),
                label=self.schema_handler.format_label(e),
            )
            for i, e in enumerate(self._entities)
        ]

        return self.async_show_form(
            step_id="list_entities",
            data_schema=vol.Schema({
                vol.Optional("delete_all", default=False): bool,
                vol.Optional("delete"): selector.SelectSelector(
                    selector.SelectSelectorConfig(
                        options=options,
                        multiple=True,
                        mode=selector.SelectSelectorMode.LIST,
                    )
                )
            }),
        )

    # ------------------------------------------------------------------
    # TEMPLATE
    # ------------------------------------------------------------------
    async def async_step_delete_template(self, user_input=None):
        """Delete a user template."""
        if user_input:
            template_id = user_input["template"]
            
            success, message = await delete_template(
                self.hass,
                self.protocol,
                template_id
            )
            
            if not success:
                return self.async_show_form(
                    step_id="delete_template",
                    data_schema=self._get_delete_template_schema(),
                    errors={"base": "delete_failed"},
                    description_placeholders={"error": message}
                )
            
            return self.async_abort(reason="template_deleted")
        
        # Get only user templates
        all_templates = await get_available_templates(self.hass, self.protocol)
        user_templates = {
            tid: info for tid, info in all_templates.items()
            if tid.startswith("user:")
        }
        dropdown_choices = get_template_dropdown_choices(user_templates)
        
        if not dropdown_choices:
            return self.async_abort(reason="no_user_templates")
        
        return self.async_show_form(
            step_id="delete_template",
            data_schema=vol.Schema({
                vol.Required("template"): vol.In(dropdown_choices)
            })
        )
    async def async_step_load_template(self, user_input=None):
        """Load a device template."""
        if user_input:
            template_id = user_input["template"]
            
            entities = await load_template(self.hass, self.protocol, template_id)
            
            if not entities:
                return self.async_show_form(
                    step_id="load_template",
                    data_schema=self._get_template_schema(),
                    errors={"base": "template_not_found"},
                )
            
            added = self.schema_handler.merge_template(self._entities, entities)
            
            if added == 0:
                return self.async_show_form(
                    step_id="load_template",
                    data_schema=self._get_template_schema(),
                    errors={"base": "template_empty_or_duplicate"},
                )
            
            await self._save_entities()
            return self.async_create_entry(title="", data={})
        
        # Get templates for dropdown
        templates = await get_available_templates(self.hass, self.protocol)
        dropdown_choices = get_template_dropdown_choices(templates)
        
        if not dropdown_choices:
            return self.async_abort(reason="no_templates")
        
        return self.async_show_form(
            step_id="load_template",
            data_schema=vol.Schema({
                vol.Required("template"): vol.In(dropdown_choices)
            })
        )
    # ------------------------------------------------------------------
    # Export template
    # ------------------------------------------------------------------
    async def async_step_export_template(self, user_input=None):
        if user_input:
            name = user_input["name"].strip()
            to_user_dir = user_input.get("save_to_user_dir", True)  # New option!
            
            if not name:
                return self.async_show_form(
                    step_id="export_template",
                    data_schema=self._export_schema(),
                    errors={"name": "required"},
                )
            
            # Optional: Get device metadata for richer templates
            metadata = {
                "name": name,
                "description": f"Exported from {self.config_entry.title}",
                "protocol": self.protocol,
            }
            
            success, message = await save_template(
                self.hass,
                self.protocol,
                name,
                self._entities,
                metadata=metadata,
                to_user_dir=to_user_dir
            )
            
            if not success:
                return self.async_show_form(
                    step_id="export_template",
                    data_schema=self._export_schema(),
                    errors={"base": "export_failed"},
                    description_placeholders={"error": message}
                )
            
            return self.async_show_form(
                step_id="export_template",
                data_schema=self._export_schema(),
                description_placeholders={"message": message}
            )
        
        return self.async_show_form(
            step_id="export_template",
            data_schema=self._export_schema()
        )
    
    def _export_schema(self):
        """Get export template schema."""
        return vol.Schema({
            vol.Required("name"): str,
            vol.Optional("save_to_user_dir", default=True): bool,  # NEW!
        })
    # ------------------------------------------------------------------
    # INTERNAL
    # ------------------------------------------------------------------
    def _get_template_schema(self, templates=None):
        """Return schema for template selection."""
        if templates is None:
            templates = []
        return vol.Schema({
            vol.Required("template"): selector.SelectSelector(
                selector.SelectSelectorConfig(
                    options=[selector.SelectOptionDict(value=t, label=t) for t in templates],
                    mode=selector.SelectSelectorMode.DROPDOWN,
                )
            )
        })
        
    @staticmethod
    def _load_template(path: str):
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)

    async def _save_entities(self):
        options = dict(self._config_entry.options)
        config_key = CONF_REGISTERS if self.protocol == CONF_PROTOCOL_MODBUS else CONF_ENTITIES
        options[config_key] = self._entities
        self.hass.config_entries.async_update_entry(self._config_entry, options=options)
        await asyncio.sleep(0.1) # give the options time to be written
        self.hass.async_create_task(
            self.hass.config_entries.async_reload(self._config_entry.entry_id)
        )

    def _save_options(self, updates: dict):
        options = dict(self._config_entry.options)
        options.update(updates)
        self.hass.config_entries.async_update_entry(self._config_entry, options=options)

    def _get_schema_handler(self):
        if self.protocol == CONF_PROTOCOL_SNMP:
            return SNMPSchemaHandler()
        elif self.protocol == CONF_PROTOCOL_MQTT:
            return MQTTSchemaHandler()
        return ModbusSchemaHandler()


# ============================================================================
# SCHEMA HANDLERS
# ============================================================================

class ModbusSchemaHandler:
    """Handles Modbus-specific schema and input processing."""

            
    @staticmethod
    def get_schema(defaults: dict | None = None) -> vol.Schema:
        defaults = defaults or {}

        schema = {
            vol.Required("name", default=defaults.get("name")): str,

            vol.Required("address", default=defaults.get("address")):
                vol.All(vol.Coerce(int), vol.Range(min=0, max=65535)),
            
            vol.Required(
                CONF_REGISTER_TYPE,
                default=defaults.get(CONF_REGISTER_TYPE, "input")
            ):
                selector.SelectSelector(
                    selector.SelectSelectorConfig(
                        options=["auto", "holding", "input", "coil", "discrete"],
                        mode=selector.SelectSelectorMode.DROPDOWN,
                    )
                ),
            vol.Required("data_type", default=defaults.get("data_type", "uint16")):
                selector.SelectSelector(
                    selector.SelectSelectorConfig(
                        options=[
                            "uint16", "int16",
                            "uint32", "int32",
                            "float32",
                            "uint64", "int64",
                        ],
                        mode=selector.SelectSelectorMode.DROPDOWN,
                    )
                ),

            vol.Required("rw", default=defaults.get("rw", "read")):
                selector.SelectSelector(
                    selector.SelectSelectorConfig(
                        options=["read", "write", "rw"],
                        mode=selector.SelectSelectorMode.DROPDOWN,
                    )
                ),
            vol.Optional("device_class", default=defaults.get("device_class", " ")): selector.SelectSelector(
                selector.SelectSelectorConfig(
                    options=[" ", "temperature", "power", "energy", "voltage", "current", "frequency", "duration"],
                    mode=selector.SelectSelectorMode.DROPDOWN,
                )
            ),
            vol.Optional("state_class", default=defaults.get("state_class", " ")): selector.SelectSelector(
                selector.SelectSelectorConfig(
                    options=[" ", "measurement", "total", "total_increasing"],
                    mode=selector.SelectSelectorMode.DROPDOWN,
                )
            ),
            vol.Optional("entity_category", default=defaults.get("entity_category", " ")): selector.SelectSelector(
                selector.SelectSelectorConfig(
                    options=[" ", "diagnostic", "config", "system"],
                    mode=selector.SelectSelectorMode.DROPDOWN,
                )
            ),
            vol.Optional("icon", default=defaults.get("icon", "")): str,  # e.g. mdi:thermometer
            vol.Optional("unit", default=defaults.get("unit", "")): str,
            vol.Optional("min", default=0.0): vol.Coerce(float),
            vol.Optional("max", default=65535.0 if "uint" in defaults.get("data_type", "") else 100.0): vol.Coerce(float),
            vol.Optional("step", default=0.1 if "float" in defaults.get("data_type", "") else 1.0): vol.Coerce(float),
            vol.Optional("format", default=defaults.get("format", "")): str,
            vol.Optional("scale", default=defaults.get("scale", 1.0)): vol.Coerce(float),
            vol.Optional("offset", default=defaults.get("offset", 0.0)): vol.Coerce(float),
            vol.Optional("options", default=defaults.get("options", "")): str,   # options: JSON string mapping raw values to labels
            vol.Optional(
                CONF_BYTE_ORDER,
                default=defaults.get(CONF_BYTE_ORDER, "big")
            ):
                selector.SelectSelector(
                    selector.SelectSelectorConfig(options=["big", "little"])
                ),

            vol.Optional(
                CONF_WORD_ORDER,
                default=defaults.get(CONF_WORD_ORDER, "big")
            ):
                selector.SelectSelector(
                    selector.SelectSelectorConfig(options=["big", "little"])
                ),

        }


        return vol.Schema(schema)

    @staticmethod
    def process_input(user_input: dict, errors: dict, existing: dict | None = None) -> dict | None:
        """
        Process user input for Modbus entity.
        Handles both new entities and edits, preserving old fields.
        """
        # Required validation first - check for None/missing, not falsy (0 is valid!)
        if "address" not in user_input or user_input.get("address") is None or user_input.get("address") == "":
            errors["address"] = "required"
            return None
        
        # Start with existing data (for edits) or empty dict
        processed = dict(existing) if existing else {}
        if "options" in processed and isinstance(processed["options"], str):
            try:
                processed["options"] = json.loads(processed["options"])
                if not isinstance(processed["options"], dict):
                    processed["options"]="" # if it is rubish, we dont use it
            except Exception:
                errors["options"] = ""
        # Update with new values, handling empty strings properly
        for key, value in user_input.items():
            if value in (" ","", None):
                processed.pop(key, None)  # Clear if empty
            elif value is not None:
                processed[key] = value        
        # Calculate size based on data_type
        type_sizes = {
            "uint16": 1, "int16": 1,
            "uint32": 2, "int32": 2,
            "float32": 2,
            "uint64": 4, "int64": 4,
        }
        dtype = processed.get("data_type")
        if dtype in type_sizes:
            processed["size"] = type_sizes[dtype]
        
        # Convert types
        try:
            processed["address"] = int(processed["address"])
            processed["size"] = int(processed.get("size", 1))
            processed["scale"] = float(processed.get("scale", 1.0))
            processed["offset"] = float(processed.get("offset", 0.0))
        except (ValueError, TypeError) as err:
            _LOGGER.error("Type conversion error: %s", err)
            errors["address"] = "invalid_number"
            return None
        
        # Ensure required fields exist with defaults
        processed.setdefault("register_type", "input")
        processed.setdefault("data_type", "uint16")
        processed.setdefault("rw", "read")
        processed.setdefault("byte_order", "big")
        processed.setdefault("word_order", "big")
        processed.setdefault("scale", 1.0)
        processed.setdefault("offset", 0.0)
        
        return processed

    def get_defaults(self, entity):
        """
        Get defaults for editing an entity.
        Returns the entity dict with all fields, using empty string for missing optional fields.
        """
        defaults = dict(entity)
        
        # Set empty string for optional fields that don't exist
        # (so form shows them as empty rather than None)
        defaults.setdefault("device_class", " ") # to ensure it work in dropdown
        defaults.setdefault("state_class", " ")
        defaults.setdefault("entity_category", " ")
        defaults.setdefault("icon", "")
        defaults.setdefault("unit", "")
        defaults.setdefault("format", "")
        opts = defaults.get("options")
        if isinstance(opts, dict):
            defaults["options"] = json.dumps(opts)
        else:
            defaults.setdefault("options", "")
        
        # Ensure numeric fields have values
        defaults.setdefault("scale", 1.0)
        defaults.setdefault("offset", 0.0)
        
        return defaults

    def format_label(self, entity):
        return f"{entity.get('name')} @ {entity.get('address')}"

    def merge_template(self, entities, template):
        """Merge template entities, processing each to add defaults."""
        added = 0
        existing = {(e.get("name"), e.get("address")) for e in entities}
        
        for template_entity in template:
            key = (template_entity.get("name"), template_entity.get("address"))
            if key not in existing:
                # Process template entity to add missing defaults
                errors = {}
                processed = self.process_input(template_entity, errors, existing=None)
                if processed and not errors:
                    entities.append(processed)
                    added += 1
                else:
                    _LOGGER.warning("Skipped invalid template entity: %s (errors: %s)", 
                                  template_entity.get("name"), errors)
        
        return added



class SNMPSchemaHandler:
    config_key = CONF_ENTITIES
            
    def get_schema(self, defaults=None):
        defaults = defaults or {}
        return vol.Schema({
            vol.Required("name", default=defaults.get("name")): str,
            vol.Required("address", default=defaults.get("address")): str,
            vol.Optional("read_mode", default="get"): selector.SelectSelector(
                selector.SelectSelectorConfig(
                    options=[
                        {"value": "get", "label": "Get (single value)"},
                        {"value": "walk", "label": "Walk (subtree table)"},
                    ],
                    mode=selector.SelectSelectorMode.DROPDOWN,
                )
            ),
            vol.Required("data_type", default=defaults.get("data_type", "string")):
                selector.SelectSelector(
                    selector.SelectSelectorConfig(
                        options=["string", "integer", "counter32", "counter64"],
                        mode=selector.SelectSelectorMode.DROPDOWN,
                    )
                ),
            vol.Optional("device_class", default=defaults.get("device_class", " ")): selector.SelectSelector(
                selector.SelectSelectorConfig(
                    options=[" ", "temperature", "power", "energy", "voltage", "current", "frequency", "duration"],
                    mode=selector.SelectSelectorMode.DROPDOWN,
                )
            ),
            vol.Optional("state_class", default=defaults.get("state_class", " ")): selector.SelectSelector(
                selector.SelectSelectorConfig(
                    options=[" ", "measurement", "total", "total_increasing"],
                    mode=selector.SelectSelectorMode.DROPDOWN,
                )
            ),
            vol.Optional("entity_category", default=defaults.get("entity_category", " ")): selector.SelectSelector(
                selector.SelectSelectorConfig(
                    options=[" ", "diagnostic", "config", "system"],
                    mode=selector.SelectSelectorMode.DROPDOWN,
                )
            ),
            vol.Optional("icon", default=defaults.get("icon", "")): str,  # e.g. mdi:thermometer
            vol.Optional("scale", default=defaults.get("scale", 1.0)): vol.Coerce(float),
            vol.Optional("offset", default=defaults.get("offset", 0.0)): vol.Coerce(float),
            vol.Optional("format", default=defaults.get("format", "")): str,
        })

    

    @staticmethod
    def process_input(
        user_input: dict,
        errors: dict,
        existing: dict | None = None,
    ) -> dict | None:
        """
        Process user input for SNMP entity.
        Handles both new entities and edits, preserving old fields.
        """
        # Required validation - check for None/missing, not falsy (empty string is invalid for OID)
        if "address" not in user_input or not user_input.get("address"):
            errors["address"] = "required"
            return None
        
        # Start with existing data (for edits) or empty dict
        processed = dict(existing) if existing else {}
        
        # Update with new values, handling empty strings properly
        for key, value in user_input.items():
            if value in (" ","", None):
                processed.pop(key, None)  # Clear if empty
            elif value is not None:
                processed[key] = value        
        
        # Convert types
        try:
            processed["scale"] = float(processed.get("scale", 1.0))
            processed["offset"] = float(processed.get("offset", 0.0))
        except (ValueError, TypeError) as err:
            _LOGGER.error("Type conversion error: %s", err)
            errors["scale"] = "invalid_number"
            return None
        
        # Ensure required fields exist with defaults
        processed.setdefault("data_type", "string")
        processed.setdefault("read_mode", "get")
        processed.setdefault("scale", 1.0)
        processed.setdefault("offset", 0.0)
        
        return processed

    def get_defaults(self, entity):
        """
        Get defaults for editing an entity.
        Returns the entity dict with all fields, using empty string for missing optional fields.
        """
        defaults = dict(entity)
        
        # Set empty string for optional fields that don't exist
        defaults.setdefault("device_class", " ")  # to ensure it work in dropdown
        defaults.setdefault("state_class", " ")
        defaults.setdefault("entity_category", " ")
        defaults.setdefault("icon", "")
        defaults.setdefault("format", "")
        
        # Ensure numeric fields have values
        defaults.setdefault("scale", 1.0)
        defaults.setdefault("offset", 0.0)
        
        # Ensure required fields
        defaults.setdefault("read_mode", "get")
        defaults.setdefault("data_type", "string")
        
        return defaults

    def format_label(self, entity):
        return f"{entity.get('name')} @ {entity.get('address')}"

    def merge_template(self, entities, template):
        """Merge template entities, processing each to add defaults."""
        added = 0
        existing = {(e.get("name"), e.get("address")) for e in entities}
        
        for template_entity in template:
            key = (template_entity.get("name"), template_entity.get("address"))
            if key not in existing:
                # Process template entity to add missing defaults
                errors = {}
                processed = self.process_input(template_entity, errors, existing=None)
                if processed and not errors:
                    entities.append(processed)
                    added += 1
                else:
                    _LOGGER.warning("Skipped invalid template entity: %s (errors: %s)", 
                                  template_entity.get("name"), errors)
        
        return added

class MQTTSchemaHandler:
    """Schema handler for MQTT entities."""
    
    config_key = "entities"  # MQTT uses 'entities' key
    
    def get_schema(self, defaults=None):
        """Return the schema for MQTT entity configuration."""
        defaults = defaults or {}
        return vol.Schema({
            vol.Required("name", default=defaults.get("name")): str,
            vol.Required("address", default=defaults.get("address")): str,  # MQTT topic
            vol.Required("data_type", default=defaults.get("data_type", "string")):
                selector.SelectSelector(
                    selector.SelectSelectorConfig(
                        options=[
                            {"value": "string", "label": "String"},
                            {"value": "integer", "label": "Integer"},
                            {"value": "float", "label": "Float"},
                            {"value": "boolean", "label": "Boolean (on/off)"},
                            {"value": "json", "label": "JSON (structured data)"},
                        ],
                        mode=selector.SelectSelectorMode.DROPDOWN,
                    )
                ),
            vol.Optional("rw", default=defaults.get("rw", "read")):
                selector.SelectSelector(
                    selector.SelectSelectorConfig(
                        options=[
                            {"value": "read", "label": "Read (Subscribe)"},
                            {"value": "write", "label": "Write (Publish)"},
                            {"value": "rw", "label": "Read/Write"},
                        ],
                        mode=selector.SelectSelectorMode.DROPDOWN,
                    )
                ),
            vol.Optional("qos", default=defaults.get("qos", 0)):
                selector.SelectSelector(
                    selector.SelectSelectorConfig(
                        options=[
                            {"value": "0", "label": "QoS 0 (At most once)"},
                            {"value": "1", "label": "QoS 1 (At least once)"},
                            {"value": "2", "label": "QoS 2 (Exactly once)"},
                        ],
                        mode=selector.SelectSelectorMode.DROPDOWN,
                    )
                ),
            vol.Optional("retain", default=defaults.get("retain", False)): bool,
            vol.Optional("scale", default=defaults.get("scale", 1.0)): vol.Coerce(float),
            vol.Optional("offset", default=defaults.get("offset", 0.0)): vol.Coerce(float),
            vol.Optional("format", default=defaults.get("format", "")): str,
            vol.Optional("options", default=defaults.get("options", "")): str,
            vol.Optional("device_class", default=defaults.get("device_class", "")): str,
            vol.Optional("state_class", default=defaults.get("state_class", "")): str,
            vol.Optional("entity_category", default=defaults.get("entity_category", "")): str,
            vol.Optional("icon", default=defaults.get("icon", "")): str,
            vol.Optional("unit", default=defaults.get("unit", "")): str,
        })
    
    def process_input(self, user_input, errors, existing=None):
        """Process and validate user input."""
        # Start with existing data (for edits) or empty dict
        processed = dict(existing) if existing else {}
        
        # Update with new values
        processed.update(user_input)
        
        # Validate topic format
        topic = processed.get("address", "")
        if not topic:
            errors["address"] = "Topic cannot be empty"
            return None
        
        # Validate QoS
        try:
            qos = int(processed.get("qos", 0))
            if qos not in (0, 1, 2):
                errors["qos"] = "QoS must be 0, 1, or 2"
                return None
            processed["qos"] = qos
        except (ValueError, TypeError):
            errors["qos"] = "Invalid QoS value"
            return None
        
        # Ensure boolean retain
        processed["retain"] = bool(processed.get("retain", False))
        
        # Parse options JSON if provided
        opts_str = processed.get("options", "")
        if isinstance(opts_str, str) and opts_str.strip():
            try:
                opts = json.loads(opts_str)
                processed["options"] = opts
            except json.JSONDecodeError:
                errors["options"] = "Invalid JSON format"
                return None
        elif isinstance(opts_str, dict):
            # Already a dict (from template)
            processed["options"] = opts_str
        else:
            processed.pop("options", None)
        
        # Clean empty strings
        for field in ["format", "device_class", "state_class", "entity_category", "icon", "unit"]:
            if not processed.get(field):
                processed.pop(field, None)
        
        # ADD REQUIRED DEFAULTS
        processed.setdefault("data_type", "string")
        processed.setdefault("rw", "read")
        processed.setdefault("qos", 0)
        processed.setdefault("retain", False)
        processed.setdefault("scale", 1.0)
        processed.setdefault("offset", 0.0)
        
        # Ensure numeric types
        try:
            processed["scale"] = float(processed.get("scale", 1.0))
            processed["offset"] = float(processed.get("offset", 0.0))
        except (ValueError, TypeError):
            errors["scale"] = "Invalid number"
            return None
        
        return processed
    
    def get_defaults(self, entity):
        """Return entity dict with defaults for form display."""
        defaults = dict(entity)
        
        # Set empty string for optional fields
        defaults.setdefault("device_class", "")
        defaults.setdefault("state_class", "")
        defaults.setdefault("entity_category", "")
        defaults.setdefault("icon", "")
        defaults.setdefault("unit", "")
        defaults.setdefault("format", "")
        
        # Ensure numeric fields
        defaults.setdefault("scale", 1.0)
        defaults.setdefault("offset", 0.0)
        defaults.setdefault("qos", 0)
        defaults.setdefault("retain", False)
        
        # Handle options
        opts = defaults.get("options")
        if isinstance(opts, dict):
            import json
            defaults["options"] = json.dumps(opts)
        else:
            defaults.setdefault("options", "")
        
        return defaults
    
    def format_label(self, entity):
        """Format entity label for display."""
        return f"{entity.get('name')} @ {entity.get('address')}"
    
    def merge_template(self, entities, template):
        """Merge template entities, processing each to add defaults."""
        added = 0
        existing = {(e.get("name"), e.get("address")) for e in entities}
        
        for template_entity in template:
            key = (template_entity.get("name"), template_entity.get("address"))
            if key not in existing:
                # Process template entity to add missing defaults
                errors = {}
                processed = self.process_input(template_entity, errors, existing=None)
                if processed and not errors:
                    entities.append(processed)
                    added += 1
                else:
                    _LOGGER.warning("Skipped invalid template entity: %s (errors: %s)", 
                                  template_entity.get("name"), errors)
        
        return added
