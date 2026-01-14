#------------------------------------------
#-- protocol BACnet const.py protocol wizard
#------------------------------------------
"""BACnet-specific constants."""

# Config key for BACnet entities (uses standard "entities" not "registers")
CONF_ENTITIES = "entities"

# BACnet data type mapping - no fixed sizes like Modbus
# BACnet types are dynamic based on MIB definitions
BACnet_DATA_TYPES = [
    "string",
    "integer",
]

def oid_key(name: str) -> str:
    """Generate consistent key from OID name."""
    return name.lower().strip().replace(" ", "_")
