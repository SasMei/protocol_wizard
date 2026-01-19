# protocols/bacnet/const.py
"""BACnet-specific constants for Protocol Wizard."""

# Config key for storing entities
CONF_ENTITIES = "entities"  # BACnet uses 'entities' like SNMP/MQTT

# BACnet Object Types
# These are the most common BACnet object types
BACNET_OBJECT_TYPES = {
    # Analog objects
    "analogInput": "Analog Input",
    "analogOutput": "Analog Output", 
    "analogValue": "Analog Value",
    
    # Binary objects
    "binaryInput": "Binary Input",
    "binaryOutput": "Binary Output",
    "binaryValue": "Binary Value",
    
    # Multi-state objects
    "multiStateInput": "Multi-State Input",
    "multiStateOutput": "Multi-State Output",
    "multiStateValue": "Multi-State Value",
    
    # Other common objects
    "accumulator": "Accumulator",
    "loop": "Loop",
    "device": "Device",
    "file": "File",
    "group": "Group",
    "notificationClass": "Notification Class",
    "program": "Program",
    "schedule": "Schedule",
    "averaging": "Averaging",
    "trendLog": "Trend Log",
    "lifeSafetyPoint": "Life Safety Point",
    "lifeSafetyZone": "Life Safety Zone",
}

# BACnet Property Names
# Most commonly used properties
BACNET_PROPERTIES = {
    "presentValue": "Present Value",
    "statusFlags": "Status Flags",
    "eventState": "Event State",
    "reliability": "Reliability",
    "outOfService": "Out Of Service",
    "units": "Engineering Units",
    "description": "Description",
    "deviceType": "Device Type",
    "objectName": "Object Name",
    "objectType": "Object Type",
    "objectIdentifier": "Object Identifier",
    "covIncrement": "COV Increment",
    "timeDelay": "Time Delay",
    "notificationClass": "Notification Class",
    "highLimit": "High Limit",
    "lowLimit": "Low Limit",
    "deadband": "Deadband",
    "limitEnable": "Limit Enable",
    "eventEnable": "Event Enable",
    "ackedTransitions": "Acked Transitions",
    "notifyType": "Notify Type",
    "eventTimeStamps": "Event Time Stamps",
    "priority": "Priority",
    "relinquishDefault": "Relinquish Default",
}

# Data type mapping from BACnet to Python/HA
BACNET_DATA_TYPES = {
    "float": {
        "name": "Float (Decimal)",
        "python_type": float,
        "default": 0.0,
    },
    "integer": {
        "name": "Integer (Whole Number)",
        "python_type": int,
        "default": 0,
    },
    "boolean": {
        "name": "Boolean (True/False)",
        "python_type": bool,
        "default": False,
    },
    "string": {
        "name": "String (Text)",
        "python_type": str,
        "default": "",
    },
    "enumerated": {
        "name": "Enumerated (Options)",
        "python_type": int,
        "default": 0,
    },
    "unsigned": {
        "name": "Unsigned Integer",
        "python_type": int,
        "default": 0,
    },
}

# BACnet Engineering Units (subset - most common)
# Full list is in BACnet standard, this is a practical subset
BACNET_UNITS = {
    # Temperature
    "degreesCelsius": "°C",
    "degreesFahrenheit": "°F",
    "degreesKelvin": "K",
    
    # Pressure
    "pascals": "Pa",
    "kilopascals": "kPa",
    "bars": "bar",
    "poundsForcePerSquareInch": "psi",
    "inchesOfWater": "inH₂O",
    
    # Flow
    "litersPerSecond": "L/s",
    "cubicMetersPerSecond": "m³/s",
    "cubicFeetPerMinute": "CFM",
    "litersPerMinute": "L/min",
    
    # Power
    "watts": "W",
    "kilowatts": "kW",
    "megawatts": "MW",
    "btusPerHour": "BTU/h",
    "horsepower": "hp",
    
    # Energy
    "joules": "J",
    "kilojoules": "kJ",
    "kilowattHours": "kWh",
    "btus": "BTU",
    
    # Electrical
    "volts": "V",
    "kilovolts": "kV",
    "amperes": "A",
    "milliamperes": "mA",
    "ohms": "Ω",
    "kilohms": "kΩ",
    
    # Speed
    "metersPerSecond": "m/s",
    "kilometersPerHour": "km/h",
    "feetPerSecond": "ft/s",
    "feetPerMinute": "ft/min",
    
    # Percentage
    "percent": "%",
    "percentObscurationPerFoot": "%/ft",
    "percentObscurationPerMeter": "%/m",
    "percentPerSecond": "%/s",
    
    # Time
    "seconds": "s",
    "minutes": "min",
    "hours": "h",
    "days": "days",
    
    # Dimensionless
    "noUnits": "",
    "partsPerMillion": "ppm",
    "partsPerBillion": "ppb",
    "gramsPerKilogram": "g/kg",
    
    # Other
    "decibelA": "dBA",
    "nephelometricTurbidityUnit": "NTU",
    "pH": "pH",
}

# BACnet Write Priorities
# Priority 1 = highest, 16 = lowest
BACNET_PRIORITIES = {
    1: "Manual Life Safety",
    2: "Automatic Life Safety",
    3: "Available",
    4: "Available",
    5: "Critical Equipment Control",
    6: "Minimum On/Off",
    7: "Available",
    8: "Manual Operator",  # Default for manual writes
    9: "Available",
    10: "Available",
    11: "Available",
    12: "Available",
    13: "Available",
    14: "Available",
    15: "Available",
    16: "Available",
}

DEFAULT_WRITE_PRIORITY = 8  # Manual Operator

# BACnet Segmentation Support
BACNET_SEGMENTATION = {
    "segmentedBoth": "Segmented Both",
    "segmentedTransmit": "Segmented Transmit",
    "segmentedReceive": "Segmented Receive",
    "noSegmentation": "No Segmentation",
}

# Default port
DEFAULT_BACNET_PORT = 47808

# Discovery timeout
DEFAULT_DISCOVERY_TIMEOUT = 10  # seconds

# Read timeout
DEFAULT_READ_TIMEOUT = 5  # seconds


def parse_bacnet_address(address: str) -> tuple[str, int, str]:
    """
    Parse BACnet address string.
    
    Format: "objectType:instance:property"
    Example: "analogInput:0:presentValue"
    
    Args:
        address: BACnet address string
    
    Returns:
        Tuple of (object_type, instance, property_name)
    
    Raises:
        ValueError: If address format is invalid
    """
    parts = address.split(":")
    if len(parts) != 3:
        raise ValueError(
            f"Invalid BACnet address '{address}'. "
            "Expected format: 'objectType:instance:property'"
        )
    
    object_type = parts[0].strip()
    instance = int(parts[1].strip())
    property_name = parts[2].strip()
    
    return object_type, instance, property_name


def format_bacnet_address(object_type: str, instance: int, property_name: str) -> str:
    """
    Format BACnet address string.
    
    Args:
        object_type: BACnet object type
        instance: Object instance number
        property_name: Property name
    
    Returns:
        Formatted address string
    """
    return f"{object_type}:{instance}:{property_name}"


def entity_key(name: str) -> str:
    """
    Generate consistent key from entity name.
    
    Args:
        name: Entity name
    
    Returns:
        Lowercase key with underscores
    """
    return name.lower().strip().replace(" ", "_")
