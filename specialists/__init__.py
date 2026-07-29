# __init__.py - Specialist Registry and Loader for AELVO OMEGA

from specialists.hermes import HermesSpecialist
from specialists.forge import ForgeSpecialist
from specialists.terminus import TerminusSpecialist
from specialists.sentinel import SentinelSpecialist
from specialists.oracle import OracleSpecialist
from specialists.herald import HeraldSpecialist
from specialists.architect import ArchitectSpecialist

# Expose instances of all standard specialists
SPECIALIST_REGISTRY = {
    "HERMES": HermesSpecialist(),
    "FORGE": ForgeSpecialist(),
    "TERMINUS": TerminusSpecialist(),
    "SENTINEL": SentinelSpecialist(),
    "ORACLE": OracleSpecialist(),
    "HERALD": HeraldSpecialist(),
    "ARCHITECT": ArchitectSpecialist()
}

def get_specialist(name: str):
    """Retrieves an active specialist instance by name."""
    return SPECIALIST_REGISTRY.get(name.upper())

def list_specialists() -> list:
    """Lists the names of all registered specialists."""
    return list(SPECIALIST_REGISTRY.keys())
