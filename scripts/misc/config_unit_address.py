from util.pzem import pzem_client, set_unit_address

OLD_UNIT_ADDRESS = 1
NEW_UNIT_ADDRESS = 2

with pzem_client() as client:
    set_unit_address(client, OLD_UNIT_ADDRESS, NEW_UNIT_ADDRESS)
