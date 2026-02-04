from util.pzem import pzem_client, set_shunt_code

UNIT_ADDRESS = 1 # We don't need to address all units here

with pzem_client() as client:
    # - 0x0000 → 100 A
    # - 0x0001 → 50 A
    # - 0x0002 → 200 A
    # - 0x0003 → 300 A
    SHUNT_CODE = 0x0000

    set_shunt_code(client, UNIT_ADDRESS, SHUNT_CODE)
