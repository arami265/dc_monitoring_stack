from util.pzem import pzem_client, read_pzem
from config.pzem import PZEM_IDS

with pzem_client() as client:
    # Read all devices
    for ID in PZEM_IDS:
        reading = read_pzem(client, ID)
