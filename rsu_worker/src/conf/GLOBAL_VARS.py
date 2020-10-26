# ENVVARS
FULL_ROUTE = "FULL_ROUTE"
ROUTE_PLANNING = "ROUTE_PLANNING"
PARTIAL_ROUTE = "PARTIAL_ROUTE"
PARTIAL_ROUTE_UPDATE = "PARTIAL_ROUTE_UPDATE"
AGGREGATE_ROUTE = "AGGREGATE_ROUTE"

TASK_STATES = {
                # OKS
                "UNSENT": 0,
                "SENT": 1,
                "ACK": 2,
                "PROCESSED": 3,
                "RESPONDED": 4,
                "COLLECTED": 5,
                
                # ERRORS
                "MAX_TRY": 97,
                "TIMEOUT": 98,
                "ERROR": 99
                }

RSUS = {'Worker-0003': '1311220301',
        'Worker-0004': '1311220303',
        'Worker-0005': '1311220302',
        'Worker-0006': '1311220331',
        'Worker-0007': '1311220332'}

WORKER = {'1311220301': 'Worker-0003', 
          '1311220303': 'Worker-0004', 
          '1311220302': 'Worker-0005', 
          '1311220331': 'Worker-0006', 
          '1311220332': 'Worker-0007'}

PORTS = {'Worker-0003': 6003,
         'Worker-0004': 6004,
         'Worker-0005': 6005,
         'Worker-0006': 6006,
         'Worker-0007': 6007}

RSU_ID = "RSU_ID"

#MongoDB Collections
TASKS = "tasks"
QUERIES = "queries"

LOG_RATE = 0.5 #in seconds

# Routes get lost because of the limitations in the available nodes
# Some routes pass through boundaries that are at the corner of 4 grids/rsu
TIMEOUT = 300000
MAX_RETRIES = 5

NEIGHBOR_LEVEL = 2
QUEUE_THRESHOLD = 100
DELAY_THRESHOLD = 5

USE_SUB_GRIDS = False

X_AXIS = 5
Y_AXIS = 5

from shapely.geometry import Polygon

LNG_EXTEND = 0.0054931640625 * 4
LAT_EXTEND = 0.00274658203125 * 4
EXTENDED_DOWNTOWN_NASH_POLY = Polygon([(-86.878722 - LNG_EXTEND, 36.249723 + LAT_EXTEND),
                              (-86.878722 - LNG_EXTEND, 36.107442 - LAT_EXTEND),
                              (-86.68081100000001 + LNG_EXTEND, 36.107442 - LAT_EXTEND),
                              (-86.68081100000001 + LNG_EXTEND, 36.249723 + LAT_EXTEND),
                              (-86.878722 - LNG_EXTEND, 36.249723 + LAT_EXTEND)])