# TOPICS
HEARTBEAT="middleware/rsu/heartbeat"

BROKER_TO_RSU="middleware/rsu/"
RSU_TO_RSU="middleware/rsu-rsu/"

QUERY_TO_BROKER="middleware/broker/task"
RESPONSE_TO_BROKER="middleware/broker/response"

ERROR_RESPONSE_TO_BROKER="middleware/broker/response/error_response"

SUB_RESPONSE_TO_BROKER="middleware/broker/response/+"
PROCESSED_TO_BROKER="middleware/processed/+"

SIMULATED_QUERY_TO_BROKER="middleware/broker/simulation_task"
SIMULATED_SINGLE_QUERY_TO_BROKER="middleware/broker/simulation_single_task"

ALLOCATION_STATUS_TO_RSU="middleware/status/"
ALLOCATION_STATUS_TO_BROKER="middleware/broker/status"

START_LOGGING="middleware/rsu/startlogging"
STOP_LOGGING="middleware/rsu/stoplogging"

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

RSUS = {'0000': 'SPE_6g', '0001': 'SPE_Pa', '0002': 'SPBPqg', '0003': 'SPBQ@b', 
        '0004': 'SPBR6h', '0005': 'SPEPyO', '0006': 'SPEPeO', '0007': 'SPB_DO', 
        '0008': 'SPBZuN', '0009': 'SPBYyN', '0010': 'SPEP70', '0011': 'SPEPOe', 
        '0012': 'SPB_r0', '0013': 'SPBZYd', '0014': 'SPBY71', '0015': 'SPEOxP', 
        '0016': 'SPEOdP', '0017': 'SPBaCP', '0018': 'SPBbQt', '0019': 'SPBXxG', 
        '0020': 'SPEO7z', '0021': 'SPEOOK', '0022': 'SPBarz', '0023': 'SPBb9J', 
        '0024': 'SPBXhJ', '0025': 'SPBQ4@', '0026': 'SPBQvA', '0027': 'SPBZs6', 
        '0028': 'SPBZ77', '0029': 'SPB_pP', '0030': 'SPB_uP', '0031': 'SPB_nt', 
        '0032': 'SPB_wt', '0033': 'SPBZWF', '0034': 'SPBZTl', '0035': 'SPBZbn', 
        '0036': 'SPBZNH', '0037': 'SPBY5O', '0038': 'SPBY@O', '0039': 'SPBY3s', 
        '0040': 'SPBYBs', '0041': 'SPEOzp', '0042': 'SPEOkp', '0043': 'SPEOv@', 
        '0044': 'SPEOoV', '0045': 'SPBbOU', '0046': 'SPBbaT', '0047': 'SPBbSV', 
        '0048': 'SPBbX9'}

WORKER = {'SPBPqg': '0002', 'SPBQ4@': '0025', 'SPBQ@b': '0003', 'SPBQvA': '0026', 
          'SPBR6h': '0004', 'SPBXhJ': '0024', 'SPBXxG': '0019', 'SPBY3s': '0039', 
          'SPBY5O': '0037', 'SPBY71': '0014', 'SPBY@O': '0038', 'SPBYBs': '0040', 
          'SPBYyN': '0009', 'SPBZ77': '0028', 'SPBZNH': '0036', 'SPBZTl': '0034', 
          'SPBZWF': '0033', 'SPBZYd': '0013', 'SPBZbn': '0035', 'SPBZs6': '0027', 
          'SPBZuN': '0008', 'SPB_DO': '0007', 'SPB_nt': '0031', 'SPB_pP': '0029', 
          'SPB_r0': '0012', 'SPB_uP': '0030', 'SPB_wt': '0032', 'SPBaCP': '0017', 
          'SPBarz': '0022', 'SPBb9J': '0023', 'SPBbOU': '0045', 'SPBbQt': '0018', 
          'SPBbSV': '0047', 'SPBbX9': '0048', 'SPBbaT': '0046', 'SPEO7z': '0020', 
          'SPEOOK': '0021', 'SPEOdP': '0016', 'SPEOkp': '0042', 'SPEOoV': '0044', 
          'SPEOv@': '0043', 'SPEOxP': '0015', 'SPEOzp': '0041', 'SPEP70': '0010', 
          'SPEPOe': '0011', 'SPEPeO': '0006', 'SPEPyO': '0005', 'SPE_6g': '0000', 
          'SPE_Pa': '0001'}

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