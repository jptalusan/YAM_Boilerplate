import numpy as np
import json
# https://gis.stackexchange.com/questions/82850/serialize-python-list-containing-geometry
from shapely.geometry import Point, mapping

class NpEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, np.integer):
            return int(obj)
        elif isinstance(obj, np.floating):
            return float(obj)
        elif isinstance(obj, np.ndarray):
            return obj.tolist()
        elif isinstance(obj, Point):
            # return f"{obj.x},{obj.y}"
            return mapping(obj)
        else:
            return super(NpEncoder, self).default(obj)