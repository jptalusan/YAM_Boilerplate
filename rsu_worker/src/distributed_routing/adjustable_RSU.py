from shapely.geometry import Polygon
from shapely.geometry import LineString
from shapely.geometry import Point
from shapely.geometry import MultiPoint
from shapely.geometry.polygon import Polygon
from shapely.ops import nearest_points

import random
import math
import datetime
import time
import numpy as np
import copy

from common.conf import GLOBAL_VARS
import common.src.utility as geo_utils
from pprint import pprint

DEBUG = 0

class adjustable_RSU(object):
    # Class variables?
    delay_threshold = GLOBAL_VARS.DELAY_THRESHOLD
    subgraph = None

    # Coords are x, y and not row, col.
    def __init__(self, grid_id, poly, coords):
        self.coords = coords
        self.grid_id = grid_id
        self.poly = poly
        
        self.queue = []
        self.curr_accuracies = []
        self.curr_delay = 0.0
        self.curr_px = 0.0

        self._sub_grids = []
        self.queue_limit = GLOBAL_VARS.QUEUE_THRESHOLD

    def get_manhattan_distance(self, other_rsu):
        return abs(self.coords[0] - other_rsu.coords[0]) + \
               abs(self.coords[1] - other_rsu.coords[1])

    # NOTE: Random allocation
    def add_task_forced(self, task, use_sub_grids=True):
        if use_sub_grids:
            if self._sub_grids:
                ids = [sg.grid_id for sg in self._sub_grids]
                ids.append(self.grid_id)

                force_id = random.choice(ids)
                if force_id == self.grid_id:
                    self.queue.append(task)
                    return True, self.grid_id
                else:
                    for sg in self._sub_grids:
                        if sg.grid_id == force_id:
                            sg.queue.append(task)
                            return True, sg.grid_id
            else:
                self.queue.append(task)
                return True, self.grid_id
        else:
            self.queue.append(task)
            return True, self.grid_id

    def add_task(self, G, rsu_arr, task, use_sub_grids=True, constraints='queue'):
        if DEBUG == 1:
            print("Trying to add task {} to {} with q[{}/{}]".format(task._id, self.grid_id, len(self.queue), self.queue_limit))
        
        ideal_grid = task.gridA

        if isinstance(task.gridA, int):
            ideal_grid = task.gridB
        elif isinstance(task.gridA, str):
            ideal_grid = task.gridA

        if isinstance(task.gridB, int):
            next_grid = geo_utils.get_grid_id_from_node(G, task.gridB)
        elif isinstance(task.gridB, str):
            next_grid = task.gridB
        elif task.gridB is None:
            next_grid = ideal_grid

        ideal_rsu = geo_utils.get_rsu_by_grid_id(rsu_arr, ideal_grid)
        next_rsu = geo_utils.get_rsu_by_grid_id(rsu_arr, next_grid)

        # Not affected by change in actual grid
        delay_ideal = self.get_manhattan_distance(ideal_rsu)
        delay_next = self.get_manhattan_distance(next_rsu)
        total_delay = delay_ideal + delay_next
        
        # constraints
        met_delay_constraint = True
        met_queue_constraint = True
        
        use_delay_constraint = False
        use_queue_constraint = False
        
        constraint_arr = constraints.split("|")
        if 'delay' in constraint_arr:
            use_delay_constraint = True
        if 'queue' in constraint_arr:
            use_queue_constraint = True
        
        if use_delay_constraint:
            if total_delay > self.delay_threshold:
                met_delay_constraint = False
        
        if use_queue_constraint:
            if (len(self.queue) + 1) > self.queue_limit:
                met_queue_constraint = False

        if met_delay_constraint and met_queue_constraint:
            if DEBUG == 1:
                print("{} Adding task: {}".format(self.grid_id, task._id))

            # This will fill up the parent rsu first before moving on to subgrids
            if (len(self.queue) + 1) < self.queue_limit:
                self.queue.append(task)
                res = True
                grid_id = self.grid_id
                return res, grid_id
            elif use_sub_grids and len(self._sub_grids) > 0:
                res, grid_id = self.add_task_to_sub_grids(G, rsu_arr, task, constraints='queue')
                return res, grid_id
            else:
                self.queue.append(task)
                res = True
                grid_id = self.grid_id
                return res, grid_id

        else:
            return False, ""

    def add_task_to_sub_grids(self, G, rsu_arr, task, constraints='queue'):
        # NOTE: This should always work since the checking is with the parent not the child sub_grid
        random.shuffle(self._sub_grids)
        self._sub_grids = sorted(self._sub_grids, key=lambda rsu: len(rsu.queue), reverse=False)

        for sg in self._sub_grids:
            res = sg.add_task(G, rsu_arr, task, use_sub_grids=False, constraints=constraints)
            if res:
                used_grid_id = sg.grid_id
                return True, used_grid_id
        return False, used_grid_id

    def get_neighbors(self, rsu_arr):
        x, y = self.coords[0], self.coords[1]
        north = self.get_idx(x, y - 1)
        if north is None:
            north = None
        else:
            north = rsu_arr[north]
            
        south = self.get_idx(x, y + 1)
        if south is None:
            south = None
        else:
            south = rsu_arr[south]
            
        east = self.get_idx(x + 1, y)
        if east is None:
            east = None
        else:
            east = rsu_arr[east]
        
        west = self.get_idx(x - 1, y)
        if west is None:
            west = None
        else:
            west = rsu_arr[west]
            
        north_east = self.get_idx(x + 1, y - 1)
        if north_east is None:
            north_east = None
        else:
            north_east = rsu_arr[north_east]
            
        north_west = self.get_idx(x - 1, y - 1)
        if north_west is None:
            north_west = None
        else:
            north_west = rsu_arr[north_west]
            
        south_east = self.get_idx(x + 1, y + 1)
        if south_east is None:
            south_east = None
        else:
            south_east = rsu_arr[south_east]
            
        south_west = self.get_idx(x - 1, y + 1)
        if south_west is None:
            south_west = None
        else:
            south_west = rsu_arr[south_west]
            
        return { 'north'     : north,
                 'south'     : south,
                 'east'      : east,
                 'west'      : west,
                 'north-east': north_east,
                 'north-west': north_west,
                 'south-east': south_east,
                 'south-west': south_west
        }
        
    def graph_neighbors(self, rsu_arr):
        d = self.get_neighbors(rsu_arr)
        
        fig, ax = plt.subplots(1, 1, figsize=(5, 5))
        
        for k, v in d.items():
            if v is not None:
                plt.plot(*v.poly.exterior.xy, color='red', alpha=1)
                plt.fill(*v.poly.exterior.xy, color='red', alpha=0.2)
                plt.text(v.poly.centroid.x, v.poly.centroid.y, str(v.get_idx()) + ':' +  str(v.grid_id), fontsize=8)
        return fig
        
    def get_idx(self, x = None, y = None):
        if x is not None and y is not None:
            if x >= self.MAX_X or y >= self.MAX_Y:
                return None
            
            if x < 0 or y < 0:
                return None
            r = y + (x * self.MAX_Y)
            return r
        
        return self.coords[1] + (self.coords[0] * self.MAX_Y)
    
    def add_sub_grid(self, rsu):
        # print("Adding_sub_grid: {} to {}".format(rsu.grid_id, self.grid_id))
        gids = [r.grid_id for r in self._sub_grids]
        if rsu.grid_id not in gids:
            self._sub_grids.append(rsu)
            return True
        return False
    
    def get_sub_grids(self):
        return self._sub_grids
    
    def get_sub_grid_by_gid(self, gid):
        for r in self._sub_grids:
            if r.grid_id == gid:
                return r
        return False

    # Only for the rsu_arr
    def set_parent_grid(self, gid):
        pass

    def set_max_size(self, X, Y):
        self.MAX_X = X
        self.MAX_Y = Y
        
    def __repr__(self):
        return str({'grid_id':self.grid_id, 'coords':self.coords, 'idx': self.get_idx(), 'queue':len(self.queue)})
    def __str__(self):
        return str({'grid_id':self.grid_id, 'coords':self.coords, 'idx': self.get_idx(), 'queue':len(self.queue)})
    
    
    # Methods that deal with task allocationm
    