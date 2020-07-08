# Yet another boiler plate for a middleware  
* New and Improved boilerplate with the goal of a truly decentralized system.  
* Broker is only used for discovery of new nodes.  
* Workers perform jobs in a pipeline.  

**Capabilities:**  
* Perform a simple message transaction from user_query.py to a worker.  
* Perform a pipeline of tasks from Worker 1 to 0 to 2 and back to the client (user_query.py).  
* Has Redis as a database in the broker to store the heartbeats sent by the workers.  

**Steps for the middleware testbed:** 
1. docker-compose build  
2. docker-compose -f docker-compose-broker.yml build  
 
**In a separate terminal:**  
1. Git clone  
2. cd YAM_Boilerplate  
3. python3 -m venv venv    
4. pip install -r requirements.txt  
5. docker-compose -f docker-compose-broker.yml up  
6. docker-compose up  
5. python user_query.py  

# Problems:  
1. Rarely there are race condition problems i think? The pipeline jumps a worker and thus the whole pipeline is broken. I think a solution would be just to take care of how the pipeline is written.  

# References:  
Basic architecture of the boilerplate is based on this zmq tutorial:  
[Designing and testing pyzmq applications](https://stefan.sofa-rockers.org/2012/02/01/designing-and-testing-pyzmq-applications-part-1/)