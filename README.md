# Yet another boiler plate for a middleware  
This was born out of spite from my stupid code that took me 3 hours just to add another 'service' to it.  

**Steps for the middleware testbed:** 
1. docker-compose up --build  
 
**In a separate terminal:**  
1. Git clone  
2. cd YAM_Boilerplate  
3. python3 -m venv venv    
4. pip install -r requirements.txt  
5. python main_prog.py  

Sample application:
**Client**  
1. 'task_count' determines the number of tasks you want to generate.  
2. main_prog.py sends a query to the broker with the msg_type = 'test_ping_query'    

**Broker**  
3. The broker receives the message and executes the test_ping_query function  
4. Here the broker *generates the query object and task objects*  
5. Set **DEBUG_MAX_WORKERS** to 0, to be able to use all available workers (in this case 3)  
6. Broker sends the tasks to the available workers  

**Workers**  
7. Workers receive the tasks with the msg_type = 'test_ping_task'  
8. They perform some processing in the function: **test_ping_task** which is to just perform some wait  
9. They publish a message to the topic: **topic**, with the msg_type: **status**  

**Broker**
10. Broker is subscribed to the topic and receives the payload  
11. It checks the task_id of the received payload and tries to find the corresponding task it has in the queue  
12. Updates the status of the task from *sent* to *done*  
13. It does this for all received messages  
14. At the same time, broker listens for heartbeats from the worker to see if they are still alive and purges if not  **(not yet implemented)**  
