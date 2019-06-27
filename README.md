#Yet another boiler plate for a middleware  
This was born out of spite from my stupid code that took me 3 hours just to add another 'service' to it.  

Steps:  
1. pip install -r requirements.txt  
2. python main_prog.py  

Sequence Diagram:  
Enter in the code below [here](https://sequencediagram.org/)  

title main_prog.py  
  
note over client:Not parallel  
loop i < 5  
  
client->broker:send HOPIA  
broker->worker:send MANI  
worker->worker:iterate counter  
end  
  
worker-->broker:Send kill command  