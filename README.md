# ME - Lamport - Java JMS - chat
### App Description
Distributed chat application using Java JMS and ActiveMQ for communication between nodes.
It is using Lamport algorithm for mutual exception for sending chat messages from nodes to global chat. 

### How to run

Application is prepared for two running brokers on different virtual machines

1. Install ActiveMQ: https://activemq.apache.org/components/classic/download/ (it is strongly depends on jdk version compatibility, for example activemq 5.11 don't work correct with jdk version 17)
2. Install maven: ```sudo apt install maven```
3. Select two vm, where you want brokers to run. Go to activemq package, file ```/conf/activemq.xml``` you need to change broker name:
 in line ```<broker xmlns="http://activemq.apache.org/schema/core" brokerName="broker1" dataDirectory="${activemq.data}">``` set unique broker name for each broker.
4. Next, in section broker add code, so it will look like that:
```
<broker ...>
    ...
    <networkConnectors>
        <networkConnector name="bridge" uri="static:(tcp://<ip address of another broker>:61616)" duplex="false"/>
    </networkConnectors>
    ...
</broker>
```
3. Start ActiveMQ broker on one virtual machine: in activemq directory, run in terminal: ./bin/activemq console
4. In project directory, run in terminal: ```mvn exec:java -Dexec.args="<NodeName> <first broker ip address> <second broker ip address> (optional)<delay time>"```, where NodeName - unique string name of instance, 
broker ip address (writes as "0.0.0.0") - in my pc it is private IP address of virtual machine where broker runs, delay time - optional Integer parameter in seconds, if you want instance to have delay 
Example: ```mvn exec:java -Dexec.args="A 192.168.56.101 192.168.56.102"```

### Console commands

Application has those command:
1. ```-ce``` use command to send request to enter critical section to other alive nodes
2. ```-cl``` when node is in critical section, use this command to leave crit. section
3. ```-logout``` command to logout from application
4. ```-m:<message text>``` command for sending message to chat when node is in critical section

### Requests

All requests in app are sending through the topics. There are 4 topics for different goals:
- join topic: it is used for sending chat join request. When node starts it automatically sends join request to all existing nodes.
- ping topic: it is used for noticing the whole chat that node is still alive. If node don't send ping message for 40 seconds, it will be removed from chat as dead.
- chat topic: it is critical section of the program. Only one node may send messages to this topic onetime.
- crit topic: it is used for sending requests, when node wants to enter to critical section.

### Critical section

Every node has its id and logical time.
Id is set when node enters chat. It is received from other existing nodes or, if chat is empty, node id will be 1.
Node has actualCount of nodes ids, that have been used in the running chat, and node has currentNodeCount - amount of nodes that are alive now.
When node (for example node A) wants to enter critical section, it creates RequestDetails object (actualRequest) with it's current id and current logical time.
Than node A sends data from this request to other nodes.

Node B, that got this request, has to decide using it's own state if request sender may enter the critical section.
If node B is in critical section and it is locked, or if node B has own actualRequest, which logical time is smaller, it will reply to request saying that node A is not allowed to enter critical section.
Otherwise, it will agree with node A entering critical section.
If all active nodes will agree that node A may enter critical section, node A will enter there.

When node leaves critical section, other nodes will repeat their actual enter critical section request.

### Logging out or end without logout

When node is using logout, program will check if node is in critical section. If yes, it will leave it.
After that, application will end running. Other nodes handle logout (or end running without logout) by noticing that node don't send ping messages anymore.
Even if node ended running when it was in critical section, ping message absence will force nodes to repeat their actual requests.

### Simulation network slowness

Node may have a delay time in second. It is not recommended to set delay time higher than 30-35 second, because maximum waiting time for ping message is 40 seconds.
Delay time may set as configuration parameter (see running section).

### Logs

Logs are written to console and saved to file in path ```/logs/app.log```