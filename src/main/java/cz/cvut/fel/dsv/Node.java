package cz.cvut.fel.dsv;

import lombok.Getter;
import lombok.Setter;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import javax.jms.*;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;


public class Node{
    private static final Logger logger = LogManager.getLogger(Node.class);
    public static Node thisNode = null;
    private ConnectionFactory connectionFactory;
    private Connection connection;
    private Session session;
    private Topic chatTopic;
    private Topic joinTopic;
    private Topic pingTopic;
    private Topic critRequestTopic;

    private HashMap<String, MessageProducer> topicProducerMap = new HashMap<>();
    private HashMap<String, MessageConsumer> topicConsumerMap = new HashMap<>();
    @Getter
    private boolean isCritRequested;
    @Getter
    @Setter
    private Integer nodeLogicalTime;
    @Getter
    @Setter
    private Integer nodeId;
    @Getter
    @Setter
    private Integer actualCount;
    @Getter
    @Setter
    private Integer currentNodeCount;
    @Getter
    private RequestDetails actualRequest;

    @Getter
    private String nodeName;

    @Getter
    @Setter
    public boolean isNodeInCrit;

    private Map<String, Instant> nodeTimes = new HashMap<>();

    private ScheduledExecutorService heartbeatExecutor;
    
    private ScheduledExecutorService pingMonitor;
    @Getter
    @Setter
    private boolean isNodesLost = false;

    private final String brokerIpAddress;


    public Node(String[] args) {
        nodeName = args[0];
        brokerIpAddress = args[1];
        nodeId = -1;
        actualCount = -1;
        currentNodeCount = -1;
        isCritRequested = false;
        nodeLogicalTime = 0;
        actualRequest = null;
        isNodeInCrit = false;
        initializeJMS();
    }

    private void initializeJMS(){
        try{
            connectionFactory = new ActiveMQConnectionFactory("tcp://"+brokerIpAddress+":61616");
            connection = connectionFactory.createConnection();
            connection.start();
            session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            chatTopic = session.createTopic(TopicName.CHAT_TOPIC);
            joinTopic = session.createTopic(TopicName.JOIN_TOPIC);
            pingTopic = session.createTopic(TopicName.PING_TOPIC);
            critRequestTopic = session.createTopic(TopicName.CRIT_TOPIC);


            topicProducerMap.put(TopicName.CHAT_TOPIC, session.createProducer(chatTopic));
            topicProducerMap.put(TopicName.JOIN_TOPIC, session.createProducer(joinTopic));
            topicProducerMap.put(TopicName.PING_TOPIC, session.createProducer(pingTopic));
            topicProducerMap.put(TopicName.CRIT_TOPIC, session.createProducer(critRequestTopic));

            MessageConsumer joinConsumer = session.createConsumer(joinTopic);
            topicConsumerMap.put(TopicName.JOIN_TOPIC, joinConsumer);
            JoinMessageListener joinListener = new JoinMessageListener(this);
            joinConsumer.setMessageListener(joinListener);

            MessageConsumer critRequestConsumer = session.createConsumer(critRequestTopic);
            topicConsumerMap.put(TopicName.CRIT_TOPIC, critRequestConsumer);
            CritMessageListener critListener = new CritMessageListener(this);
            critRequestConsumer.setMessageListener(critListener);

            MessageConsumer chatConsumer = session.createConsumer(chatTopic);
            topicConsumerMap.put(TopicName.CHAT_TOPIC, chatConsumer);
            ChatMessageListener chatListener = new ChatMessageListener(this);
            chatConsumer.setMessageListener(chatListener);

            MessageConsumer pingConsumer = session.createConsumer(pingTopic);
            topicConsumerMap.put(TopicName.PING_TOPIC, pingConsumer);
            PingMessageListener pingListener = new PingMessageListener(this);
            pingConsumer.setMessageListener(pingListener);

            sendJoinMessage("");
            Thread.sleep(1000);
            if (!joinListener.isMessageReceived()) {
                logger.info("No messages received. Setting nodeId to 1. I am first.");
                nodeId = 1;
                actualCount = 1;
                currentNodeCount = 1;
            }
        } catch (JMSException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    public static void main(String[] args) {
        try {
            thisNode = new Node(args);
            Thread consoleListenerThread = new Thread(Node::handleInputCommand);
            consoleListenerThread.start();
        }catch (Exception e){
            e.printStackTrace();
        }

    }

    private static void handleInputCommand() {
        try (Scanner scanner = new Scanner(System.in)) {
            while (true) {
                if (!scanner.hasNextLine()) {
                    continue;
                }
                String input = scanner.nextLine();
                if ("-ce".equals(input)) {
                    thisNode.requestCriticalSection();
                } else if (input.startsWith("-m")) {
                    thisNode.sendChatMessage(input.split(":")[1]);
                }else if("-cl".equals(input)){
                    thisNode.leaveCriticalSection();
                }else if("-logout".equals(input)){
                    thisNode.logout();
                    break;
                }
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }


    public void sendJoinMessage(String name){
        try {
            TextMessage message = null;
            if(nodeId==-1) {
                message = session.createTextMessage(nodeName+"|"+nodeId+"|"+nodeLogicalTime+"|JOIN");
                startPinging();
            }else{
                message = session.createTextMessage(nodeName+"|"+nodeId+"|"+nodeLogicalTime+"|SETID:"+actualCount+":"+name+":"+currentNodeCount);
            }
            topicProducerMap.get(TopicName.JOIN_TOPIC).send(message);
        }catch (JMSException e){
            e.printStackTrace();
        }
    }

    public void requestCriticalSection() throws InterruptedException {
        if(!isCritRequested && actualRequest==null) {
            isCritRequested = true;
            actualRequest = new RequestDetails(nodeId, nodeLogicalTime);
            if(currentNodeCount==1){
                enterCritSection();
            }else {
                sendEnterCritMessage();
            }
//            Thread.sleep(1000);
        }else{
            logger.error("Request not allowed. You are already requested to enter critical section or you are in critical section now.");
        }
    }

    public void leaveCriticalSection() throws InterruptedException {
        if(isNodeInCrit) {
            isNodeInCrit = false;
            isCritRequested = false;
            actualRequest = null;
            sendLeaveCritMessage();
            nodeLogicalTime++;
            logger.info("Leaving critical section");
        }else{
            logger.error("Request not allowed. You are not in critical section now.");
        }
    }

    public void sendEnterCritMessage(){
        try {
            TextMessage message = session.createTextMessage(nodeName+"|"+nodeId+"|"+nodeLogicalTime+"|ENTER:"+actualRequest.getNodeId()+":"+actualRequest.getNodeLogicalTime());
            topicProducerMap.get(TopicName.CRIT_TOPIC).send(message);
            nodeLogicalTime++;
        }catch (JMSException e){
            e.printStackTrace();
        }
    }

    public void sendLeaveCritMessage(){
        try {
            TextMessage message = session.createTextMessage(nodeName+"|"+nodeId+"|"+nodeLogicalTime+"|LEAVE");
            topicProducerMap.get(TopicName.CRIT_TOPIC).send(message);
        }catch (JMSException e){
            e.printStackTrace();
        }
    }

    public void sendCritReply(String text){
        try {
            TextMessage message = session.createTextMessage(nodeName+"|"+nodeId+"|"+nodeLogicalTime+"|REPLY:"+text);
            topicProducerMap.get(TopicName.CRIT_TOPIC).send(message);
        }catch (JMSException e){
            e.printStackTrace();
        }
    }

    public void processEnterCritRequest(String sName, Integer sId, Integer requestLogicalTime){
        if(isNodeInCrit){
            sendCritReply("LOCKED"+":"+sName);
        }else{
            if(!isCritRequested){
                sendCritReply("OK"+":"+sName);
            }else {
                if(Objects.equals(requestLogicalTime, actualRequest.getNodeLogicalTime())){
                    if(sId<nodeId){
                        sendCritReply("OK"+":"+sName);
                    } else if (sId>nodeId) {
                        sendCritReply("NO:"+actualRequest.getNodeId()+":"+actualRequest.getNodeLogicalTime()+":"+sName);
                    }
                } else if (requestLogicalTime<actualRequest.getNodeLogicalTime()) {
                    sendCritReply("OK"+":"+sName);
                } else {
                    sendCritReply("NO:"+actualRequest.getNodeId()+":"+actualRequest.getNodeLogicalTime()+":"+sName);
                }
            }
        }
        nodeLogicalTime++;
    }

    public void enterCritSection(){
        isNodeInCrit = true;
        isCritRequested = false;
        actualRequest = null;
        nodeLogicalTime++;
        logger.info("Entering critical section");
        sendChatMessage("Critical section is locked by node "+nodeName);
    }

    public void sendChatMessage(String text){
        if(isNodeInCrit){
            try {
                TextMessage message = session.createTextMessage(nodeName+"|"+nodeId+"|"+nodeLogicalTime+"|"+text);
                topicProducerMap.get(TopicName.CHAT_TOPIC).send(message);
            }catch (JMSException e){
                e.printStackTrace();
            }
        }else{
            logger.error("You are not in critical section. You cant send chat messages");
        }
    }

    public void logout(){
        try {
            if(isNodeInCrit){
                leaveCriticalSection();
            } else if (isCritRequested) {
                isCritRequested = false;
                actualRequest = null;
            }
            TextMessage message = session.createTextMessage(nodeName+"|"+nodeId+"|"+nodeLogicalTime+"|LOGOUT");
            topicProducerMap.get(TopicName.JOIN_TOPIC).send(message);
            logger.info("Logging out");
            topicProducerMap.entrySet().forEach((entry)->
            {
                try {
                    entry.getValue().close();
                } catch (JMSException e) {
                    e.printStackTrace();
                }
            });
            topicConsumerMap.entrySet().forEach((entry)->
            {
                try {
                    entry.getValue().close();
                } catch (JMSException e) {
                    e.printStackTrace();
                }
            });
            session.close();
            connection.close();
            heartbeatExecutor.shutdown();
            pingMonitor.shutdown();
        }catch (JMSException | InterruptedException e){
            e.printStackTrace();
        }
    }


    public void startPinging(){
        heartbeatExecutor = Executors.newSingleThreadScheduledExecutor();
//        Runnable task1 = () -> {
//            System.out.println("Executing the task1 at: " + LocalTime.now());
//        };

        heartbeatExecutor.scheduleAtFixedRate(this::sendPingMessage, 0, 20, TimeUnit.SECONDS);
        pingMonitor = Executors.newSingleThreadScheduledExecutor();
        pingMonitor.scheduleAtFixedRate(this::checkPings, 5, 10, TimeUnit.SECONDS);

    }

    private void checkPings() {
        List<String> lostNodes = new ArrayList<>();
        AtomicBoolean isStateChanged = new AtomicBoolean(false);
        nodeTimes.entrySet().forEach((entry)->
        {
            if (Duration.between(entry.getValue(), Instant.now()).getSeconds() > 30) {
                currentNodeCount--;
                lostNodes.add(entry.getKey());
                logger.info("Node " + entry.getKey() + " seems to die. Updating currentNodeCount = " + currentNodeCount);
                isStateChanged.set(true);
            }
            });
        for(String s: lostNodes){
            nodeTimes.remove(s);
        }
        if(isStateChanged.get()){
            if(isCritRequested){
                isNodesLost = true;
                if(currentNodeCount==1){
                    enterCritSection();
                }else {
                    sendEnterCritMessage();
                }
            }
        }
    }

    public void updateTime(String senderName){
        if(nodeTimes.containsKey(senderName)){
            nodeTimes.replace(senderName, Instant.now());
        }else{
            nodeTimes.put(senderName, Instant.now());
        }
    }



    public void sendPingMessage(){
        try {
            TextMessage message = session.createTextMessage(nodeName+"|"+nodeId+"|"+nodeLogicalTime+"|PING");
            topicProducerMap.get(TopicName.PING_TOPIC).send(message);
        }catch (JMSException e){
            e.printStackTrace();
        }
    }
}
