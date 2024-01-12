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

    private final String broker1IpAddress;

    private final String broker2IpAddress;
    @Getter
    private boolean isCritRequested;
    @Getter
    @Setter
    public boolean isNodeInCrit;
    @Getter
    @Setter
    private Integer nodeLogicalTime;
    @Getter
    @Setter
    private Integer nodeId;
    @Getter
    private String nodeName;
    @Getter
    @Setter
    private Integer actualCount;
    @Getter
    @Setter
    private Integer currentNodeCount;
    @Getter
    private RequestDetails actualRequest;

    @Getter
    @Setter
    private Map<String, Instant> nodeTimes = new HashMap<>();
    @Getter
    @Setter
    private boolean isNodesLost = false;
    private ScheduledExecutorService pingExecutor;
    private ScheduledExecutorService pingMonitor;
    @Getter
    private Integer delayTime = 0;

    public Node(String[] args) {
        if(args.length!=3 && args.length!=4){
            logger.error("Node " + nodeName + ": " +"Configuration must have at least 3 parameters: <nodeName> <firstBrokerIpAddress> <secondBrokerIpAddress> (optional)<delayTimeInSeconds>");
            System.exit(1);
        }
        nodeName = args[0];
        broker1IpAddress = args[1];
        broker2IpAddress = args[2];
        if(args.length==4) {
            delayTime = Integer.valueOf(args[3]);
        }
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
            connectionFactory = new ActiveMQConnectionFactory("failover:(tcp://"+broker1IpAddress+":61616,tcp://"+broker2IpAddress+":61616)");
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
            Thread.sleep(delayTime*1000+1000);

            if (!joinListener.isMessageReceived()) {
                logger.info("Node " + nodeName + ": " +"No messages received. Setting nodeId to 1. I am first.");
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
                if(thisNode.getNodeId()!=-1) {
                    if ("-ce".equals(input)) {
                        if(!thisNode.isNodeInCrit) {
                            thisNode.requestCriticalSection();
                        }else{
                            logger.error("Node " + thisNode.getNodeName() + ": "+"Node is already in critical section");
                        }
                    } else if (input.startsWith("-m")) {
                        thisNode.sendChatMessage(input.split(":")[1]);
                    } else if ("-cl".equals(input)) {
                        if(thisNode.isNodeInCrit) {
                            thisNode.leaveCriticalSection();
                        }else{
                            logger.error("Node " + thisNode.getNodeName() + ": "+"Node is not in critical section");
                        }
                    } else if ("-logout".equals(input)) {
                        thisNode.logout();
                        System.exit(1);
                    }
                }else{
                    logger.info("Node " + thisNode.getNodeName() + ": " +"Node is joining chat now. Please wait.");
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
            }else{
                message = session.createTextMessage(nodeName+"|"+nodeId+"|"+nodeLogicalTime+"|SETID:"+actualCount+":"+name+":"+currentNodeCount);
            }
            Thread.sleep(delayTime*1000);
            topicProducerMap.get(TopicName.JOIN_TOPIC).send(message);
            startPinging();
        }catch (JMSException | InterruptedException e){
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
        }else{
            logger.error("Node " + nodeName + ": " +"Request not allowed. You are already requested to enter critical section or you are in critical section now.");
        }
    }

    public void leaveCriticalSection() throws InterruptedException {
        if(isNodeInCrit) {
            isNodeInCrit = false;
            isCritRequested = false;
            actualRequest = null;
            sendLeaveCritMessage();
            nodeLogicalTime++;
            logger.info("Node " + nodeName + ": " +"Leaving critical section");
        }else{
            logger.error("Node " + nodeName + ": " +"Request not allowed. You are not in critical section now.");
        }
    }

    public void sendEnterCritMessage(){
        try {
            TextMessage message = session.createTextMessage(nodeName+"|"+nodeId+"|"+nodeLogicalTime+"|ENTER:"+actualRequest.getNodeId()+":"+actualRequest.getNodeLogicalTime());
            Thread.sleep(delayTime*1000);
            topicProducerMap.get(TopicName.CRIT_TOPIC).send(message);
            nodeLogicalTime++;
        }catch (JMSException | InterruptedException e){
            e.printStackTrace();
        }
    }

    public void sendLeaveCritMessage(){
        try {
            TextMessage message = session.createTextMessage(nodeName+"|"+nodeId+"|"+nodeLogicalTime+"|LEAVE");
            Thread.sleep(delayTime*1000);
            topicProducerMap.get(TopicName.CRIT_TOPIC).send(message);
        }catch (JMSException | InterruptedException e){
            e.printStackTrace();
        }
    }

    public void sendCritReply(String text){
        try {
            TextMessage message = session.createTextMessage(nodeName+"|"+nodeId+"|"+nodeLogicalTime+"|REPLY:"+text);
            Thread.sleep(delayTime*1000);
            topicProducerMap.get(TopicName.CRIT_TOPIC).send(message);
        }catch (JMSException | InterruptedException e){
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
        logger.info("Node " + nodeName + ": " +"Entering critical section");
        sendChatMessage("Critical section is locked by node "+nodeName);
    }

    public void sendChatMessage(String text){
        if(isNodeInCrit){
            try {
                TextMessage message = session.createTextMessage(nodeName+"|"+nodeId+"|"+nodeLogicalTime+"|"+text);
                Thread.sleep(delayTime*1000);
                topicProducerMap.get(TopicName.CHAT_TOPIC).send(message);
            }catch (JMSException | InterruptedException e){
                e.printStackTrace();
            }
        }else{
            logger.error("Node " + nodeName + ": " +"This node is not in critical section. You cant send chat messages");
        }
    }

    public void logout(){
        try {
            if(isNodeInCrit){
                leaveCriticalSection();
                Thread.sleep(1000);
            } else if (isCritRequested) {
                isCritRequested = false;
                actualRequest = null;
            }
            logger.info("Node " + nodeName + ": " +"Logging out");
            topicConsumerMap.entrySet().forEach((entry)->
            {
                try {
                    entry.getValue().close();
                } catch (JMSException e) {
                    e.printStackTrace();
                }
            });
            topicProducerMap.entrySet().forEach((entry)->
            {
                try {
                    entry.getValue().close();
                } catch (JMSException e) {
                    e.printStackTrace();
                }
            });
            session.close();
            connection.close();
            pingExecutor.shutdown();
            pingMonitor.shutdown();
        }catch (JMSException | InterruptedException e){
            e.printStackTrace();
        }
    }


    public void startPinging(){
        pingExecutor = Executors.newSingleThreadScheduledExecutor();
        pingExecutor.scheduleAtFixedRate(this::sendPingMessage, 0, 20, TimeUnit.SECONDS);
        pingMonitor = Executors.newSingleThreadScheduledExecutor();
        pingMonitor.scheduleAtFixedRate(this::checkPings, 5, 10, TimeUnit.SECONDS);
    }

    private void checkPings() {
        List<String> lostNodes = new ArrayList<>();
        AtomicBoolean isStateChanged = new AtomicBoolean(false);
        nodeTimes.entrySet().forEach((entry)->
        {
            if (Duration.between(entry.getValue(), Instant.now()).getSeconds() > 40) {
                currentNodeCount--;
                lostNodes.add(entry.getKey());
                logger.info("Node " + nodeName + ": " +"Node " + entry.getKey() + " seems to die. Updating currentNodeCount = " + currentNodeCount);
                isStateChanged.set(true);
            }
            });
        for(String s: lostNodes){
            nodeTimes.remove(s);
        }
        if(nodeTimes.size()+1!=currentNodeCount){
            currentNodeCount=nodeTimes.size()+1;
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
            TextMessage message = session.createTextMessage(nodeName+"|PING");
            Thread.sleep(delayTime*1000);
            topicProducerMap.get(TopicName.PING_TOPIC).send(message);
        }catch (JMSException | InterruptedException e){
            e.printStackTrace();
        }
    }

    public void sendActualizeMessage(String name){
        try {
            TextMessage message = session.createTextMessage(nodeName+"|"+nodeId+"|"+nodeLogicalTime+"|ACTUALIZE:"+actualCount+":"+name+":"+currentNodeCount);
            Thread.sleep(delayTime*1000);
            topicProducerMap.get(TopicName.JOIN_TOPIC).send(message);
        }catch (JMSException | InterruptedException e){
            e.printStackTrace();
        }
    }
}
