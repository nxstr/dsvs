package cz.cvut.fel.dsv;

import lombok.Getter;
import lombok.Setter;
import org.apache.activemq.ActiveMQConnectionFactory;

import javax.jms.*;
import java.util.HashMap;

public class Node{
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
    private Integer nodeLogicalTime;
    @Getter
    @Setter
    private Integer nodeId;
    @Getter
    @Setter
    private Integer actualCount;

    @Getter
    private String nodeName;


    public Node(String[] args) {
        nodeName = args[0];
        nodeId = -1;
        actualCount = -1;
        isCritRequested = false;
        nodeLogicalTime = 0;
        initializeJMS();
    }

    private void initializeJMS(){
        try{
            connectionFactory = new ActiveMQConnectionFactory("tcp://192.168.56.101:61616");
            connection = connectionFactory.createConnection();
            connection.start();
            session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
//            chatTopic = session.createTopic(TopicName.CHAT_TOPIC);
            joinTopic = session.createTopic(TopicName.JOIN_TOPIC);
//            pingTopic = session.createTopic(TopicName.PING_TOPIC);
//            critRequestTopic = session.createTopic(TopicName.CRIT_TOPIC);


//            topicProducerMap.put(TopicName.CHAT_TOPIC, session.createProducer(chatTopic));
            topicProducerMap.put(TopicName.JOIN_TOPIC, session.createProducer(joinTopic));
//            topicProducerMap.put(TopicName.PING_TOPIC, session.createProducer(pingTopic));
//            topicProducerMap.put(TopicName.CRIT_TOPIC, session.createProducer(critRequestTopic));

            MessageConsumer joinConsumer = session.createConsumer(joinTopic);
            topicConsumerMap.put(TopicName.JOIN_TOPIC, joinConsumer);
            JoinMessageListener joinListener = new JoinMessageListener(this);
            joinConsumer.setMessageListener(joinListener);
            sendJoinMessage("");
            Thread.sleep(1000);
            if (!joinListener.isMessageReceived()) {
                // Set nodeId to 0
                System.out.println("No messages received. Setting nodeId to 1. I am first.");
                nodeId = 1;
                actualCount = 1;
                // Your logic to set nodeId to 0
            }
        } catch (JMSException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    public static void main(String[] args) {
        try {
            Node thisNode = new Node(args);
        }catch (Exception e){
            e.printStackTrace();
        }

    }

    public void sendJoinMessage(String name){
        try {
//            CustomMessage message = new CustomMessage();
            TextMessage message = null;
            if(nodeId==-1) {
                message = session.createTextMessage("i am join"+nodeName);
            }else{
                message = session.createTextMessage("SETID:"+actualCount+":"+name);
            }
//            ObjectMessage m = session.createObjectMessage(message);
            topicProducerMap.get(TopicName.JOIN_TOPIC).send(message);
        }catch (JMSException e){
            e.printStackTrace();
        }
    }
}
