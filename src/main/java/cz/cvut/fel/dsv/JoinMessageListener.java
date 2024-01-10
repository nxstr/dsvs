package cz.cvut.fel.dsv;

import lombok.Getter;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import javax.jms.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class JoinMessageListener implements MessageListener {

    private static final Logger logger = LogManager.getLogger(JoinMessageListener.class);

    private final Node node;
    @Getter
    private volatile boolean messageReceived = false;
    private String senderName;
    private Integer senderId;
    private Integer senderLogicalTime;
    private String messageText;

    private List<Integer> logicalTimeValues = new ArrayList<>();

    public JoinMessageListener(Node node) {
        this.node = node;
    }

    @Override
    public void onMessage(Message message) {
        if (message instanceof TextMessage) {
            try {
                convertMessage((TextMessage) message);
                processMessage();
            } catch (JMSException e) {
                e.printStackTrace();
            }
        }
    }

    public void processMessage() throws JMSException {
        if(!Objects.equals(senderName, node.getNodeName())) {
            logger.info("Node " + node.getNodeName() + ": " +"JOIN_TOPIC: Received message from node " + senderName + "|" +senderId+"|"+senderLogicalTime + ". Message: "+ messageText);
            if (messageText.startsWith("JOIN")) {
                if(node.getNodeId()!=-1) {
                    node.setActualCount(node.getActualCount() + 1);
                    node.setCurrentNodeCount(node.getCurrentNodeCount() + 1);
                    logger.info("Node " + node.getNodeName() + ": " +"Set current node count to " + node.getCurrentNodeCount());
                    node.sendJoinMessage(senderName);
                }
            } else {
                if (messageText.startsWith("SETID:")) {
                    if (Objects.equals(messageText.split(":")[2], node.getNodeName())){
                        logicalTimeValues.add(senderLogicalTime);
                        if(!Objects.equals(messageText.split(":")[1], node.getNodeId().toString())) {
                            if(node.getNodeId()==-1) {
                                node.setNodeId(Integer.parseInt(messageText.split(":")[1]));
                                node.setActualCount(node.getNodeId());
                                node.setCurrentNodeCount(Integer.parseInt(messageText.split(":")[3]));
                                logger.info("Node " + node.getNodeName() + ": " +"Setting new nodeId = " + node.getNodeId());
                                messageReceived = true;
                            } else if (node.getNodeId()!=-1) {
                                node.setActualCount(node.getActualCount()+1);
                                node.setCurrentNodeCount(Integer.parseInt(messageText.split(":")[3]));
                                node.setNodeId(node.getNodeId()+1);
                                node.sendActualizeMessage(senderName);
                                logger.info("Node " + node.getNodeName() + ": " +"I updated data: id = "+node.getNodeId()+", actualCount = "+node.getActualCount()+", nodeCurrentCount = "+node.getCurrentNodeCount());
                            }
                        }
                        node.updateTime(senderName);
                        if(logicalTimeValues.size()==node.getCurrentNodeCount()-1){
                            double d = logicalTimeValues.stream()
                                    .mapToDouble(a -> a)
                                    .average().orElse(0);
                            node.setNodeLogicalTime((int) d);
                            logicalTimeValues.clear();
                        }
                    }
                } else if (messageText.startsWith("ACTUALIZE:")) {
                    if(!Objects.equals(senderName, node.getNodeName())){
                        node.setActualCount(Integer.parseInt(messageText.split(":")[1]));
                        node.setCurrentNodeCount(Integer.parseInt(messageText.split(":")[3]));
                        node.updateTime(senderName);
                        logger.info("Node " + node.getNodeName() + ": " +"It seems that network has problems, node's data werent actual. Actualizing... actualCount = " + node.getActualCount() + ", currentNodeCount = " + node.getCurrentNodeCount());
                    }
                } else {
                    throw new JMSException("message not recognized");
                }
            }
        }

    }

    private void convertMessage(TextMessage message) throws JMSException {
        senderName = message.getText().split("\\|")[0];
        senderId = Integer.parseInt(message.getText().split("\\|")[1]);
        senderLogicalTime = Integer.parseInt(message.getText().split("\\|")[2]);
        messageText = message.getText().split("\\|")[3];
    }

}
