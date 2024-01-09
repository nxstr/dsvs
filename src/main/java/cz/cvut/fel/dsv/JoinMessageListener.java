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
//        if (message instanceof ObjectMessage) {
//            try {
//                CustomMessage customMessage = (CustomMessage) ((ObjectMessage) message).getObject();
//                processMessage(customMessage);
//                messageReceived = true;
//            } catch (JMSException e) {
//                e.printStackTrace();
//            }
//        }
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
            logger.info("JOIN_TOPIC: Received message from node " + senderName + "|" +senderId+"|"+senderLogicalTime + ". Message: "+ messageText);
            if (messageText.startsWith("JOIN")) {
                    node.setActualCount(node.getActualCount() + 1);
                    node.setCurrentNodeCount(node.getCurrentNodeCount()+1);
                System.out.println("Set current count " + node.getCurrentNodeCount());
                    node.sendJoinMessage(senderName);
            } else if (messageText.startsWith("LOGOUT")) {
                node.setCurrentNodeCount(node.getCurrentNodeCount()-1);
            } else {
                if (messageText.startsWith("SETID:")) {
                    if (Objects.equals(messageText.split(":")[2], node.getNodeName())){
                        logicalTimeValues.add(senderLogicalTime);
                        if(!Objects.equals(messageText.split(":")[1], node.getNodeId().toString())) {
                            node.setNodeId(Integer.parseInt(messageText.split(":")[1]));
                            node.setActualCount(node.getNodeId());
                            node.setCurrentNodeCount(Integer.parseInt(messageText.split(":")[3]));
                            logger.info("Setting new nodeId = " + node.getNodeId());
                            messageReceived = true;
                        }
                        if(logicalTimeValues.size()==node.getCurrentNodeCount()-1){
                            double d = logicalTimeValues.stream()
                                    .mapToDouble(a -> a)
                                    .average().orElse(0);
                            node.setNodeLogicalTime((int) d);
                            logicalTimeValues.clear();
                        }
                    }
                } else {
                    throw new JMSException("message not recognized");
                }
            }
//            logger.info("This node id is "+node.getNodeId()+node.getNodeName());
        }

    }

    private void convertMessage(TextMessage message) throws JMSException {
        senderName = message.getText().split("\\|")[0];
        senderId = Integer.parseInt(message.getText().split("\\|")[1]);
        senderLogicalTime = Integer.parseInt(message.getText().split("\\|")[2]);
        messageText = message.getText().split("\\|")[3];
    }

}
