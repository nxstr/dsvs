package cz.cvut.fel.dsv;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageListener;
import javax.jms.TextMessage;
import java.util.List;
import java.util.Objects;

public class CritMessageListener implements MessageListener {
    private static final Logger logger = LogManager.getLogger(CritMessageListener.class);
    private final Node node;
    private String senderName;
    private Integer senderId;
    private Integer senderLogicalTime;
    private Integer requestLogicalTime;
    private String messageText;

    private static Integer positiveReplies;
    private static boolean isDenied;

    public CritMessageListener(Node node) {
        this.node = node;
        positiveReplies = 0;
        isDenied = false;
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
        if(node.isNodesLost()){
            positiveReplies=0;
            node.setNodesLost(false);
        }
        if(!Objects.equals(senderName, node.getNodeName())) {
            if (messageText.startsWith("ENTER")) {
                requestLogicalTime = Integer.parseInt(messageText.split(":")[2]);
                node.processEnterCritRequest(senderName, senderId, requestLogicalTime);
                logger.info("CRIT_TOPIC: Received message from node " + senderName + "|" +senderId + "|" + senderLogicalTime + ". Message: "+ messageText);
                logger.info("Node " + node.getNodeName() + " logic time: "+node.getNodeLogicalTime());
            } else if(messageText.startsWith("REPLY")){
                if(messageText.split(":")[1].equals("OK")){
                    if(messageText.split(":")[2].equals(node.getNodeName())){
                        logger.info("CRIT_TOPIC: Received message from node " + senderName + "|" +senderId + "|" + senderLogicalTime + ". Message: "+ messageText);
                        logger.info("Node " + node.getNodeName() + " logic time: "+node.getNodeLogicalTime());
                        positiveReplies++;
                        System.out.println(positiveReplies + " counts " + node.getCurrentNodeCount());
                        if(!isDenied && positiveReplies.equals(node.getCurrentNodeCount()-1)){
                            node.enterCritSection();
                            isDenied = false;
                            positiveReplies = 0;
                        }
                    }
                } else if (messageText.split(":")[1].equals("LOCKED")) {
                    if(messageText.split(":")[2].equals(node.getNodeName())){
                        logger.info("CRIT_TOPIC: Received message from node " + senderName + "|" +senderId + "|" + senderLogicalTime + ". Message: "+ messageText);
                        logger.info("Node " + node.getNodeName() + " logic time: "+node.getNodeLogicalTime());
                        logger.info("Critical section is locked by "+senderName);
                        isDenied = true;
                        positiveReplies = 0;
                    }
                } else if (messageText.split(":")[1].equals("NO")) {
                    if(messageText.split(":")[4].equals(node.getNodeName())){
                        logger.info("CRIT_TOPIC: Received message from node " + senderName + "|" +senderId + "|" + senderLogicalTime + ". Message: "+ messageText);
                        logger.info("Node " + node.getNodeName() + " logic time: "+node.getNodeLogicalTime());
                        isDenied = true;
                        positiveReplies = 0;
                        logger.info("There is earlier request from node " + senderName + ". It has RLT " + messageText.split(":")[3]);
                    }
                }
            } else if (messageText.startsWith("LEAVE")) {
                logger.info("CRIT_TOPIC: Received message from node " + senderName + "|" +senderId + "|" + senderLogicalTime + ". Message: "+ messageText);
                logger.info("Node " + node.getNodeName() + " logic time: "+node.getNodeLogicalTime());
                if(node.isCritRequested()){
                    isDenied = false;
                    positiveReplies = 0;
                    node.sendEnterCritMessage();
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
