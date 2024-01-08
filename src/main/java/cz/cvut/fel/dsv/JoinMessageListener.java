package cz.cvut.fel.dsv;

import lombok.Getter;

import javax.jms.*;
import java.util.Objects;

public class JoinMessageListener implements MessageListener {

    private final Node node;
    @Getter
    private volatile boolean messageReceived = false;

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
                processMessage((TextMessage) message);
            } catch (JMSException e) {
                e.printStackTrace();
            }
        }
    }

    public void processMessage(TextMessage message) throws JMSException {
        if(!Objects.equals(message.getText().split("\\|")[0], node.getNodeName())) {
            System.out.println("Received message: " + message.getText());
            if (message.getText().split("\\|")[2].startsWith("JOIN")) {
                    node.setActualCount(node.getActualCount() + 1);
                    node.sendJoinMessage(message.getText().split("\\|")[0]);
            } else {
                if (message.getText().split("\\|")[2].startsWith("SETID:")) {
                    if (Objects.equals(message.getText().split(":")[2], node.getNodeName()) &&
                            !Objects.equals(message.getText().split(":")[1], node.getNodeId().toString())) {
                        node.setNodeId(Integer.parseInt(message.getText().split(":")[1]));
                        node.setActualCount(node.getNodeId());
                        System.out.println("Setting new nodeId = " + node.getNodeId());
                        messageReceived = true;
                    }
                } else {
                    throw new JMSException("message not recognized");
                }
            }
            System.out.println("this node id is "+node.getNodeId()+node.getNodeName());
        }

    }

}
