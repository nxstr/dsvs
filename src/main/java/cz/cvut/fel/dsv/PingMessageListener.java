package cz.cvut.fel.dsv;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageListener;
import javax.jms.TextMessage;
import java.time.Instant;
import java.util.*;

public class PingMessageListener implements MessageListener {
    private static final Logger logger = LogManager.getLogger(PingMessageListener.class);
    private final Node node;
    private String senderName;

    public PingMessageListener(Node node) {
        this.node = node;
    }

    @Override
    public void onMessage(Message message) {
        if (message instanceof TextMessage) {
            try {
                convertMessage((TextMessage) message);
                if(!Objects.equals(senderName, node.getNodeName())) {
                node.updateTime(senderName);
                }
            } catch (JMSException e) {
                e.printStackTrace();
            }
        }
    }

    private void convertMessage(TextMessage message) throws JMSException {
        senderName = message.getText().split("\\|")[0];
    }
}
