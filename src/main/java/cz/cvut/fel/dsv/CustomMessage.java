package cz.cvut.fel.dsv;

import lombok.Getter;
import lombok.Setter;

import java.io.Serializable;

public class CustomMessage implements Serializable {
    @Getter
    @Setter
    private String text;
    @Getter
    @Setter
    private Integer senderId;
    @Getter
    @Setter
    private Integer senderLogicalTime;

    public CustomMessage(String text, Integer senderId, Integer senderLogicalTime) {
        this.text = text;
        this.senderId = senderId;
        this.senderLogicalTime = senderLogicalTime;
    }

    public CustomMessage() {
    }
}
