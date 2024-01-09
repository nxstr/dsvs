package cz.cvut.fel.dsv;

import lombok.Getter;

@Getter
public class RequestDetails {
    private final Integer nodeId;
    private final Integer nodeLogicalTime;

    public RequestDetails(Integer nodeId, Integer nodeLogicalTime) {
        this.nodeId = nodeId;
        this.nodeLogicalTime = nodeLogicalTime;
    }
}
