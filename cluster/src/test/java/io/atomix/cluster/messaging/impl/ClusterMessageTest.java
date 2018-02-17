package io.atomix.cluster.messaging.impl;

import io.atomix.cluster.NodeId;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class ClusterMessageTest {

    private ClusterMessage clusterMessage;

    @Before
    public void setUp() throws Exception {
        clusterMessage = new ClusterMessage(NodeId.from("node1"), "subject", new byte[]{1, 3, 3, 7});
    }

    @Test
    public void testMessageFromBytes() throws Exception {
        ClusterMessage message = ClusterMessage.fromBytes(clusterMessage.getBytes());
        Assert.assertArrayEquals(clusterMessage.payload(), message.payload());
        Assert.assertEquals(clusterMessage.subject(), message.subject());
        Assert.assertEquals(clusterMessage.sender(), message.sender());
    }

    @Test
    public void testPayloadFromBytes() throws Exception {
        Assert.assertArrayEquals(clusterMessage.payload(), ClusterMessage.getPayload(clusterMessage.getBytes()));
    }
}
