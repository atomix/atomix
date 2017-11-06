/*
 * Copyright 2016-present Open Networking Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.atomix.elector.impl;

import io.atomix.cluster.NodeId;
import io.atomix.leadership.Leadership;
import io.atomix.primitives.elector.LeaderElectionEvent;
import io.atomix.primitives.elector.LeaderElectorEventListener;
import io.atomix.primitives.elector.impl.RaftLeaderElector;
import io.atomix.primitives.elector.impl.RaftLeaderElectorService;
import io.atomix.primitives.impl.AbstractRaftPrimitiveTest;
import io.atomix.protocols.raft.proxy.RaftProxy;
import io.atomix.protocols.raft.service.RaftService;
import org.junit.Test;

import java.util.LinkedList;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Unit tests for {@link RaftLeaderElector}.
 */
public class RaftLeaderElectorTest extends AbstractRaftPrimitiveTest<RaftLeaderElector> {

    NodeId node1 = new NodeId("node1");
    NodeId node2 = new NodeId("node2");
    NodeId node3 = new NodeId("node3");

    @Override
    protected RaftService createService() {
        return new RaftLeaderElectorService();
    }

    @Override
    protected RaftLeaderElector createPrimitive(RaftProxy proxy) {
        return new RaftLeaderElector(proxy);
    }

    @Test
    public void testRun() throws Throwable {
        leaderElectorRunTests();
    }

    private void leaderElectorRunTests() throws Throwable {
        RaftLeaderElector elector1 = newPrimitive("test-elector-run");
        elector1.run("foo", node1).thenAccept(result -> {
            assertEquals(node1, result.leaderNodeId());
            assertEquals(1, result.leader().term());
            assertEquals(1, result.candidates().size());
            assertEquals(node1, result.candidates().get(0));
        }).join();

        RaftLeaderElector elector2 = newPrimitive("test-elector-run");
        elector2.run("foo", node2).thenAccept(result -> {
            assertEquals(node1, result.leaderNodeId());
            assertEquals(1, result.leader().term());
            assertEquals(2, result.candidates().size());
            assertEquals(node1, result.candidates().get(0));
            assertEquals(node2, result.candidates().get(1));
        }).join();
    }

    @Test
    public void testWithdraw() throws Throwable {
        leaderElectorWithdrawTests();
    }

    private void leaderElectorWithdrawTests() throws Throwable {
        RaftLeaderElector elector1 = newPrimitive("test-elector-withdraw");
        elector1.run("foo", node1).join();
        RaftLeaderElector elector2 = newPrimitive("test-elector-withdraw");
        elector2.run("foo", node2).join();

        LeaderEventListener listener1 = new LeaderEventListener();
        elector1.addListener(listener1).join();

        LeaderEventListener listener2 = new LeaderEventListener();
        elector2.addListener(listener2).join();

        elector1.withdraw("foo").join();

        listener1.nextEvent().thenAccept(result -> {
            assertEquals(node2, result.newLeadership().leaderNodeId());
            assertEquals(2, result.newLeadership().leader().term());
            assertEquals(1, result.newLeadership().candidates().size());
            assertEquals(node2, result.newLeadership().candidates().get(0));
        }).join();

        listener2.nextEvent().thenAccept(result -> {
            assertEquals(node2, result.newLeadership().leaderNodeId());
            assertEquals(2, result.newLeadership().leader().term());
            assertEquals(1, result.newLeadership().candidates().size());
            assertEquals(node2, result.newLeadership().candidates().get(0));
        }).join();

        Leadership leadership1 = elector1.getLeadership("foo").join();
        assertEquals(node2, leadership1.leader().nodeId());
        assertEquals(1, leadership1.candidates().size());

        Leadership leadership2 = elector2.getLeadership("foo").join();
        assertEquals(node2, leadership2.leader().nodeId());
        assertEquals(1, leadership2.candidates().size());
    }

    @Test
    public void testAnoint() throws Throwable {
        leaderElectorAnointTests();
    }

    private void leaderElectorAnointTests() throws Throwable {
        RaftLeaderElector elector1 = newPrimitive("test-elector-anoint");
        RaftLeaderElector elector2 = newPrimitive("test-elector-anoint");
        RaftLeaderElector elector3 = newPrimitive("test-elector-anoint");
        elector1.run("foo", node1).join();
        elector2.run("foo", node2).join();

        LeaderEventListener listener1 = new LeaderEventListener();
        elector1.addListener(listener1).join();
        LeaderEventListener listener2 = new LeaderEventListener();
        elector2.addListener(listener2);
        LeaderEventListener listener3 = new LeaderEventListener();
        elector3.addListener(listener3).join();

        elector3.anoint("foo", node3).thenAccept(result -> {
            assertFalse(result);
        }).join();
        assertFalse(listener1.hasEvent());
        assertFalse(listener2.hasEvent());
        assertFalse(listener3.hasEvent());

        elector3.anoint("foo", node2).thenAccept(result -> {
            assertTrue(result);
        }).join();

        listener1.nextEvent().thenAccept(result -> {
            assertEquals(node2, result.newLeadership().leaderNodeId());
            assertEquals(2, result.newLeadership().candidates().size());
            assertEquals(node1, result.newLeadership().candidates().get(0));
            assertEquals(node2, result.newLeadership().candidates().get(1));
        }).join();
        listener2.nextEvent().thenAccept(result -> {
            assertEquals(node2, result.newLeadership().leaderNodeId());
            assertEquals(2, result.newLeadership().candidates().size());
            assertEquals(node1, result.newLeadership().candidates().get(0));
            assertEquals(node2, result.newLeadership().candidates().get(1));
        }).join();
        listener3.nextEvent().thenAccept(result -> {
            assertEquals(node2, result.newLeadership().leaderNodeId());
            assertEquals(2, result.newLeadership().candidates().size());
            assertEquals(node1, result.newLeadership().candidates().get(0));
            assertEquals(node2, result.newLeadership().candidates().get(1));
        }).join();
    }

    @Test
    public void testPromote() throws Throwable {
        leaderElectorPromoteTests();
    }

    private void leaderElectorPromoteTests() throws Throwable {
        RaftLeaderElector elector1 = newPrimitive("test-elector-promote");
        RaftLeaderElector elector2 = newPrimitive("test-elector-promote");
        RaftLeaderElector elector3 = newPrimitive("test-elector-promote");
        elector1.run("foo", node1).join();
        elector2.run("foo", node2).join();

        LeaderEventListener listener1 = new LeaderEventListener();
        elector1.addListener(listener1).join();
        LeaderEventListener listener2 = new LeaderEventListener();
        elector2.addListener(listener2).join();
        LeaderEventListener listener3 = new LeaderEventListener();
        elector3.addListener(listener3).join();

        elector3.promote("foo", node3).thenAccept(result -> {
            assertFalse(result);
        }).join();

        assertFalse(listener1.hasEvent());
        assertFalse(listener2.hasEvent());
        assertFalse(listener3.hasEvent());

        elector3.run("foo", node3).join();

        listener1.nextEvent().thenAccept(result -> {
            assertEquals(node3, result.newLeadership().candidates().get(2));
        }).join();
        listener2.nextEvent().thenAccept(result -> {
            assertEquals(node3, result.newLeadership().candidates().get(2));
        }).join();
        listener3.nextEvent().thenAccept(result -> {
            assertEquals(node3, result.newLeadership().candidates().get(2));
        }).join();

        elector3.promote("foo", node3).thenAccept(result -> {
            assertTrue(result);
        }).join();

        listener1.nextEvent().thenAccept(result -> {
            assertEquals(node3, result.newLeadership().candidates().get(0));
        }).join();
        listener2.nextEvent().thenAccept(result -> {
            assertEquals(node3, result.newLeadership().candidates().get(0));
        }).join();
        listener3.nextEvent().thenAccept(result -> {
            assertEquals(node3, result.newLeadership().candidates().get(0));
        }).join();
    }

    @Test
    public void testLeaderSessionClose() throws Throwable {
        leaderElectorLeaderSessionCloseTests();
    }

    private void leaderElectorLeaderSessionCloseTests() throws Throwable {
        RaftLeaderElector elector1 = newPrimitive("test-elector-leader-session-close");
        elector1.run("foo", node1).join();
        RaftLeaderElector elector2 = newPrimitive("test-elector-leader-session-close");
        LeaderEventListener listener = new LeaderEventListener();
        elector2.run("foo", node2).join();
        elector2.addListener(listener).join();
        elector1.close();
        listener.nextEvent().thenAccept(result -> {
            assertEquals(node2, result.newLeadership().leaderNodeId());
            assertEquals(1, result.newLeadership().candidates().size());
            assertEquals(node2, result.newLeadership().candidates().get(0));
        }).join();
    }

    @Test
    public void testNonLeaderSessionClose() throws Throwable {
        leaderElectorNonLeaderSessionCloseTests();
    }

    private void leaderElectorNonLeaderSessionCloseTests() throws Throwable {
        RaftLeaderElector elector1 = newPrimitive("test-elector-non-leader-session-close");
        elector1.run("foo", node1).join();
        RaftLeaderElector elector2 = newPrimitive("test-elector-non-leader-session-close");
        LeaderEventListener listener = new LeaderEventListener();
        elector2.run("foo", node2).join();
        elector1.addListener(listener).join();
        elector2.close().join();
        listener.nextEvent().thenAccept(result -> {
            assertEquals(node1, result.newLeadership().leaderNodeId());
            assertEquals(1, result.newLeadership().candidates().size());
            assertEquals(node1, result.newLeadership().candidates().get(0));
        }).join();
    }

    @Test
    public void testQueries() throws Throwable {
        leaderElectorQueryTests();
    }

    private void leaderElectorQueryTests() throws Throwable {
        RaftLeaderElector elector1 = newPrimitive("test-elector-query");
        RaftLeaderElector elector2 = newPrimitive("test-elector-query");
        elector1.run("foo", node1).join();
        elector2.run("foo", node2).join();
        elector2.run("bar", node2).join();
        elector1.getElectedTopics(node1).thenAccept(result -> {
            assertEquals(1, result.size());
            assertTrue(result.contains("foo"));
        }).join();
        elector2.getElectedTopics(node1).thenAccept(result -> {
            assertEquals(1, result.size());
            assertTrue(result.contains("foo"));
        }).join();
        elector1.getLeadership("foo").thenAccept(result -> {
            assertEquals(node1, result.leaderNodeId());
            assertEquals(node1, result.candidates().get(0));
            assertEquals(node2, result.candidates().get(1));
        }).join();
        elector2.getLeadership("foo").thenAccept(result -> {
            assertEquals(node1, result.leaderNodeId());
            assertEquals(node1, result.candidates().get(0));
            assertEquals(node2, result.candidates().get(1));
        }).join();
        elector1.getLeadership("bar").thenAccept(result -> {
            assertEquals(node2, result.leaderNodeId());
            assertEquals(node2, result.candidates().get(0));
        }).join();
        elector2.getLeadership("bar").thenAccept(result -> {
            assertEquals(node2, result.leaderNodeId());
            assertEquals(node2, result.candidates().get(0));
        }).join();
        elector1.getLeaderships().thenAccept(result -> {
            assertEquals(2, result.size());
            Leadership fooLeadership = result.get("foo");
            assertEquals(node1, fooLeadership.leaderNodeId());
            assertEquals(node1, fooLeadership.candidates().get(0));
            assertEquals(node2, fooLeadership.candidates().get(1));
            Leadership barLeadership = result.get("bar");
            assertEquals(node2, barLeadership.leaderNodeId());
            assertEquals(node2, barLeadership.candidates().get(0));
        }).join();
        elector2.getLeaderships().thenAccept(result -> {
            assertEquals(2, result.size());
            Leadership fooLeadership = result.get("foo");
            assertEquals(node1, fooLeadership.leaderNodeId());
            assertEquals(node1, fooLeadership.candidates().get(0));
            assertEquals(node2, fooLeadership.candidates().get(1));
            Leadership barLeadership = result.get("bar");
            assertEquals(node2, barLeadership.leaderNodeId());
            assertEquals(node2, barLeadership.candidates().get(0));
        }).join();
    }

    private static class LeaderEventListener implements LeaderElectorEventListener {
        Queue<LeaderElectionEvent> eventQueue = new LinkedList<>();
        CompletableFuture<LeaderElectionEvent> pendingFuture;

        @Override
        public void onEvent(LeaderElectionEvent event) {
            synchronized (this) {
                if (pendingFuture != null) {
                    pendingFuture.complete(event);
                    pendingFuture = null;
                } else {
                    eventQueue.add(event);
                }
            }
        }

        public boolean hasEvent() {
            return !eventQueue.isEmpty();
        }

        public void clearEvents() {
            eventQueue.clear();
        }

        public CompletableFuture<LeaderElectionEvent> nextEvent() {
            synchronized (this) {
                if (eventQueue.isEmpty()) {
                    if (pendingFuture == null) {
                        pendingFuture = new CompletableFuture<>();
                    }
                    return pendingFuture;
                } else {
                    return CompletableFuture.completedFuture(eventQueue.poll());
                }
            }
        }
    }
}
