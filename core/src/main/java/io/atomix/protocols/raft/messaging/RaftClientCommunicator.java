/*
 * Copyright 2017-present Open Networking Laboratory
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
package io.atomix.protocols.raft.messaging;

import io.atomix.cluster.ClusterCommunicationService;
import io.atomix.cluster.MessageSubject;
import io.atomix.util.serializer.KryoNamespaces;
import io.atomix.util.serializer.Serializer;

/**
 * Raft client protocol that uses a cluster communicator.
 */
public class RaftClientCommunicator implements RaftClientProtocol {
    private final RaftClientProtocolListener listener;
    private final RaftClientProtocolDispatcher dispatcher;

    public RaftClientCommunicator(ClusterCommunicationService clusterCommunicator) {
        this(null, clusterCommunicator);
    }

    public RaftClientCommunicator(String prefix, ClusterCommunicationService clusterCommunicator) {
        this(prefix, Serializer.using(KryoNamespaces.RAFT), clusterCommunicator);
    }

    public RaftClientCommunicator(String prefix, Serializer serializer, ClusterCommunicationService clusterCommunicator) {
        MessageContext context = new MessageContext(prefix);
        this.listener = new RaftClientMessageListener(context, serializer, clusterCommunicator);
        this.dispatcher = new RaftClientMessageDispatcher(context, serializer, clusterCommunicator);
    }

    @Override
    public RaftClientProtocolListener listener() {
        return listener;
    }

    @Override
    public RaftClientProtocolDispatcher dispatcher() {
        return dispatcher;
    }

    /**
     * Protocol message context.
     */
    static class MessageContext {
        private final String prefix;
        final MessageSubject openSessionSubject;
        final MessageSubject closeSessionSubject;
        final MessageSubject keepAliveSubject;
        final MessageSubject querySubject;
        final MessageSubject commandSubject;
        final MessageSubject metadataSubject;
        final MessageSubject resetSubject;

        private MessageContext(String prefix) {
            this.prefix = prefix;
            this.openSessionSubject = getSubject(prefix, "open");
            this.closeSessionSubject = getSubject(prefix, "close");
            this.keepAliveSubject = getSubject(prefix, "keep-alive");
            this.querySubject = getSubject(prefix, "query");
            this.commandSubject = getSubject(prefix, "command");
            this.metadataSubject = getSubject(prefix, "metadata");
            this.resetSubject = getSubject(prefix, "reset");
        }

        private static MessageSubject getSubject(String prefix, String type) {
            if (prefix == null) {
                return new MessageSubject(type);
            } else {
                return new MessageSubject(String.format("%s-%s", prefix, type));
            }
        }

        MessageSubject publishSubject(long sessionId) {
            if (prefix == null) {
                return new MessageSubject(String.format("publish-%d", sessionId));
            } else {
                return new MessageSubject(String.format("%s-publish-%d", prefix, sessionId));
            }
        }
    }
}
