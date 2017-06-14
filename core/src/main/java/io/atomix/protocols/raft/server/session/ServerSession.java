/*
 * Copyright 2016 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License
 */
package io.atomix.protocols.raft.server.session;

import java.util.function.Consumer;

/**
 * Provides an interface to communicating with a client via session events.
 * <p>
 * Each client that connects to a Raft cluster must open a {@link Session} in order to submit operations to the cluster.
 * When a client first connects to a server, it must register a new session. Once the session has been registered,
 * it can be used to submit {@link Command commands} and {@link Query queries}
 * or {@link #publish(String, Object) publish} session events.
 * <p>
 * Sessions represent a connection between a single client and all servers in a Raft cluster. Session information
 * is replicated via the Raft consensus algorithm, and clients can safely switch connections between servers without
 * losing their session. All consistency guarantees are provided within the context of a session. Once a session is
 * expired or closed, linearizability, sequential consistency, and other guarantees for events and operations are
 * effectively lost. Session implementations guarantee linearizability for session messages by coordinating between
 * the client and a single server at any given time. This means messages {@link #publish(String, Object) published}
 * via the {@link Session} are guaranteed to arrive on the other side of the connection exactly once and in the order
 * in which they are sent by replicated state machines. In the event of a server-to-client message being lost, the
 * message will be resent so long as at least one Raft server is able to communicate with the client and the client's
 * session does not expire while switching between servers.
 * <p>
 * Messages are sent to the other side of the session using the {@link #publish(String, Object)} method:
 * <pre>
 *   {@code
 *     session.publish("myEvent", "Hello world!");
 *   }
 * </pre>
 * When the message is published, it will be queued to be sent to the other side of the connection. Copycat guarantees
 * that the message will eventually be received by the client unless the session itself times out or is closed.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
public interface ServerSession {

    /**
     * Returns the session identifier.
     *
     * @return The session identifier.
     */
    long id();

    /**
     * Returns the session state.
     *
     * @return The session state.
     */
    State state();

    /**
     * Adds a state change listener to the session.
     *
     * @param listener the state change listener to add
     */
    void addStateChangeListener(Consumer<State> listener);

    /**
     * Removes a state change listener from the session.
     *
     * @param listener the state change listener to remove
     */
    void removeStateChangeListener(Consumer<State> listener);

    /**
     * Publishes a {@code null} named event to the session.
     *
     * @param event The event to publish.
     * @throws NullPointerException   If {@code event} is {@code null}
     * @throws io.atomix.protocols.raft.error.UnknownSessionException If the session is closed
     */
    void publish(Object event);

    /**
     * Session state enums.
     */
    enum State {
        OPEN(true),
        SUSPICIOUS(true),
        EXPIRED(false),
        CLOSED(false);

        private final boolean active;

        State(boolean active) {
            this.active = active;
        }

        public boolean active() {
            return active;
        }
    }

}
