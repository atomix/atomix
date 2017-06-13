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
package io.atomix.protocols.raft.protocol;

import java.util.concurrent.Executor;
import java.util.function.Consumer;

/**
 * Raft client protocol listener.
 */
public interface RaftClientProtocolListener {

    /**
     * Registers a publish request listener.
     *
     * @param sessionId the session for which to listen for the publish request
     * @param listener the listener to register
     * @param executor the executor with which to execute the listener callback
     */
    void registerPublishListener(long sessionId, Consumer<PublishRequest> listener, Executor executor);

    /**
     * Unregisters the publish request listener for the given session.
     *
     * @param sessionId the session for which to unregister the listener
     */
    void unregisterPublishListener(long sessionId);

}
