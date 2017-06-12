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
package io.atomix.protocol.phiaccrual;

import io.atomix.protocol.Protocol;

import java.util.function.Consumer;

/**
 * Phi accrual failure detection protocol.
 */
public interface PhiAccrualFailureDetectorProtocol extends Protocol {
    
    /**
     * Handles a heartbeat message.
     *
     * @param message the heartbeat message to handle
     */
    void handleHeartbeat(HeartbeatMessage message);
    
    /**
     * Registers a consumer for heartbeat messages.
     *
     * @param message the heartbeat message consumer
     */
    void onHeartbeat(Consumer<HeartbeatMessage> message);

}
