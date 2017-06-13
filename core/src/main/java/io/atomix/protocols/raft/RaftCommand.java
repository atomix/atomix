/*
 * Copyright 2015 the original author or authors.
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
 * limitations under the License.
 */
package io.atomix.protocols.raft;

/**
 * Base interface for operations that modify system state.
 * <p>
 * Commands are submitted by clients to a Raft server and used to modify Raft cluster-wide state. When a command
 * is submitted to the cluster, if the command is received by a follower, the Raft protocol dictates that it must be
 * forwarded to the cluster leader. Once the leader receives a command, it logs and replicates the command to a majority
 * of the cluster before applying it to its state machine and responding with the result.
 *
 * @param <T> command result type
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public interface RaftCommand<T> extends RaftOperation<T> {
}
