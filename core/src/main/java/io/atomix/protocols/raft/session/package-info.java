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

/**
 * Interfaces for managing client {@link io.atomix.copycat.session.Session sessions} on a {@link io.atomix.copycat.server.CopycatServer Copycat server}.
 * <p>
 * Client session information is exposed to server {@link io.atomix.copycat.server.StateMachine state machines} in the form of a
 * {@link io.atomix.copycat.server.session.ServerSession} object. Server sessions provide functionality in addition to normal session state
 * information to allow replicated state machines to {@link io.atomix.copycat.server.session.ServerSession#publish(String, Object) publish}
 * event notifications to clients through their session.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
package io.atomix.protocols.raft.session;
