/*
 * Copyright 2015-present Open Networking Laboratory
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
 * Provides error constants and exceptions associated with the Raft consensus protocol.
 * <p>
 * Copycat protocol errors are designed to be transported across networks in the most efficient manner possible. Each protocol exception
 * is associated with a 1-byte identifier. Rather than serializing complete {@link java.lang.Exception} objects, the protocol exception is
 * sent using only its identifier and the exception is recreated by the receiving side of a connection.
 */
package io.atomix.protocols.raft.error;
