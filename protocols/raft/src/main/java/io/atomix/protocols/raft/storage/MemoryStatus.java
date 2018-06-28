/*
 * Copyright 2018-present Open Networking Foundation
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
package io.atomix.protocols.raft.storage;

/**
 * Provides an interface for submitting memory status to the Raft cluster.
 */
public interface MemoryStatus {

    /**
     * Returns a boolean indicating whether the node is running out of memory.
     *
     * @return true if the node is running out of memory.
     */
    boolean isRunningOutOfMemory();

}
