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
package io.atomix.protocols.raft.metadata;

import java.util.Objects;

import static com.google.common.base.MoreObjects.toStringHelper;

/**
 * Copycat client metadata.
 */
public class RaftClientMetadata {
    private final long id;

    public RaftClientMetadata(long id) {
        this.id = id;
    }

    /**
     * Returns the client identifier.
     *
     * @return The client identifier.
     */
    public long id() {
        return id;
    }

    @Override
    public int hashCode() {
        return Objects.hash(id);
    }

    @Override
    public boolean equals(Object object) {
        if (object instanceof RaftClientMetadata) {
            RaftClientMetadata metadata = (RaftClientMetadata) object;
            return metadata.id == id;
        }
        return false;
    }

    @Override
    public String toString() {
        return toStringHelper(this)
                .add("id", id)
                .toString();
    }
}
