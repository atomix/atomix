/*
 * Copyright 2014-present Open Networking Laboratory
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
package org.onosproject.store.cluster.messaging;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.Objects;

/**
 * Representation of a message subject.
 * Cluster messages have associated subjects that dictate how they get handled
 * on the receiving side.
 */
public final class MessageSubject {

    private final String value;

    public MessageSubject(String value) {
        this.value = checkNotNull(value);
    }

    public String value() {
        return value;
    }

    @Override
    public String toString() {
        return value;
    }

    @Override
    public int hashCode() {
        return value.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        MessageSubject that = (MessageSubject) obj;
        return Objects.equals(this.value, that.value);
    }

    // for serializer
    protected MessageSubject() {
        this.value = "";
    }
}
