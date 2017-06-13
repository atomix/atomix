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
package io.atomix.protocols.raft.protocol;

import io.atomix.cluster.MessageSubject;

/**
 * Base interface for requests.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public interface RaftRequest {

    /**
     * Request type.
     */
    enum Type {
        CONNECT("connect"),
        OPEN_SESSION("open-session"),
        CLOSE_SESSION("close-session"),
        KEEP_ALIVE("keep-alive"),
        COMMAND("command"),
        QUERY("query"),
        PUBLISH("publish"),
        RESET("reset"),
        METADATA("metadata");

        private final String friendlyName;

        Type(String friendlyName) {
            this.friendlyName = friendlyName;
        }

        /**
         * Returns the friendly name for the request type.
         *
         * @return the friendly name for the request type
         */
        public String friendlyName() {
            return friendlyName;
        }

        /**
         * Returns the message subject for the given cluster.
         *
         * @param cluster the cluster for which to return the message subject
         * @return the message subject for the given cluster
         */
        public MessageSubject subject(String cluster) {
            return new MessageSubject(String.format("%s-%s", cluster, friendlyName));
        }
    }

    /**
     * Returns the request type.
     *
     * @return the request type
     */
    Type type();

    /**
     * Request builder.
     *
     * @param <T> The builder type.
     */
    interface Builder<T extends Builder<T, U>, U extends RaftRequest> extends io.atomix.util.Builder<U> {
    }

}
