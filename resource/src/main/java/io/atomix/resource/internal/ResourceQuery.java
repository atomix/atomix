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
 * limitations under the License
 */
package io.atomix.resource.internal;

import io.atomix.catalyst.buffer.BufferInput;
import io.atomix.catalyst.buffer.BufferOutput;
import io.atomix.catalyst.serializer.CatalystSerializable;
import io.atomix.catalyst.serializer.Serializer;
import io.atomix.copycat.Query;

import java.util.Properties;

/**
 * Wrapper for resource queries.
 * <p>
 * This class wraps {@link Query queries} submitted to the Atomix cluster on behalf of a resource.
 * Wrapping the query allows Atomix to control the query {@link io.atomix.copycat.Query.ConsistencyLevel}
 * based on the user resource configuration.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public final class ResourceQuery<T extends Query<U>, U> extends ResourceOperation<T, U> implements Query<U> {
  public ResourceQuery() {
  }

  public ResourceQuery(T query) {
    super(query);
  }

  @Override
  public ConsistencyLevel consistency() {
    return operation.consistency();
  }

  @Override
  public String toString() {
    return String.format("%s[query=%s]", getClass().getSimpleName(), operation);
  }

  /**
   * Resource configuration query.
   */
  public static class Config implements Query<Properties>, CatalystSerializable {
    public Config() {
    }

    @Override
    public void writeObject(BufferOutput<?> buffer, Serializer serializer) {
    }

    @Override
    public void readObject(BufferInput<?> buffer, Serializer serializer) {
    }
  }

}
