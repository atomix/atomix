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
package io.atomix.manager.resource.internal;

import io.atomix.copycat.Query;
import io.atomix.resource.Resource;

/**
 * Instance-level resource query.
 * <p>
 * Instance queries are submitted by {@link Resource} instances to a specific state machine
 * in the Atomix cluster. The query {@link #resource()} identifies the state machine to which
 * the query is being submitted.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public final class InstanceQuery<T extends Query<U>, U> extends InstanceOperation<T, U> implements Query<U> {

  public InstanceQuery() {
  }

  public InstanceQuery(long resource, T query) {
    super(resource, query);
  }

  @Override
  public ConsistencyLevel consistency() {
    return operation.consistency();
  }

  @Override
  public String toString() {
    return String.format("%s[resource=%d, query=%s, consistency=%s]", getClass().getSimpleName(), resource, operation, consistency());
  }

}
