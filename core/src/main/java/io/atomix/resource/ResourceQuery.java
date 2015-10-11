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
package io.atomix.resource;

import io.atomix.Consistency;
import io.atomix.catalyst.serializer.SerializeWith;
import io.atomix.copycat.client.Query;

/**
 * Resource query.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
@SerializeWith(id=401)
public class ResourceQuery<T extends Query<U>, U> extends ResourceOperation<T, U> implements Query<U> {

  public ResourceQuery() {
  }

  public ResourceQuery(long resource, T query, Consistency consistency) {
    super(resource, query, consistency);
  }

  @Override
  public ConsistencyLevel consistency() {
    ConsistencyLevel consistency = operation.consistency();
    return consistency != null ? consistency : this.consistency.readConsistency();
  }

  @Override
  public String toString() {
    return String.format("%s[resource=%d, query=%s, consistency=%s]", getClass().getSimpleName(), resource, operation, consistency());
  }

}
