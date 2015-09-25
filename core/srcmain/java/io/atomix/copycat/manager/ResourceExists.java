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
package io.atomix.copycat.manager;

import io.atomix.catalogue.client.Operation;
import io.atomix.catalogue.client.Query;
import io.atomix.catalyst.serializer.SerializeWith;
import io.atomix.catalyst.util.BuilderPool;

/**
 * Resource exists command.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
@SerializeWith(id=417)
public class ResourceExists extends PathOperation<Boolean> implements Query<Boolean> {

  /**
   * Returns a new ResourceExists builder.
   *
   * @return A new ResourceExists command builder.
   */
  public static Builder builder() {
    return Operation.builder(ResourceExists.Builder.class, ResourceExists.Builder::new);
  }

  public ResourceExists() {
  }

  /**
   * @throws NullPointerException if {@code path} is null
   */
  public ResourceExists(String path) {
    super(path);
  }

  @Override
  public ConsistencyLevel consistency() {
    return ConsistencyLevel.LINEARIZABLE;
  }

  /**
   * Resource exists builder.
   */
  public static class Builder extends PathOperation.Builder<Builder, ResourceExists, Boolean> {
    public Builder(BuilderPool<Builder, ResourceExists> pool) {
      super(pool);
    }

    @Override
    protected ResourceExists create() {
      return new ResourceExists();
    }
  }

}
