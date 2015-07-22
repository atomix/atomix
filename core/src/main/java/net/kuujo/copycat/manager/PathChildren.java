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
package net.kuujo.copycat.manager;

import net.kuujo.alleycat.SerializeWith;
import net.kuujo.copycat.BuilderPool;
import net.kuujo.copycat.raft.ConsistencyLevel;
import net.kuujo.copycat.raft.Operation;
import net.kuujo.copycat.raft.Query;

import java.util.List;

/**
 * Path children command.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
@SerializeWith(id=413)
public class PathChildren extends PathOperation<List<String>> implements Query<List<String>> {

  /**
   * Returns a new PathChildren builder.
   *
   * @return A new PathChildren command builder.
   */
  public static Builder builder() {
    return Operation.builder(PathChildren.Builder.class, PathChildren.Builder::new);
  }

  public PathChildren() {
  }

  public PathChildren(String path) {
    super(path);
  }

  @Override
  public ConsistencyLevel consistency() {
    return ConsistencyLevel.LINEARIZABLE;
  }

  /**
   * Path children builder.
   */
  public static class Builder extends PathOperation.Builder<Builder, PathChildren, List<String>> {
    public Builder(BuilderPool<Builder, PathChildren> pool) {
      super(pool);
    }

    @Override
    protected PathChildren create() {
      return new PathChildren();
    }
  }

}
