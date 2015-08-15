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

import net.kuujo.copycat.util.BuilderPool;
import net.kuujo.copycat.raft.protocol.Command;
import net.kuujo.copycat.raft.protocol.Operation;
import net.kuujo.copycat.io.serializer.SerializeWith;

/**
 * Delete getPath command.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
@SerializeWith(id=411)
public class DeletePath extends PathOperation<Boolean> implements Command<Boolean> {

  /**
   * Returns a new DeletePath builder.
   *
   * @return A new DeletePath command builder.
   */
  public static Builder builder() {
    return Operation.builder(DeletePath.Builder.class, DeletePath.Builder::new);
  }

  public DeletePath() {
  }

  public DeletePath(String path) {
    super(path);
  }

  /**
   * Create path builder.
   */
  public static class Builder extends PathOperation.Builder<Builder, DeletePath, Boolean> {
    public Builder(BuilderPool<Builder, DeletePath> pool) {
      super(pool);
    }

    @Override
    protected DeletePath create() {
      return new DeletePath();
    }
  }

}
