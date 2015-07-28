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

import net.kuujo.copycat.BuilderPool;
import net.kuujo.copycat.Command;
import net.kuujo.copycat.Operation;
import net.kuujo.copycat.io.serializer.SerializeWith;

/**
 * Create getPath command.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
@SerializeWith(id=410)
public class CreatePath extends PathOperation<Boolean> implements Command<Boolean> {

  /**
   * Returns a new CreatePath builder.
   *
   * @return A new CreatePath command builder.
   */
  public static Builder builder() {
    return Operation.builder(CreatePath.Builder.class, CreatePath.Builder::new);
  }

  public CreatePath() {
  }

  public CreatePath(String path) {
    super(path);
  }

  /**
   * Create path builder.
   */
  public static class Builder extends PathOperation.Builder<Builder, CreatePath, Boolean> {

    public Builder(BuilderPool<Builder, CreatePath> pool) {
      super(pool);
    }

    @Override
    protected CreatePath create() {
      return new CreatePath();
    }
  }

}
