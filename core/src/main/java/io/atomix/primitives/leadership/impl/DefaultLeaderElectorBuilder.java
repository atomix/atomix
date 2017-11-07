/*
 * Copyright 2016-present Open Networking Foundation
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
package io.atomix.primitives.leadership.impl;

import io.atomix.primitives.DistributedPrimitiveCreator;
import io.atomix.primitives.leadership.AsyncLeaderElector;
import io.atomix.primitives.leadership.LeaderElectorBuilder;

import java.util.concurrent.TimeUnit;

/**
 * Default implementation of {@code LeaderElectorBuilder}.
 */
public class DefaultLeaderElectorBuilder<T> extends LeaderElectorBuilder<T> {

  private final DistributedPrimitiveCreator primitiveCreator;

  public DefaultLeaderElectorBuilder(DistributedPrimitiveCreator primitiveCreator) {
    this.primitiveCreator = primitiveCreator;
  }

  @Override
  public AsyncLeaderElector<T> buildAsync() {
    return primitiveCreator.newAsyncLeaderElector(name(), electionTimeoutMillis(), TimeUnit.MILLISECONDS);
  }
}
