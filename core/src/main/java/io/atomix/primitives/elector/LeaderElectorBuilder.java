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
package io.atomix.primitives.elector;

import io.atomix.primitives.DistributedPrimitive;
import io.atomix.primitives.DistributedPrimitiveBuilder;

/**
 * Builder for constructing new {@link AsyncLeaderElector} instances.
 */
public abstract class LeaderElectorBuilder
    extends DistributedPrimitiveBuilder<LeaderElectorBuilder, AsyncLeaderElector> {
  public LeaderElectorBuilder() {
    super(DistributedPrimitive.Type.LEADER_ELECTOR);
  }
}
