/*
 * Copyright 2014 the original author or authors.
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
package net.kuujo.copycat.internal;

import net.kuujo.copycat.CopycatState;
import net.kuujo.copycat.DiscreteResource;
import net.kuujo.copycat.ResourceContext;
import net.kuujo.copycat.ResourcePartitionContext;
import net.kuujo.copycat.cluster.Cluster;

/**
 * Abstract discrete resource implementation.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public abstract class AbstractDiscreteResource<T extends DiscreteResource> extends AbstractResource<T> implements DiscreteResource {
  protected final ResourcePartitionContext partition;

  protected AbstractDiscreteResource(ResourceContext context) {
    super(context);
    this.partition = context.partitions().iterator().next();
  }

  @Override
  public Cluster cluster() {
    return partition.cluster();
  }

  @Override
  public CopycatState state() {
    return partition.state();
  }

}
