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
package io.atomix.variables;

import io.atomix.copycat.client.CopycatClient;
import io.atomix.resource.ResourceTypeInfo;
import io.atomix.variables.util.DistributedValueFactory;

import java.util.Properties;

/**
 * Stores a single replicated value, providing atomic operations for modifying the value.
 * <p>
 * The value resource stores a single value that can be accessed across the Atomix cluster. The value
 * must be of a type that is serializable by the {@link io.atomix.catalyst.serializer.Serializer} on
 * any client that opens the resource and on all replicas in the cluster. Atomic operations like
 * {@link #compareAndSet(Object, Object)} can be used to check and update the state of the value.
 * <pre>
 *   {@code
 *   DistributedValue<String> value = atomix.getValue("foo").get();
 *   value.compareAndSet("foo", "bar").thenAccept(succeeded -> {
 *     ...
 *   });
 *   }
 * </pre>
 * Changes to the state of the value are linearizable and are therefore guaranteed to take place some
 * time between the invocation of a state changing method and the completion of the returned
 * {@link java.util.concurrent.CompletableFuture}.
 * <h3>Implementation</h3>
 * State management for the {@code DistributedValue} resource is implemented as a basic Copycat
 * {@link io.atomix.copycat.server.StateMachine}. Changes to the value are written to a log and replicated
 * to a majority of the cluster before being applied to the state machine. State change are applied to the
 * state machine atomically, and the state machine keeps track of state changes that apply to the current
 * system state. Once a write no longer contributes to the state machine's state, it is released to be
 * removed from the log during compaction.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
@ResourceTypeInfo(id=-1, factory=DistributedValueFactory.class)
public class DistributedValue<T> extends AbstractDistributedValue<DistributedValue<T>, T> {

  public DistributedValue(CopycatClient client, Properties options) {
    super(client, options);
  }

}
