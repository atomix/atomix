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
 * limitations under the License
 */
package io.atomix.resource;

import io.atomix.copycat.client.CopycatClient;

import java.util.concurrent.CompletableFuture;

/**
 * Base class for configurable resources.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
public abstract class ConfigurableResource<T extends ConfigurableResource<T, U>, U extends Resource.Config> extends Resource<T> implements Configurable<T, U> {

  public ConfigurableResource(CopycatClient client) {
    super(client);
  }

  @Override
  @SuppressWarnings("unchecked")
  public CompletableFuture<T> configure(U config) {
    return client.submit(new ResourceStateMachine.ConfigureCommand(config)).thenApply(v -> (T) this);
  }

}
