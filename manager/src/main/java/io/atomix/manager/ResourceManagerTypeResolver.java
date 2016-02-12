/*
 * Copyright 2016 the original author or authors.
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
package io.atomix.manager;

import io.atomix.catalyst.serializer.SerializableTypeResolver;
import io.atomix.catalyst.serializer.SerializerRegistry;
import io.atomix.manager.state.*;
import io.atomix.resource.*;

/**
 * Resource manager serializable type resolver.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
public class ResourceManagerTypeResolver implements SerializableTypeResolver {
  private final ResourceRegistry resourceRegistry;

  public ResourceManagerTypeResolver(ResourceRegistry resourceRegistry) {
    this.resourceRegistry = resourceRegistry;
  }

  @Override
  public void resolve(SerializerRegistry registry) {
    // Register instance types.
    registry.resolve(new InstanceTypeResolver());

    // Register resource state machine types.
    registry.register(ResourceCommand.class, -50);
    registry.register(ResourceQuery.class, -51);
    registry.register(ResourceStateMachine.ConfigureCommand.class, -52);
    registry.register(ResourceStateMachine.DeleteCommand.class, -53);

    // Register resource types.
    for (ResourceType type : resourceRegistry.types()) {
      try {
        registry.resolve(type.typeResolver().newInstance());
      } catch (InstantiationException | IllegalAccessException e) {
        throw new ResourceException("failed to instantiate resource type resolver");
      }
    }

    // Register resource manager types.
    registry.register(GetResource.class, -58);
    registry.register(GetResourceIfExists.class, -59);
    registry.register(GetResourceKeys.class, -60);
    registry.register(ResourceExists.class, -61);
    registry.register(CloseResource.class, -62);
    registry.register(DeleteResource.class, -63);
  }

}
