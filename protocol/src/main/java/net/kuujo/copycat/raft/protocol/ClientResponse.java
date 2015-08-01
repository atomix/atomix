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
package net.kuujo.copycat.raft.protocol;

import net.kuujo.copycat.util.BuilderPool;
import net.kuujo.copycat.util.ReferenceFactory;
import net.kuujo.copycat.util.ReferenceManager;

/**
 * Client response.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public abstract class ClientResponse<T extends ClientResponse<T>> extends AbstractResponse<T> {

  public ClientResponse(ReferenceManager<T> referenceManager) {
    super(referenceManager);
  }

  /**
   * Client response builder.
   */
  public static abstract class Builder<T extends Builder<T, U>, U extends ClientResponse<U>> extends AbstractResponse.Builder<T, U> {
    protected Builder(BuilderPool<T, U> pool, ReferenceFactory<U> factory) {
      super(pool, factory);
    }
  }

}
