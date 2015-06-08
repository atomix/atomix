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
package net.kuujo.copycat;

import net.kuujo.copycat.raft.ManagedProtocol;
import net.kuujo.copycat.raft.RaftClient;

/**
 * Copycat client.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class CopycatClient extends Copycat {

  /**
   * Returns a new Copycat client builder.
   *
   * @return A new Copycat client builder.
   */
  public static Builder builder() {
    return new Builder();
  }

  public CopycatClient(ManagedProtocol protocol, ClassLoader classLoader) {
    super(protocol, classLoader);
  }

  /**
   * Client builder.
   */
  public static class Builder implements Copycat.Builder<CopycatClient> {
    private final RaftClient.Builder builder = RaftClient.builder();
    private ClassLoader classLoader = Thread.currentThread().getContextClassLoader();

    private Builder() {
    }

    /**
     * Sets the Copycat class loader.
     *
     * @param classLoader The Copycat class loader.
     * @return The Copycat builder.
     */
    public Builder withClassLoader(ClassLoader classLoader) {
      this.classLoader = classLoader;
      return this;
    }

    @Override
    public CopycatClient build() {
      return new CopycatClient(builder.build(), classLoader);
    }
  }

}
