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
package net.kuujo.copycat.util;

import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import net.kuujo.alleycat.Alleycat;

/**
 * Netty context factory.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class NettyContextFactory implements ContextFactory {
  private final EventLoopGroup group = new NioEventLoopGroup(Runtime.getRuntime().availableProcessors() * 2, new CopycatThreadFactory("copycat-thread-%d"));

  @Override
  public Context createContext(String name, Alleycat serializer) {
    return new NettyContext(name, group.next(), serializer);
  }

}
