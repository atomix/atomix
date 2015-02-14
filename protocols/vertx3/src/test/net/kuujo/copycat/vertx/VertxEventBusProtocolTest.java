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
package net.kuujo.copycat.vertx;

import io.vertx.core.Vertx;
import net.kuujo.copycat.protocol.Protocol;
import net.kuujo.copycat.test.ProtocolTest;
import org.testng.annotations.Test;

/**
 * Vert.x 3 event bus protocol test.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
@Test
public class VertxEventBusProtocolTest extends ProtocolTest {
  private Vertx vertx = Vertx.vertx();

  @Override
  protected Protocol createProtocol() {
    return new VertxEventBusProtocol(vertx);
  }

  @Override
  protected String createUri(int id) {
    return String.format("eventbus://test%d", id);
  }

}
