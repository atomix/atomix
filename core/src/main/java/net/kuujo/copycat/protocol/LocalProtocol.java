/*
 * Copyright 2014 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package net.kuujo.copycat.protocol;

import java.net.URI;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Local protocol implementation.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class LocalProtocol extends AbstractProtocol {

  public LocalProtocol() {
    super();
    config.computeIfAbsent("registry", key -> new ConcurrentHashMap<>());
  }

  public LocalProtocol(Map<String, Object> config) {
    super(config);
    config.computeIfAbsent("registry", key -> new ConcurrentHashMap<>());
  }

  private LocalProtocol(LocalProtocol protocol) {
    super(protocol);
    config.computeIfAbsent("registry", key -> new ConcurrentHashMap<>());
  }

  @Override
  public LocalProtocol copy() {
    return new LocalProtocol(this);
  }

  @Override
  @SuppressWarnings("unchecked")
  public ProtocolClient createClient(URI uri) {
    return new LocalProtocolClient(uri.getAuthority(), (Map<String, LocalProtocolServer>) config.get("registry"));
  }

  @Override
  @SuppressWarnings("unchecked")
  public ProtocolServer createServer(URI uri) {
    return new LocalProtocolServer(uri.getAuthority(), (Map<String, LocalProtocolServer>) config.get("registry"));
  }

}
