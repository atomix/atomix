/*
 * Copyright 2018-present Open Networking Foundation
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
package io.atomix.cluster.messaging;

import io.atomix.utils.config.Config;

import java.time.Duration;

/**
 * Messaging configuration.
 */
public class MessagingConfig implements Config {
  private Duration connectTimeout = Duration.ofSeconds(1);
  private TlsConfig tlsConfig = new TlsConfig();

  /**
   * Returns the Netty connection timeout.
   *
   * @return the Netty connection timeout
   */
  public Duration getConnectTimeout() {
    return connectTimeout;
  }

  /**
   * Sets the Netty connection timeout.
   *
   * @param connectTimeout the Netty connection timeout
   * @return the messaging configuration
   */
  public MessagingConfig setConnectTimeout(Duration connectTimeout) {
    this.connectTimeout = connectTimeout;
    return this;
  }

  /**
   * Returns the TLS configuration.
   *
   * @return the TLS configuration
   */
  public TlsConfig getTlsConfig() {
    return tlsConfig;
  }

  /**
   * Sets the TLS configuration.
   *
   * @param tlsConfig the TLS configuration
   * @return the messaging configuration
   */
  public MessagingConfig setTlsConfig(TlsConfig tlsConfig) {
    this.tlsConfig = tlsConfig;
    return this;
  }
}
