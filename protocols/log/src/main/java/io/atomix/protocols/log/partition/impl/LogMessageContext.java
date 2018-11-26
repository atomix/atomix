/*
 * Copyright 2017-present Open Networking Foundation
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
package io.atomix.protocols.log.partition.impl;

/**
 * Protocol message context.
 */
class LogMessageContext {
  final String appendSubject;
  final String consumeSubject;
  final String resetSubject;
  final String backupSubject;

  LogMessageContext(String prefix) {
    this.appendSubject = getSubject(prefix, "append");
    this.consumeSubject = getSubject(prefix, "read");
    this.resetSubject = getSubject(prefix, "reset");
    this.backupSubject = getSubject(prefix, "backup");
  }

  private static String getSubject(String prefix, String type) {
    return String.format("%s-%s", prefix, type);
  }
}