/*
 * Copyright 2016-present Open Networking Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.atomix.core.tree;

/**
 * An exception to be thrown when a node cannot be removed normally because
 * it does not exist or because it is not a leaf node.
 */
public class IllegalDocumentModificationException extends DocumentException {
  public IllegalDocumentModificationException() {
  }

  public IllegalDocumentModificationException(String message) {
    super(message);
  }

  public IllegalDocumentModificationException(String message,
                                              Throwable cause) {
    super(message, cause);
  }

  public IllegalDocumentModificationException(Throwable cause) {
    super(cause);
  }
}
