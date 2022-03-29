// SPDX-FileCopyrightText: 2015-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.storage.buffer;

import io.atomix.utils.concurrent.ReferenceFactory;
import io.atomix.utils.concurrent.ReferencePool;

/**
 * Buffer pool.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class BufferPool extends ReferencePool<Buffer> {

  public BufferPool(ReferenceFactory<Buffer> factory) {
    super(factory);
  }

}
