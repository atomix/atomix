// SPDX-FileCopyrightText: 2017-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.rest;

import io.atomix.core.Atomix;
import io.atomix.utils.net.Address;
import io.atomix.rest.impl.VertxRestService;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Atomix REST service.
 */
public interface RestService {

  /**
   * Returns a new REST service builder.
   *
   * @return a new REST service builder
   */
  static Builder builder() {
    return new VertxRestService.Builder();
  }

  /**
   * Returns the REST service address.
   *
   * @return the REST service address
   */
  Address address();

  /**
   * REST service builder.
   */
  abstract class Builder implements io.atomix.utils.Builder<ManagedRestService> {
    protected Address address;
    protected Atomix atomix;

    /**
     * Sets the REST service address.
     *
     * @param address the REST service address
     * @return the REST service builder
     * @throws NullPointerException if the address is null
     */
    public Builder withAddress(Address address) {
      this.address = checkNotNull(address, "address cannot be null");
      return this;
    }

    /**
     * Sets the Atomix instance.
     *
     * @param atomix the Atomix instance
     * @return the REST service builder
     * @throws NullPointerException if the Atomix instance is null
     */
    public Builder withAtomix(Atomix atomix) {
      this.atomix = checkNotNull(atomix, "atomix cannot be null");
      return this;
    }
  }
}
