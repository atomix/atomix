// SPDX-FileCopyrightText: 2017-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.protocols.log.roles;

import io.atomix.protocols.log.DistributedLogServer.Role;
import io.atomix.protocols.log.impl.DistributedLogServerContext;

/**
 * None role.
 */
public class NoneRole extends LogServerRole {
  public NoneRole(DistributedLogServerContext service) {
    super(Role.NONE, service);
  }
}
