// SPDX-FileCopyrightText: 2017-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.protocols.backup.roles;

import io.atomix.protocols.backup.PrimaryBackupServer.Role;
import io.atomix.protocols.backup.service.impl.PrimaryBackupServiceContext;

/**
 * None role.
 */
public class NoneRole extends PrimaryBackupRole {
  public NoneRole(PrimaryBackupServiceContext service) {
    super(Role.NONE, service);
  }
}
