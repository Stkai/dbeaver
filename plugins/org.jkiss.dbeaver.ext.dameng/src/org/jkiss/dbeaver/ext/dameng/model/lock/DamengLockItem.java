/*
 * DBeaver - Universal Database Manager
 * Copyright (C) 2010-2023 DBeaver Corp and others
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

package org.jkiss.dbeaver.ext.dameng.model.lock;

import org.jkiss.dbeaver.model.admin.locks.DBAServerLockItem;
import org.jkiss.dbeaver.model.exec.jdbc.JDBCResultSet;
import org.jkiss.dbeaver.model.impl.jdbc.JDBCUtils;
import org.jkiss.dbeaver.model.meta.Property;

/**
 * @author Shengkai Bai
 */
public class DamengLockItem implements DBAServerLockItem {
    private Long addr;
    private String lockType;

    private String lockMode;

    public DamengLockItem(JDBCResultSet dbResult) {
        this.addr = JDBCUtils.safeGetLong(dbResult, "ADDR");
        this.lockType = JDBCUtils.safeGetString(dbResult, "LTYPE");
        this.lockMode = JDBCUtils.safeGetString(dbResult, "LMODE");
    }

    @Property(viewable = true)
    public Long getAddr() {
        return addr;
    }

    @Property(viewable = true)
    public String getLockType() {
        return lockType;
    }

    @Property(viewable = true)
    public String getLockMode() {
        return lockMode;
    }
}
