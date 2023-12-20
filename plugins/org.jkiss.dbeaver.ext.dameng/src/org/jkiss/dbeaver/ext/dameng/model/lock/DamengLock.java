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

import org.jkiss.dbeaver.model.admin.locks.DBAServerLock;
import org.jkiss.dbeaver.model.exec.jdbc.JDBCResultSet;
import org.jkiss.dbeaver.model.impl.jdbc.JDBCUtils;
import org.jkiss.dbeaver.model.meta.Property;

import java.util.ArrayList;
import java.util.List;

/**
 * @author Shengkai Bai
 */
public class DamengLock implements DBAServerLock {

    private Long addr;

    private Long trxId;

    private Long tId;
    private DBAServerLock hold;

    private List<DBAServerLock> waiters = new ArrayList<>(0);

    public DamengLock(JDBCResultSet dbResult) {
        this.addr = JDBCUtils.safeGetLong(dbResult, "ADDR");
        this.trxId = JDBCUtils.safeGetLong(dbResult, "TRX_ID");
        this.tId = JDBCUtils.safeGetLong(dbResult, "TID");
    }

    @Override
    public String getTitle() {
        return String.valueOf(trxId);
    }

    @Override
    public Object getId() {
        return trxId;
    }

    @Override
    public DBAServerLock getHoldBy() {
        return this.hold;
    }

    @Override
    public void setHoldBy(DBAServerLock lock) {
        this.hold = lock;
    }

    @Override
    public Object getHoldID() {
        return this.tId;
    }

    @Override
    public List<DBAServerLock> waitThis() {
        return this.waiters;
    }

    @Property(viewable = true)
    public Long getTrxId() {
        return trxId;
    }

    @Property(viewable = true)
    public Long gettId() {
        return tId;
    }
}
