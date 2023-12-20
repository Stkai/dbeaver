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

import org.jkiss.dbeaver.DBException;
import org.jkiss.dbeaver.ext.dameng.model.DamengDataSource;
import org.jkiss.dbeaver.model.DBPDataSource;
import org.jkiss.dbeaver.model.admin.locks.DBAServerLockManager;
import org.jkiss.dbeaver.model.exec.DBCSession;
import org.jkiss.dbeaver.model.exec.jdbc.JDBCPreparedStatement;
import org.jkiss.dbeaver.model.exec.jdbc.JDBCResultSet;
import org.jkiss.dbeaver.model.exec.jdbc.JDBCSession;
import org.jkiss.dbeaver.model.impl.admin.locks.LockGraphManager;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author Shengkai Bai
 */
public class DamengLockManager extends LockGraphManager implements DBAServerLockManager<DamengLock, DamengLockItem> {

    private DamengDataSource dataSource;

    public DamengLockManager(DamengDataSource dataSource) {
        this.dataSource = dataSource;
    }

    @Override
    public DBPDataSource getDataSource() {
        return dataSource;
    }

    @Override
    public Map<?, DamengLock> getLocks(DBCSession session, Map<String, Object> options) throws DBException {
        try {
            Map<Object, DamengLock> locks = new HashMap<>(10);

            String sql = "SELECT\n" +
                    "LK.*,\n" +
                    "SESS.*,\n" +
                    "TAB.NAME\n" +
                    "FROM\n" +
                    "V$LOCK LK\n" +
                    "LEFT JOIN GV$SESSIONS SESS ON LK.TRX_ID = SESS.TRX_ID\n" +
                    "LEFT JOIN SYSOBJECTS TAB ON\n" +
                    "LK.TABLE_ID = TAB.ID\n" +
                    "WHERE\n" +
                    "LK.TRX_ID IN (\n" +
                    "SELECT\n" +
                    "TRX_ID\n" +
//                    "TID\n" +
                    "FROM\n" +
                    "V$LOCK L\n" +
                    "WHERE\n" +
                    "L.BLOCKED > 0)\n" +
                    "OR BLOCKED > 0";
            try (JDBCPreparedStatement dbStat = ((JDBCSession) session).prepareStatement(sql)) {
                try (JDBCResultSet dbResult = dbStat.executeQuery()) {
                    while (dbResult.next()) {
                        DamengLock lock = new DamengLock(dbResult);
                        locks.put(lock.getId(), lock);
                    }
                }
            }
            super.buildGraphs(locks);
            return locks;
        } catch (SQLException e) {
            throw new DBException(e, session.getDataSource());
        }
    }

    @Override
    public Collection<DamengLockItem> getLockItems(DBCSession session, Map<String, Object> options) throws DBException {
        try {
            List<DamengLockItem> lockItems = new ArrayList<>();
            String sql = "SELECT * FROM V$LOCK WHERE TRX_ID = ? OR TID = ? ";
            try (JDBCPreparedStatement dbStat = ((JDBCSession) session).prepareStatement(sql)) {
                String otype = (String) options.get(LockGraphManager.keyType);
                dbStat.setLong(1, (Long) options.get(otype));
                dbStat.setLong(2, (Long) options.get(otype));
                try (JDBCResultSet dbResult = dbStat.executeQuery()) {

                    while (dbResult.next()) {
                        lockItems.add(new DamengLockItem(dbResult));
                    }
                }
            }
        } catch (SQLException e) {
            throw new DBException(e, session.getDataSource());

        }
        return null;
    }

    @Override
    public void alterSession(DBCSession session, DamengLock sessionType, Map<String, Object> options) throws DBException {

    }

    @Override
    public Class<DamengLock> getLocksType() {
        return DamengLock.class;
    }
}
