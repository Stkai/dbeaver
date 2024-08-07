/*
 * DBeaver - Universal Database Manager
 * Copyright (C) 2010-2024 DBeaver Corp and others
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

package org.jkiss.dbeaver.ext.dameng.model.session;

import org.jkiss.dbeaver.DBException;
import org.jkiss.dbeaver.ext.dameng.model.DamengDataSource;
import org.jkiss.dbeaver.model.DBPDataSource;
import org.jkiss.dbeaver.model.admin.sessions.DBAServerSessionDetails;
import org.jkiss.dbeaver.model.admin.sessions.DBAServerSessionDetailsProvider;
import org.jkiss.dbeaver.model.admin.sessions.DBAServerSessionManager;
import org.jkiss.dbeaver.model.admin.sessions.DBAServerSessionManagerSQL;
import org.jkiss.dbeaver.model.exec.DBCSession;
import org.jkiss.dbeaver.model.exec.jdbc.JDBCPreparedStatement;
import org.jkiss.dbeaver.model.exec.jdbc.JDBCResultSet;
import org.jkiss.dbeaver.model.exec.jdbc.JDBCSession;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 * @author Shengkai Bai
 */
public class DamengSessionManager implements DBAServerSessionManager<DamengServerSession>, DBAServerSessionManagerSQL {

    private DamengDataSource dataSource;

    public DamengSessionManager(DamengDataSource dataSource) {
        this.dataSource = dataSource;
    }

    @Override
    public DBPDataSource getDataSource() {
        return dataSource;
    }

    @Override
    public Collection<DamengServerSession> getSessions(DBCSession session, Map<String, Object> options) throws DBException {
        try (JDBCPreparedStatement dbStat = ((JDBCSession) session).prepareStatement(generateSessionReadQuery(options))) {
            try (JDBCResultSet dbResult = dbStat.executeQuery()) {
                List<DamengServerSession> sessions = new ArrayList<>();
                while (dbResult.next()) {
                    sessions.add(new DamengServerSession(dbResult));
                }
                return sessions;
            }

        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void alterSession(DBCSession session, DamengServerSession damengServerSession, Map<String, Object> options) throws DBException {
        try {
            try (JDBCPreparedStatement dbStat = ((JDBCSession) session).prepareStatement("SP_CLOSE_SESSION(?)")) {
                dbStat.setLong(1, damengServerSession.getId());
                dbStat.execute();
            }
        } catch (SQLException e) {
            throw new DBException(e, session.getDataSource());
        }
    }

    @Override
    public boolean canGenerateSessionReadQuery() {
        return true;
    }

    @Override
    public String generateSessionReadQuery(Map<String, Object> options) {
        return "SELECT * FROM GV$SESSIONS";
    }

}
