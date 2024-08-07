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

import org.jkiss.dbeaver.model.admin.sessions.AbstractServerSession;
import org.jkiss.dbeaver.model.exec.jdbc.JDBCResultSet;
import org.jkiss.dbeaver.model.impl.jdbc.JDBCTransactionIsolation;
import org.jkiss.dbeaver.model.impl.jdbc.JDBCUtils;
import org.jkiss.dbeaver.model.meta.Property;

import java.sql.Timestamp;
import java.util.Objects;

/**
 * @author Shengkai Bai
 */
public class DamengServerSession extends AbstractServerSession {

    private static final String CAT_SESSION = "Session";
    private static final String CAT_SQL = "SQL";
    private static final String CAT_CLIENT = "Client";

    private long id;

    private Integer sqlId;

    private String user;

    private String schema;

    private String runStatus;

    private String state;

    private String sql;

    private JDBCTransactionIsolation isolationLevel;

    private String appName;

    private Timestamp createTime;

    private String clientType;

    private String clientHost;

    private String clientIp;

    private String clientVersion;

    private String clientInfo;

    private String clientIdentifier;

    private Long threadId;

    public DamengServerSession(JDBCResultSet dbResult) {
        this.id = JDBCUtils.safeGetLong(dbResult, "SESS_ID");
        this.user = JDBCUtils.safeGetString(dbResult, "USER_NAME");
        this.schema = JDBCUtils.safeGetString(dbResult, "CURR_SCH");
        this.runStatus = JDBCUtils.safeGetString(dbResult, "RUN_STATUS");
        this.state = JDBCUtils.safeGetString(dbResult, "STATE");
        this.sql = JDBCUtils.safeGetString(dbResult, "SQL_TEXT");
        this.isolationLevel = JDBCTransactionIsolation.getByCode(JDBCUtils.safeGetInt(dbResult, "ISO_LEVEL") + 1);
        this.appName = JDBCUtils.safeGetString(dbResult, "APPNAME");
        this.clientType = JDBCUtils.safeGetString(dbResult, "CLNT_TYPE");
        this.clientHost = JDBCUtils.safeGetString(dbResult, "CLNT_HOST");
        this.clientIp = JDBCUtils.safeGetString(dbResult, "CLNT_IP");
        this.createTime = JDBCUtils.safeGetTimestamp(dbResult, "CREATE_TIME");
        this.clientVersion = JDBCUtils.safeGetString(dbResult, "CLNT_VER");
        this.clientInfo = JDBCUtils.safeGetString(dbResult, "CLIENT_INFO");
        this.clientIdentifier = JDBCUtils.safeGetString(dbResult, "CLIENT_IDENTIFIER");
        this.sqlId = JDBCUtils.safeGetInteger(dbResult, "SQL_ID");
        this.threadId = JDBCUtils.safeGetLong(dbResult, "THRD_ID");
    }

    @Override
    public String getActiveQuery() {
        return sql;
    }

    @Property(category = CAT_SESSION, viewable = true, order = 1)
    public long getId() {
        return id;
    }

    @Property(category = CAT_SESSION, viewable = true, order = 2)
    public String getUser() {
        return user;
    }

    @Property(category = CAT_SESSION, viewable = true, order = 3)
    public String getSchema() {
        return schema;
    }

    @Property(category = CAT_SESSION, viewable = true, order = 4)
    public String getRunStatus() {
        return runStatus;
    }

    @Property(category = CAT_SESSION, viewable = true, order = 5)
    public String getState() {
        return state;
    }

    @Property(category = CAT_SESSION, viewable = true, order = 6)
    public Timestamp getCreateTime() {
        return createTime;
    }

    @Property(category = CAT_SESSION, viewable = true, order = 7)
    public String getIsolationLevel() {
        return isolationLevel.getTitle();
    }

    @Property(category = CAT_CLIENT, viewable = true, order = 7)
    public String getAppName() {
        return appName;
    }

    @Property(category = CAT_CLIENT, viewable = true, order = 8)
    public String getClientHost() {
        return clientHost;
    }

    @Property(category = CAT_CLIENT, viewable = true, order = 9)
    public String getClientType() {
        return clientType;
    }

    @Property(category = CAT_CLIENT, viewable = true, order = 10)
    public String getClientIp() {
        return clientIp;
    }

    @Property(category = CAT_CLIENT, viewable = true, order = 11)
    public String getClientVersion() {
        return clientVersion;
    }

    @Property(category = CAT_CLIENT, order = 12)
    public String getClientInfo() {
        return clientInfo;
    }

    @Property(category = CAT_CLIENT, order = 13)
    public String getClientIdentifier() {
        return clientIdentifier;
    }

    @Property(category = CAT_SQL, order = 14)
    public Integer getSqlId() {
        return sqlId;
    }

    @Property(category = CAT_SQL, order = 15)
    public Long getThreadId() {
        return threadId;
    }

    @Override
    public Object getActiveQueryId() {
        return sqlId;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DamengServerSession that = (DamengServerSession) o;
        return id == that.id;
    }

    @Override
    public int hashCode() {
        return Objects.hash(id);
    }

    @Override
    public String toString() {
        return id + "-" + schema;
    }
}
