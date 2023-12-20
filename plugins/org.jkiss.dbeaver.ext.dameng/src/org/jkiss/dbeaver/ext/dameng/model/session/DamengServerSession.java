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

    private long id;

    private String instanceName;

    private String user;

    private String schema;

    private String status;

    private String state;

    private String sql;

    private JDBCTransactionIsolation isolationLevel;

    private String appName;

    private Timestamp createTime;

    private String clientType;

    private String clientHost;

    private String clientIp;

    private String clientVersion;


    public DamengServerSession(JDBCResultSet dbResult) {
        this.id = JDBCUtils.safeGetLong(dbResult, "SESS_ID");
        this.instanceName = JDBCUtils.safeGetString(dbResult, "INSTANCE_NAME");
        this.user = JDBCUtils.safeGetString(dbResult, "USER_NAME");
        this.schema = JDBCUtils.safeGetString(dbResult, "CURR_SCH");
        this.status = JDBCUtils.safeGetString(dbResult, "RUN_STATUS");
        this.state = JDBCUtils.safeGetString(dbResult, "STATE");
        this.sql = JDBCUtils.safeGetString(dbResult, "SQL_TEXT");
        this.isolationLevel = JDBCTransactionIsolation.getByCode(JDBCUtils.safeGetInt(dbResult, "ISO_LEVEL") + 1);
        this.appName = JDBCUtils.safeGetString(dbResult, "APPNAME");
        this.clientType = JDBCUtils.safeGetString(dbResult, "CLNT_TYPE");
        this.clientHost = JDBCUtils.safeGetString(dbResult, "CLNT_HOST");
        this.clientIp = JDBCUtils.safeGetString(dbResult, "CLNT_IP");
        this.createTime = JDBCUtils.safeGetTimestamp(dbResult, "CREATE_TIME");
        this.clientVersion = JDBCUtils.safeGetString(dbResult, "CLNT_VER");
    }

    @Override
    public String getActiveQuery() {
        return sql;
    }

    @Property(viewable = true, order = 1)
    public long getId() {
        return id;
    }

    @Property(viewable = true)
    public String getInstanceName() {
        return instanceName;
    }

    @Property(viewable = true, order = 2)
    public String getUser() {
        return user;
    }

    @Property(viewable = true, order = 3)
    public String getSchema() {
        return schema;
    }

    @Property(viewable = true, order = 4)
    public String getStatus() {
        return status;
    }

    @Property(viewable = true, order = 5)
    public String getState() {
        return state;
    }

    @Property(viewable = true, order = 6)
    public Timestamp getCreateTime() {
        return createTime;
    }

    @Property(viewable = true, order = 7)
    public String getIsolationLevel() {
        return isolationLevel.getTitle();
    }

    @Property(viewable = true, order = 7)
    public String getAppName() {
        return appName;
    }

    @Property(viewable = true, order = 8)
    public String getClientHost() {
        return clientHost;
    }

    @Property(viewable = true, order = 9)
    public String getClientType() {
        return clientType;
    }

    @Property(viewable = true, order = 10)
    public String getClientIp() {
        return clientIp;
    }

    @Property(viewable = true, order = 11)
    public String getClientVersion() {
        return clientVersion;
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
