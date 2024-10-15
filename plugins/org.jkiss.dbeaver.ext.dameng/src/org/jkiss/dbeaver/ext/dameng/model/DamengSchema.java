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

package org.jkiss.dbeaver.ext.dameng.model;

import org.jkiss.code.NotNull;
import org.jkiss.dbeaver.DBException;
import org.jkiss.dbeaver.ext.generic.model.GenericDataSource;
import org.jkiss.dbeaver.ext.generic.model.GenericSchema;
import org.jkiss.dbeaver.model.DBPEvaluationContext;
import org.jkiss.dbeaver.model.DBPObjectStatisticsCollector;
import org.jkiss.dbeaver.model.DBPQualifiedObject;
import org.jkiss.dbeaver.model.DBUtils;
import org.jkiss.dbeaver.model.exec.DBCException;
import org.jkiss.dbeaver.model.exec.jdbc.JDBCPreparedStatement;
import org.jkiss.dbeaver.model.exec.jdbc.JDBCResultSet;
import org.jkiss.dbeaver.model.exec.jdbc.JDBCSession;
import org.jkiss.dbeaver.model.exec.jdbc.JDBCStatement;
import org.jkiss.dbeaver.model.impl.jdbc.cache.JDBCCompositeCache;
import org.jkiss.dbeaver.model.impl.jdbc.cache.JDBCStructCache;
import org.jkiss.dbeaver.model.meta.Property;
import org.jkiss.dbeaver.model.runtime.DBRProgressMonitor;

import java.sql.SQLException;
import java.util.List;

/**
 * @author Shengkai Bai
 */
public class DamengSchema extends GenericSchema implements DBPQualifiedObject, DBPObjectStatisticsCollector {

    @NotNull
    private String schemaName;

    private boolean persisted;

    private boolean hasStatistics;

    private final ConstraintCache constraintCache = new ConstraintCache();

    public DamengSchema(GenericDataSource dataSource, String schemaName, boolean persisted) {
        super(dataSource, null, schemaName);
        this.schemaName = schemaName;
        this.persisted = persisted;
    }

    @NotNull
    @Override
    @Property(viewable = true, order = 1)
    public String getName() {
        return schemaName;
    }

    public void setName(@NotNull String schemaName) {
        this.schemaName = schemaName;
    }

    @Override
    public boolean isStatisticsCollected() {
        return hasStatistics;
    }

    @Override
    public void collectObjectStatistics(DBRProgressMonitor monitor, boolean totalSizeOnly, boolean forceRefresh) throws DBException {
        if (hasStatistics || forceRefresh) {
            return;
        }
        try (JDBCSession session = DBUtils.openMetaSession(monitor, this, "Load table status")) {
            JDBCPreparedStatement dbStat = session.prepareStatement(
                    "SELECT TABLE_NAME,TABLE_USED_PAGES(OWNER,TABLE_NAME) * PAGE AS DISK_SIZE " +
                            "FROM ALL_TABLES " +
                            "WHERE owner = ?"
            );
            dbStat.setString(1, getName());
            try (JDBCResultSet dbResult = dbStat.executeQuery()) {
                while (dbResult.next()) {
                    String tableName = dbResult.getString(1);
                    DamengTable table = (DamengTable) getTable(monitor, tableName);
                    if (table != null) {
                        table.fetchStatistics(dbResult);
                    }
                }
            }
        } catch (SQLException e) {
            throw new DBCException("Error reading schema relation statistics", e);
        } finally {
            this.hasStatistics = true;
        }
    }

    @NotNull
    @Override
    public String getFullyQualifiedName(DBPEvaluationContext context) {
        return DBUtils.getFullQualifiedName(getDataSource(), this);
    }

    @Override
    public boolean isPersisted() {
        return persisted;
    }

//    @Override
//    public List<DamengTable> getTables(DBRProgressMonitor monitor) throws DBException {
//        return tableCache.getAllObjects(monitor, this);
//    }
//
//    public class TableCache extends JDBCStructLookupCache<DamengSchema, DamengTable, DamengTableColumn> {
//
//        public TableCache() {
//            super(GenericUtils.getColumn(getDataSource(), GenericConstants.OBJECT_TABLE, JDBCConstants.TABLE_NAME));
////            super(objectNameColumn);
//        }
//
//        @Override
//        public JDBCStatement prepareLookupStatement(JDBCSession session, DamengSchema damengSchema, DamengTable table, String objectName) throws SQLException {
//            JDBCPreparedStatement dbStat = session.prepareStatement("SELECT " +
//                    "TAB_OBJ.NAME, " +
//                    "TAB_OBJ.SUBTYPE$, " +
//                    "SCH_OBJ.NAME AS SCH_NAME " +
//                    "FROM " +
//                    "SYSOBJECTS TAB_OBJ " +
//                    "LEFT JOIN SYSOBJECTS SCH_OBJ ON " +
//                    "TAB_OBJ.SCHID = SCH_OBJ.ID " +
//                    "LEFT JOIN SYSTABLECOMMENTS COMMENT_OBJ ON " +
//                    "TAB_OBJ.NAME = COMMENT_OBJ.TVNAME " +
//                    "AND SCH_OBJ.NAME = COMMENT_OBJ.SCHNAME " +
//                    "WHERE " +
//                    "TAB_OBJ.SUBTYPE$ LIKE '_TAB' " +
//                    "AND SCH_OBJ.NAME = ?");
//            dbStat.setString(1, damengSchema.getName());
//            return dbStat;
//        }
//
//        @Override
//        protected JDBCStatement prepareChildrenStatement(JDBCSession session, DamengSchema damengSchema, DamengTable forObject) throws SQLException {
//            return null;
//        }
//
//        @Override
//        protected DamengTableColumn fetchChild(JDBCSession session, DamengSchema damengSchema, DamengTable parent, JDBCResultSet dbResult) throws SQLException, DBException {
//            return null;
//        }
//
//        @Override
//        protected DamengTable fetchObject(JDBCSession session, DamengSchema damengSchema, JDBCResultSet resultSet) throws SQLException, DBException {
//            return null;
//        }
//    }

    class ConstraintCache extends JDBCCompositeCache<DamengSchema, DamengTable, DamengTableConstraint, DamengTableConstraintColumn> {

        public ConstraintCache() {
            super((JDBCStructCache) getTableCache(), DamengTable.class, "TABLE_NAME", "CONNAME");
        }

        protected ConstraintCache(JDBCStructCache<DamengSchema, ?, ?> parentCache, Class<DamengTable> parentType, Object parentColumnName, Object objectColumnName) {
            super(parentCache, parentType, parentColumnName, objectColumnName);
        }

        @Override
        protected JDBCStatement prepareObjectsStatement(JDBCSession session, DamengSchema damengSchema, DamengTable forParent) throws SQLException {
            session.prepareStatement("SELECT\n" +
                    "COLS.NAME\n" +
                    "FROM\n" +
                    "SYSCOLUMNS COLS,\n" +
                    "SYSCONS CONS,\n" +
                    "SYSINDEXES INDS\n" +
                    "WHERE\n" +
                    "CONS.ID = 134218884\n" +
                    "AND COLS.ID = 1079\n" +
                    "AND COLS.ID = CONS.TABLEID\n" +
                    "AND INDS.ID = CONS.INDEXID\n" +
                    "AND SF_COL_IS_IDX_KEY(INDS.KEYNUM,INDS.KEYINFO,COLS.COLID)= 1");
            return null;
        }

        @Override
        protected DamengTableConstraint fetchObject(JDBCSession session, DamengSchema damengSchema, DamengTable damengTable, String childName, JDBCResultSet resultSet) throws SQLException, DBException {
            return new DamengTableConstraint(damengTable,childName,damengSchema.getDataSource().getMetaModel().getUniqueConstraintType(resultSet));
        }

        @Override
        protected DamengTableConstraintColumn[] fetchObjectRow(JDBCSession session, DamengTable damengTable, DamengTableConstraint forObject, JDBCResultSet resultSet) throws SQLException, DBException {
            return new DamengTableConstraintColumn[0];
        }

        @Override
        protected void cacheChildren(DBRProgressMonitor monitor, DamengTableConstraint object, List<DamengTableConstraintColumn> children) {

        }
    }

    /**
     * SELECT
     * 	COLS.NAME
     * FROM
     * 	SYSCOLUMNS COLS,
     * 	SYSCONS CONS,
     * 	SYSINDEXES INDS
     * WHERE
     * 	CONS.ID = 134218884
     * 	AND COLS.ID = 1079
     * 	AND COLS.ID = CONS.TABLEID
     * 	AND INDS.ID = CONS.INDEXID
     * 	AND SF_COL_IS_IDX_KEY(INDS.KEYNUM,INDS.KEYINFO,COLS.COLID)= 1
     */

}
