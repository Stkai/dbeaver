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

package org.jkiss.dbeaver.ext.dameng.edit;

import org.jkiss.code.NotNull;
import org.jkiss.dbeaver.ext.dameng.model.DamengSchema;
import org.jkiss.dbeaver.ext.dameng.model.DamengSequence;
import org.jkiss.dbeaver.ext.generic.edit.GenericSequenceManager;
import org.jkiss.dbeaver.ext.generic.model.GenericSequence;
import org.jkiss.dbeaver.ext.generic.model.GenericStructContainer;
import org.jkiss.dbeaver.model.DBPDataSource;
import org.jkiss.dbeaver.model.edit.DBECommandContext;
import org.jkiss.dbeaver.model.edit.DBEPersistAction;
import org.jkiss.dbeaver.model.exec.DBCExecutionContext;
import org.jkiss.dbeaver.model.impl.edit.SQLDatabasePersistAction;
import org.jkiss.dbeaver.model.impl.sql.edit.SQLObjectEditor;
import org.jkiss.dbeaver.model.rm.RMConstants;
import org.jkiss.dbeaver.model.runtime.DBRProgressMonitor;
import org.jkiss.dbeaver.runtime.DBWorkbench;

import java.util.List;
import java.util.Map;

/**
 * @author Shengkai Bai
 */
public class DamengSequenceManager extends GenericSequenceManager {

    @Override
    public boolean canCreateObject(Object container) {
        return DBWorkbench.getPlatform().getWorkspace().hasRealmPermission(RMConstants.PERMISSION_METADATA_EDITOR);
    }

    @Override
    public long getMakerOptions(DBPDataSource dataSource) {
        return FEATURE_EDITOR_ON_CREATE;
    }

    @Override
    protected DamengSequence createDatabaseObject(
        @NotNull DBRProgressMonitor monitor,
        @NotNull DBECommandContext context,
        Object container,
        Object copyFrom,
        @NotNull Map<String, Object> options
    ) {
        GenericStructContainer structContainer = (GenericStructContainer) container;
        DamengSchema schema = (DamengSchema) structContainer.getSchema();
        DamengSequence sequence = new DamengSequence((GenericStructContainer) container, getBaseObjectName());
        setNewObjectName(monitor, schema, sequence);
        return sequence;
    }

    @Override
    protected void addObjectCreateActions(
        @NotNull DBRProgressMonitor monitor,
        @NotNull DBCExecutionContext executionContext,
        @NotNull List<DBEPersistAction> actions,
        @NotNull SQLObjectEditor<GenericSequence, GenericStructContainer>.ObjectCreateCommand command,
        @NotNull Map<String, Object> options
    ) {
        DamengSequence sequence = (DamengSequence) command.getObject();
        actions.add(new SQLDatabasePersistAction("Create sequence", sequence.buildStatement(false)));
    }

    @Override
    protected void addObjectModifyActions(DBRProgressMonitor monitor, DBCExecutionContext executionContext, List<DBEPersistAction> actionList, SQLObjectEditor<GenericSequence, DamengSchema>.ObjectChangeCommand command, Map<String, Object> options) throws DBException {
        actionList.add(new SQLDatabasePersistAction("Alter Sequence", buildStatement(command.getObject(), false)
        ));
    }

    @Override
    protected void addObjectDeleteActions(DBRProgressMonitor monitor, DBCExecutionContext executionContext, List<DBEPersistAction> actions, SQLObjectEditor<GenericSequence, DamengSchema>.ObjectDeleteCommand command, Map<String, Object> options) throws DBException {
        actions.add(
                new SQLDatabasePersistAction("Drop sequence", "DROP SEQUENCE " + command.getObject().getFullyQualifiedName(DBPEvaluationContext.DDL))
        );
    }

    public String buildStatement(GenericSequence sequence, boolean forUpdate) {
        Number incrementBy = sequence.getIncrementBy();
        Number start = sequence.getMinValue();
        Number maxValue = sequence.getMaxValue();
        Number minValue = sequence.getMinValue();

        StringBuilder sb = new StringBuilder();
        if (forUpdate) {
            sb.append("ALTER SEQUENCE ");
        } else {
            sb.append("CREATE SEQUENCE ");
        }
        sb.append(sequence.getFullyQualifiedName(DBPEvaluationContext.DDL)).append(" ");
        if (incrementBy != null) {
            sb.append("INCREMENT BY ").append(incrementBy).append(" ");
        }
        if (start != null) {
            sb.append("START WITH ").append(start).append(" ");
        }
        if (maxValue != null) {
            sb.append("MAXVALUE ").append(maxValue).append(" ");
        }
        if (minValue != null) {
            sb.append("MINVALUE ").append(minValue).append(" ");
        }
        return sb.toString();
    }
}
