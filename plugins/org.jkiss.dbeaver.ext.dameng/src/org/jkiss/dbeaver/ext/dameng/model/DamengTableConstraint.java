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

import org.jkiss.dbeaver.ext.generic.model.GenericTableBase;
import org.jkiss.dbeaver.ext.generic.model.GenericTableConstraintColumn;
import org.jkiss.dbeaver.ext.generic.model.GenericUniqueKey;
import org.jkiss.dbeaver.model.runtime.DBRProgressMonitor;
import org.jkiss.dbeaver.model.struct.DBSEntityConstraintType;
import org.jkiss.utils.CommonUtils;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/**
 * @author Baishengkai
 * @since 2024/10/13
 */
public class DamengTableConstraint extends GenericUniqueKey {
    private final List<GenericTableConstraintColumn> columns = new ArrayList<>();

    public DamengTableConstraint(GenericTableBase table, String name, String remarks, DBSEntityConstraintType constraintType, boolean persisted) {
        super(table, name, remarks, constraintType, persisted);
    }

    @Override
    public List<GenericTableConstraintColumn> getAttributeReferences(DBRProgressMonitor monitor) {
        return super.getAttributeReferences(monitor);
    }

    @Override
    public void setAttributeReferences(List<GenericTableConstraintColumn> columns) {
        this.columns.clear();
        this.columns.addAll(columns);
        if (!CommonUtils.isEmpty(columns) && columns.size() > 1) {
            columns.sort(Comparator.comparingInt(GenericTableConstraintColumn::getOrdinalPosition));
        }
    }

    protected static DBSEntityConstraintType getConstraintType(String code) {
        return switch (code) {
            case "P" -> DBSEntityConstraintType.PRIMARY_KEY;
            case "U" -> DBSEntityConstraintType.UNIQUE_KEY;
            case "F" -> DBSEntityConstraintType.FOREIGN_KEY;
            case "C" -> DBSEntityConstraintType.CHECK;
            default -> DBSEntityConstraintType.UNIQUE_KEY;
        };
    }
}
