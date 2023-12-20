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

package org.jkiss.dbeaver.ext.dameng.ui.editors;

import org.eclipse.jface.action.Action;
import org.eclipse.jface.action.IContributionManager;
import org.eclipse.jface.action.Separator;
import org.eclipse.osgi.util.NLS;
import org.eclipse.swt.widgets.Composite;
import org.jkiss.dbeaver.ext.dameng.model.DamengDataSource;
import org.jkiss.dbeaver.ext.dameng.model.session.DamengServerSession;
import org.jkiss.dbeaver.ext.dameng.model.session.DamengSessionManager;
import org.jkiss.dbeaver.model.admin.sessions.DBAServerSession;
import org.jkiss.dbeaver.model.admin.sessions.DBAServerSessionManager;
import org.jkiss.dbeaver.model.exec.DBCExecutionContext;
import org.jkiss.dbeaver.ui.DBeaverIcons;
import org.jkiss.dbeaver.ui.UIIcon;
import org.jkiss.dbeaver.ui.UIUtils;
import org.jkiss.dbeaver.ui.views.session.AbstractSessionEditor;
import org.jkiss.dbeaver.ui.views.session.SessionManagerViewer;

import java.util.List;

/**
 * @author Shengkai Bai
 */
public class DamengSessionEditor extends AbstractSessionEditor {

    private KillSessionAction killSessionAction;


    @Override
    public void createEditorControl(Composite parent) {
        killSessionAction = new KillSessionAction();
        super.createEditorControl(parent);
    }

    @Override
    protected SessionManagerViewer<DamengServerSession> createSessionViewer(DBCExecutionContext executionContext, Composite parent) {
        return new SessionManagerViewer<>(this, parent, new DamengSessionManager((DamengDataSource) executionContext.getDataSource())) {
            @Override
            protected void contributeToToolbar(DBAServerSessionManager sessionManager, IContributionManager contributionManager) {
                contributionManager.add(killSessionAction);
                contributionManager.add(new Separator());
            }
        };
    }

    private class KillSessionAction extends Action {

        public KillSessionAction() {
            super("Kill Session", DBeaverIcons.getImageDescriptor(UIIcon.REJECT));
        }

        @Override
        public void run() {
            final List<DBAServerSession> sessions = getSessionsViewer().getSelectedSessions();
            if (sessions != null && UIUtils.confirmAction(
                    getSite().getShell(),
                    this.getText(),
                    NLS.bind("Kill session {0}?", sessions))) {
                getSessionsViewer().alterSessions(sessions, null);
            }
        }
    }
}
