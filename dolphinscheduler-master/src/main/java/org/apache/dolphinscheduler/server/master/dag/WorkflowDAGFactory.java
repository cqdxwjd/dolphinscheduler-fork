/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.dolphinscheduler.server.master.dag;

import org.apache.dolphinscheduler.dao.entity.ProcessTaskRelationLog;
import org.apache.dolphinscheduler.dao.entity.TaskDefinitionLog;
import org.apache.dolphinscheduler.dao.repository.ProcessTaskRelationLogDao;
import org.apache.dolphinscheduler.dao.repository.TaskDefinitionLogDao;

import java.util.List;

import lombok.extern.slf4j.Slf4j;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class WorkflowDAGFactory implements IWorkflowDAGFactory {

    @Autowired
    private TaskDefinitionLogDao taskDefinitionLogDao;

    @Autowired
    private ProcessTaskRelationLogDao processTaskRelationLogDao;

    @Override
    public IWorkflowDAG createWorkflowDAG(WorkflowIdentify workflowIdentify) {
        List<TaskDefinitionLog> taskDefinitions = queryTaskNodes(workflowIdentify);
        List<ProcessTaskRelationLog> taskRelations = queryTaskEdges(workflowIdentify);
        return WorkflowDAGBuilder.newBuilder()
                .addTaskNodes(taskDefinitions)
                .addTaskEdges(taskRelations)
                .build();
    }

    private List<TaskDefinitionLog> queryTaskNodes(WorkflowIdentify workflowIdentify) {
        return taskDefinitionLogDao.queryByWorkflowDefinitionCodeAndVersion(
                workflowIdentify.getWorkflowCode(), workflowIdentify.getWorkflowVersion());
    }

    private List<ProcessTaskRelationLog> queryTaskEdges(WorkflowIdentify workflowIdentify) {
        return processTaskRelationLogDao.queryByWorkflowDefinitionCodeAndVersion(
                workflowIdentify.getWorkflowCode(), workflowIdentify.getWorkflowVersion());
    }
}
