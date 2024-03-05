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

import org.apache.dolphinscheduler.dao.entity.TaskInstance;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import lombok.experimental.SuperBuilder;
import lombok.extern.slf4j.Slf4j;

/**
 * The WorkflowExecutionDAG represent a running workflow instance DAG.
 */
@Slf4j
@SuperBuilder
public class WorkflowExecutionDAG extends BasicDAG<TaskInstance> implements IWorkflowExecutionDAG {

    private final WorkflowExecutionContext workflowExecutionContext;

    private final TaskExecutionRunnableRepository taskExecutionRunnableRepository;

    private List<TaskExecutionValve> taskExecutionValves;

    @Override
    public TaskExecutionRunnable getTaskExecutionRunnableById(Integer taskInstanceId) {
        return taskExecutionRunnableRepository.getTaskExecutionRunnableById(taskInstanceId);
    }

    @Override
    public TaskExecutionRunnable getTaskExecutionRunnableByName(String taskName) {
        return taskExecutionRunnableRepository.getTaskExecutionRunnableByName(taskName);
    }

    @Override
    public List<TaskExecutionRunnable> getActiveTaskExecutionRunnable() {
        return new ArrayList<>(taskExecutionRunnableRepository.getActiveTaskExecutionRunnable());
    }

    @Override
    public List<TaskExecutionRunnable> getDirectPreTaskExecutionRunnable(String taskName) {
        return getDirectPreNodeNames(taskName)
                .stream()
                .map(taskExecutionRunnableRepository::getTaskExecutionRunnableByName)
                .collect(Collectors.toList());
    }

    @Override
    public boolean isTaskAbleToBeTriggered(String taskNodeName) {
        // todo: Check whether the workflow instance is finished or ready to finish.
        List<DAGNode<TaskInstance>> directPreNodes = getDirectPreNodes(taskNodeName);
        for (DAGNode<TaskInstance> directPreNode : directPreNodes) {
            if (directPreNode.isSkip()) {
                continue;
            }
            TaskExecutionRunnable taskExecutionRunnable = getTaskExecutionRunnableByName(directPreNode.getNodeName());
            if (taskExecutionRunnable == null || !taskExecutionRunnable.canAccessTo(taskNodeName)) {
                return false;
            }
        }
        return true;
    }
}
