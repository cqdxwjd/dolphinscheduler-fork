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

import org.apache.dolphinscheduler.server.master.events.IEventRepository;
import org.apache.dolphinscheduler.server.master.events.TaskOperationEvent;
import org.apache.dolphinscheduler.server.master.events.TaskOperationType;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Builder
@AllArgsConstructor
public class DAGEngine implements IDAGEngine {

    private final WorkflowExecutionContext workflowExecutionContext;

    @Getter
    private final IWorkflowDAG workflowDAG;

    @Getter
    private final IWorkflowExecutionDAG workflowExecutionDAG;

    private final ITaskExecutionRunnableFactory taskExecutionRunnableFactory;

    private final IEventRepository eventRepository;

    @Override
    public void triggerNextTasks(String parentTaskNodeName) {
        workflowDAG.getDirectPostNodeNames(parentTaskNodeName).forEach(this::triggerTask);
    }

    @Override
    public void triggerTask(String taskName) {
        if (!workflowExecutionDAG.isTaskAbleToBeTriggered(taskName)) {
            return;
        }

        TaskExecutionRunnable taskExecutionRunnable = taskExecutionRunnableFactory.createTaskExecutionRunnable(null);
        TaskOperationEvent taskOperationEvent = TaskOperationEvent.builder()
                .taskExecutionRunnable(taskExecutionRunnable)
                .taskOperationType(TaskOperationType.DISPATCH)
                .build();
        eventRepository.storeEventToTail(taskOperationEvent);
    }

    @Override
    public void pauseTask(Integer taskInstanceId) {
        TaskExecutionRunnable taskExecutionRunnable = workflowExecutionDAG.getTaskExecutionRunnableById(taskInstanceId);
        if (taskExecutionRunnable == null) {
            log.error("Cannot find the ITaskExecutionRunnable for taskInstance: {}", taskInstanceId);
            return;
        }
        TaskOperationEvent taskOperationEvent = TaskOperationEvent.builder()
                .taskExecutionRunnable(taskExecutionRunnable)
                .taskOperationType(TaskOperationType.PAUSE)
                .build();
        eventRepository.storeEventToTail(taskOperationEvent);
    }

    @Override
    public void killTask(Integer taskInstanceId) {
        TaskExecutionRunnable taskExecutionRunnable = workflowExecutionDAG.getTaskExecutionRunnableById(taskInstanceId);
        if (taskExecutionRunnable == null) {
            log.error("Cannot find the ITaskExecutionRunnable for taskInstance: {}", taskInstanceId);
            return;
        }

        TaskOperationEvent taskOperationEvent = TaskOperationEvent.builder()
                .taskExecutionRunnable(taskExecutionRunnable)
                .taskOperationType(TaskOperationType.KILL)
                .build();
        eventRepository.storeEventToTail(taskOperationEvent);
    }

}
