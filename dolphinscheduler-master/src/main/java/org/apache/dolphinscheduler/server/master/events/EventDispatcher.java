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

package org.apache.dolphinscheduler.server.master.events;

import org.apache.dolphinscheduler.server.master.dag.IWorkflowExecutionRunnable;
import org.apache.dolphinscheduler.server.master.dag.WorkflowExecuteRunnableRepository;

import lombok.extern.slf4j.Slf4j;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class EventDispatcher implements IEventDispatcher<IEvent> {

    @Autowired
    private EventEngine eventEngine;

    @Autowired
    private WorkflowExecuteRunnableRepository workflowExecuteRunnableRepository;

    @Override
    public void start() {
        log.info(getClass().getName() + " started");
    }

    @Override
    public void dispatchEvent(IEvent iEvent) {
        Integer workflowInstanceId;
        if (iEvent instanceof IWorkflowEvent) {
            workflowInstanceId = ((IWorkflowEvent) iEvent).getWorkflowInstanceId();
        } else if (iEvent instanceof ITaskEvent) {
            workflowInstanceId = ((ITaskEvent) iEvent).getWorkflowInstanceId();
        } else {
            throw new IllegalArgumentException("Unsupported event type: " + iEvent.getClass().getName());
        }

        IWorkflowExecutionRunnable workflowExecuteRunnable =
                workflowExecuteRunnableRepository.getWorkflowExecutionRunnableById(workflowInstanceId);
        if (workflowExecuteRunnable == null) {
            log.error("Cannot find the IWorkflowExecutionRunnable for event: {}", iEvent);
            return;
        }
        workflowExecuteRunnable.storeEventToTail(iEvent);
        log.debug("Success dispatch event {} to EventRepository", iEvent);
        eventEngine.notify();
    }

    @Override
    public void stop() {
        log.info(getClass().getName() + " stopped");
    }

}
