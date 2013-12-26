/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
* http://www.apache.org/licenses/LICENSE-2.0
 * 
* Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.hadoop.mapreduce.v2.app.rm;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.JobCounter;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.jobhistory.JobHistoryEvent;
import org.apache.hadoop.mapreduce.jobhistory.NormalizedResourceEvent;
import org.apache.hadoop.mapreduce.v2.api.records.JobId;
import org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptId;
import org.apache.hadoop.mapreduce.v2.api.records.TaskType;
import org.apache.hadoop.mapreduce.v2.app.AppContext;
import org.apache.hadoop.mapreduce.v2.app.client.ClientService;
import org.apache.hadoop.mapreduce.v2.app.job.event.JobCounterUpdateEvent;
import org.apache.hadoop.mapreduce.v2.app.job.event.JobDiagnosticsUpdateEvent;
import org.apache.hadoop.mapreduce.v2.app.job.event.JobEvent;
import org.apache.hadoop.mapreduce.v2.app.job.event.JobEventType;
import org.apache.hadoop.mapreduce.v2.app.job.event.JobUpdatedNodesEvent;
import org.apache.hadoop.mapreduce.v2.app.job.event.TaskAttemptContainerAssignedEvent;
import org.apache.hadoop.mapreduce.v2.app.job.event.TaskAttemptDiagnosticsUpdateEvent;
import org.apache.hadoop.mapreduce.v2.app.job.event.TaskAttemptEvent;
import org.apache.hadoop.mapreduce.v2.app.job.event.TaskAttemptEventType;
import org.apache.hadoop.mapreduce.v2.app.job.event.TaskAttemptKillEvent;
import org.apache.hadoop.util.StringInterner;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateResponse;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerExitStatus;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.NMToken;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.NodeReport;
import org.apache.hadoop.yarn.api.records.NodeState;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.client.api.NMTokenCache;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.util.RackResolver;

import com.google.common.annotations.VisibleForTesting;

/**
 * Allocates the container from the ResourceManager scheduler.
 */
//public class RMContainerAllocatorWithCap extends RMContainerAllocator {
public class RMContainerAllocatorWithCap extends RMContainerRequestor
    implements ContainerAllocator  {
    private static final Log LOG = LogFactory.getLog(RMContainerAllocatorWithCap.class);
      public static final   float DEFAULT_COMPLETED_MAPS_PERCENT_FOR_REDUCE_SLOWSTART = 0.05f;
    private static final Priority PRIORITY_FAST_FAIL_MAP;
    private static final Priority PRIORITY_REDUCE;
    private static final Priority PRIORITY_MAP;
    private Thread eventHandlingThread;
    private final AtomicBoolean stopped;

    static {
        PRIORITY_FAST_FAIL_MAP = RecordFactoryProvider.getRecordFactory(null).newRecordInstance(Priority.class);
        PRIORITY_FAST_FAIL_MAP.setPriority(5);
        PRIORITY_REDUCE = RecordFactoryProvider.getRecordFactory(null).newRecordInstance(Priority.class);
        PRIORITY_REDUCE.setPriority(10);
        PRIORITY_MAP = RecordFactoryProvider.getRecordFactory(null).newRecordInstance(Priority.class);
        PRIORITY_MAP.setPriority(20);
    }
    /*
     * Vocabulary Used: pending -> requests which are NOT yet sent to RM
     * scheduled -> requests which are sent to RM but not yet assigned assigned
     * -> requests which are assigned to a container completed -> request
     * corresponding to which container has completed
     *
     * Lifecycle of map scheduled->assigned->completed
     *
     * Lifecycle of reduce pending->scheduled->assigned->completed
     *
     * Maps are scheduled as soon as their requests are received. Reduces are
     * added to the pending and are ramped up (added to scheduled) based on
     * completed maps and current availability in the cluster.
     */
    //reduces which are not yet scheduled
    private final LinkedList<ContainerRequest> pendingReduces =
            new LinkedList<ContainerRequest>();
    //holds information about the assigned containers to task attempts
    private final AssignedRequests assignedRequests = new AssignedRequests();
    //holds scheduled requests to be fulfilled by RM
    private final ScheduledRequests scheduledRequests = new ScheduledRequests();
    private int containersAllocated = 0;
    private int containersReleased = 0;
    private int hostLocalAssigned = 0;
    private int rackLocalAssigned = 0;
    private int lastCompletedTasks = 0;
    private boolean recalculateReduceSchedule = false;
    private int mapResourceReqt = Integer.MAX_VALUE;//memory
    private int reduceResourceReqt = Integer.MAX_VALUE;//memory
    private boolean reduceStarted = false;
    private float maxReduceRampupLimit = 0;
    private float maxReducePreemptionLimit = 0;
    private float reduceSlowStart = 0;
    private long retryInterval;
    private long retrystartTime;
    BlockingQueue<ContainerAllocatorEvent> eventQueue = new LinkedBlockingQueue<ContainerAllocatorEvent>();
    private ScheduleStats scheduleStats = new ScheduleStats();

    public RMContainerAllocatorWithCap(ClientService clientService, AppContext context) {
        super(clientService, context);
        this.stopped = new AtomicBoolean(false);
    }

    @Override
    protected void serviceInit(Configuration conf) throws Exception {
        super.serviceInit(conf);
        reduceSlowStart = conf.getFloat(
                MRJobConfig.COMPLETED_MAPS_FOR_REDUCE_SLOWSTART,
                DEFAULT_COMPLETED_MAPS_PERCENT_FOR_REDUCE_SLOWSTART);
        maxReduceRampupLimit = conf.getFloat(
                MRJobConfig.MR_AM_JOB_REDUCE_RAMPUP_UP_LIMIT,
                MRJobConfig.DEFAULT_MR_AM_JOB_REDUCE_RAMP_UP_LIMIT);
        maxReducePreemptionLimit = conf.getFloat(
                MRJobConfig.MR_AM_JOB_REDUCE_PREEMPTION_LIMIT,
                MRJobConfig.DEFAULT_MR_AM_JOB_REDUCE_PREEMPTION_LIMIT);
        RackResolver.init(conf);
        retryInterval = getConfig().getLong(MRJobConfig.MR_AM_TO_RM_WAIT_INTERVAL_MS,
                MRJobConfig.DEFAULT_MR_AM_TO_RM_WAIT_INTERVAL_MS);
        // Init startTime to current time. If all goes well, it will be reset after
        // first attempt to contact RM.
        retrystartTime = System.currentTimeMillis();
    }

    @Override
    protected void serviceStart() throws Exception {
        this.eventHandlingThread = new Thread() {

            @SuppressWarnings("unchecked")
            @Override
            public void run() {

                ContainerAllocatorEvent event;

                while (!stopped.get() && !Thread.currentThread().isInterrupted()) {
                    try {
                        event = RMContainerAllocatorWithCap.this.eventQueue.take();
                    } catch (InterruptedException e) {
                        if (!stopped.get()) {
                            LOG.error("Returning, interrupted : " + e);
                        }
                        return;
                    }

                    try {
                        handleEvent(event);
                    } catch (Throwable t) {
                        LOG.error("Error in handling event type " + event.getType()
                                + " to the ContainreAllocator", t);
                        // Kill the AM
                        eventHandler.handle(new JobEvent(getJob().getID(),
                                JobEventType.INTERNAL_ERROR));
                        return;
                    }
                }
            }
        };
        this.eventHandlingThread.start();
        super.serviceStart();
    }

    @Override
    protected synchronized void heartbeat() throws Exception {
        scheduleStats.updateAndLogIfChanged("Before Scheduling: ");
        List<Container> allocatedContainers = getResources();
        if (allocatedContainers.size() > 0) {
            scheduledRequests.assign(allocatedContainers);
        }

        int completedMaps = getJob().getCompletedMaps();
        int completedTasks = completedMaps + getJob().getCompletedReduces();
        if (lastCompletedTasks != completedTasks) {
            lastCompletedTasks = completedTasks;
            recalculateReduceSchedule = true;
        }

        if (recalculateReduceSchedule) {
            preemptReducesIfNeeded();
            scheduleReduces(
                    getJob().getTotalMaps(), completedMaps,
                    scheduledRequests.maps.size(), scheduledRequests.reduces.size(),
                    assignedRequests.maps.size(), assignedRequests.reduces.size(),
                    mapResourceReqt, reduceResourceReqt,
                    pendingReduces.size(),
                    maxReduceRampupLimit, reduceSlowStart);
            recalculateReduceSchedule = false;
        }

        scheduleStats.updateAndLogIfChanged("After Scheduling: ");
    }

    @Override
    protected void serviceStop() throws Exception {
        if (stopped.getAndSet(true)) {
            // return if already stopped
            return;
        }
        if (eventHandlingThread != null) {
            eventHandlingThread.interrupt();
        }
        super.serviceStop();
        scheduleStats.log("Final Stats: ");
    }

   
    public boolean getIsReduceStarted() {
        return reduceStarted;
    }


    public void setIsReduceStarted(boolean reduceStarted) {
        this.reduceStarted = reduceStarted;
    }

    @Override
    public void handle(ContainerAllocatorEvent event) {
        int qSize = eventQueue.size();
        if (qSize != 0 && qSize % 1000 == 0) {
            LOG.info("Size of event-queue in RMContainerAllocator is " + qSize);
        }
        int remCapacity = eventQueue.remainingCapacity();
        if (remCapacity < 1000) {
            LOG.warn("Very low remaining capacity in the event-queue "
                    + "of RMContainerAllocator: " + remCapacity);
        }
        try {
            eventQueue.put(event);
        } catch (InterruptedException e) {
            throw new YarnRuntimeException(e);
        }
    }


    @SuppressWarnings({"unchecked"})
    protected synchronized void handleEvent(ContainerAllocatorEvent event) {
        recalculateReduceSchedule = true;
        if (event.getType() == ContainerAllocator.EventType.CONTAINER_REQ) {
            ContainerRequestEvent reqEvent = (ContainerRequestEvent) event;
            JobId jobId = getJob().getID();
            int supportedMaxContainerCapability =
                    getMaxContainerCapability().getMemory();
            if (reqEvent.getAttemptID().getTaskId().getTaskType().equals(TaskType.MAP)) {
                int tmp_mapResourceReqt = reqEvent.getCapability().getMemory();
                eventHandler.handle(new JobHistoryEvent(jobId,
                        new NormalizedResourceEvent(org.apache.hadoop.mapreduce.TaskType.MAP,
                        tmp_mapResourceReqt)));
                LOG.info("mapResourceReqt:" + tmp_mapResourceReqt);
                if (tmp_mapResourceReqt > supportedMaxContainerCapability) {
                    String diagMsg = "MAP capability required is more than the supported "
                            + "max container capability in the cluster. Killing the Job. mapResourceReqt: "
                            + tmp_mapResourceReqt + " maxContainerCapability:" + supportedMaxContainerCapability;
                    LOG.info(diagMsg);
                    eventHandler.handle(new JobDiagnosticsUpdateEvent(
                            jobId, diagMsg));
                    eventHandler.handle(new JobEvent(jobId, JobEventType.JOB_KILL));
                }
                // }//limin
                //set the rounded off memory                
                scheduledRequests.addMap(reqEvent);//maps are immediately scheduled
            } else {
                int tmp_reduceResourceReqt = reqEvent.getCapability().getMemory();
                eventHandler.handle(new JobHistoryEvent(jobId,
                        new NormalizedResourceEvent(
                        org.apache.hadoop.mapreduce.TaskType.REDUCE,
                        tmp_reduceResourceReqt)));
                LOG.info("reduceResourceReqt:" + tmp_reduceResourceReqt);
                if (tmp_reduceResourceReqt > supportedMaxContainerCapability) {
                    String diagMsg = "REDUCE capability required is more than the "
                            + "supported max container capability in the cluster. Killing the "
                            + "Job. reduceResourceReqt: " + tmp_reduceResourceReqt
                            + " maxContainerCapability:" + supportedMaxContainerCapability;
                    LOG.info(diagMsg);
                    eventHandler.handle(new JobDiagnosticsUpdateEvent(
                            jobId, diagMsg));
                    eventHandler.handle(new JobEvent(jobId, JobEventType.JOB_KILL));
                }
                //  }//limin
                //set the rounded off memory
                //  reqEvent.getCapability().setMemory(reduceResourceReqt);//limin
                if (reqEvent.getEarlierAttemptFailed()) {
                    //add to the front of queue for fail fast
                    pendingReduces.addFirst(new ContainerRequest(reqEvent, PRIORITY_REDUCE));
                } else {
                    pendingReduces.add(new ContainerRequest(reqEvent, PRIORITY_REDUCE));
                    //reduces are added to pending and are slowly ramped up
                }
            }

        } else if (event.getType() == ContainerAllocator.EventType.CONTAINER_DEALLOCATE) {

            LOG.info("Processing the event " + event.toString());

            TaskAttemptId aId = event.getAttemptID();

            boolean removed = scheduledRequests.remove(aId);
            if (!removed) {
                ContainerId containerId = assignedRequests.get(aId);
                if (containerId != null) {
                    removed = true;
                    assignedRequests.remove(aId);
                    containersReleased++;
                    release(containerId);
                }
            }
            if (!removed) {
                LOG.error("Could not deallocate container for task attemptId "
                        + aId);
            }
        } else if (event.getType() == ContainerAllocator.EventType.CONTAINER_FAILED) {
            ContainerFailedEvent fEv = (ContainerFailedEvent) event;
            String host = getHost(fEv.getContMgrAddress());
            containerFailedOnHost(host);
        }
    }

    private static String getHost(String contMgrAddress) {
        String host = contMgrAddress;
        String[] hostport = host.split(":");
        if (hostport.length == 2) {
            host = hostport[0];
        }
        return host;
    }

    private void preemptReducesIfNeeded() {
        if (reduceResourceReqt == 0) {
            return; //no reduces
        }
        //check if reduces have taken over the whole cluster and there are 
        //unassigned maps
        if (scheduledRequests.maps.size() > 0) {
            int memLimit = getMemLimit();
            int availableMemForMap = memLimit - ((assignedRequests.reduces.size()
                    - assignedRequests.preemptionWaitingReduces.size()) * reduceResourceReqt);
            //availableMemForMap must be sufficient to run atleast 1 map
            if (availableMemForMap < mapResourceReqt) {
                //to make sure new containers are given to maps and not reduces
                //ramp down all scheduled reduces if any
                //(since reduces are scheduled at higher priority than maps)
                LOG.info("Ramping down all scheduled reduces:" + scheduledRequests.reduces.size());
                for (ContainerRequest req : scheduledRequests.reduces.values()) {
                    pendingReduces.add(req);
                }
                scheduledRequests.reduces.clear();

                //preempt for making space for atleast one map
                int premeptionLimit = Math.max(mapResourceReqt,
                        (int) (maxReducePreemptionLimit * memLimit));

                int preemptMem = Math.min(scheduledRequests.maps.size() * mapResourceReqt,
                        premeptionLimit);

                int toPreempt = (int) Math.ceil((float) preemptMem / reduceResourceReqt);
                toPreempt = Math.min(toPreempt, assignedRequests.reduces.size());

                LOG.info("Going to preempt " + toPreempt);
                assignedRequests.preemptReduce(toPreempt);
            }
        }
    }

    @Private
    public void scheduleReduces(
            int totalMaps, int completedMaps,
            int scheduledMaps, int scheduledReduces,
            int assignedMaps, int assignedReduces,
            int mapResourceReqt, int reduceResourceReqt,
            int numPendingReduces,
            float maxReduceRampupLimit, float reduceSlowStart) {

        if (numPendingReduces == 0) {
            return;
        }

        int headRoom = getAvailableResources() != null
                ? getAvailableResources().getMemory() : 0;
        LOG.info("Recalculating schedule, headroom=" + headRoom);

        //check for slow start
        if (!getIsReduceStarted()) {//not set yet
            int completedMapsForReduceSlowstart = (int) Math.ceil(reduceSlowStart
                    * totalMaps);
            if (completedMaps < completedMapsForReduceSlowstart) {
                LOG.info("Reduce slow start threshold not met. "
                        + "completedMapsForReduceSlowstart "
                        + completedMapsForReduceSlowstart);
                return;
            } else {
                LOG.info("Reduce slow start threshold reached. Scheduling reduces.");
                setIsReduceStarted(true);
            }
        }

        //if all maps are assigned, then ramp up all reduces irrespective of the
        //headroom
        if (scheduledMaps == 0 && numPendingReduces > 0) {
            LOG.info("All maps assigned. "
                    + "Ramping up all remaining reduces:" + numPendingReduces);
            scheduleAllReduces();
            return;
        }

        float completedMapPercent = 0f;
        if (totalMaps != 0) {//support for 0 maps
            completedMapPercent = (float) completedMaps / totalMaps;
        } else {
            completedMapPercent = 1;
        }
        int netScheduledMapMem = 0;
        int netScheduledReduceMem = 0;
        if (this.scheduledRequests != null && this.assignedRequests != null) {
            netScheduledMapMem = this.scheduledRequests.getMapTotalMem()
                    + this.assignedRequests.getMapTotalMem();//limin
            netScheduledReduceMem = this.scheduledRequests.getReduceTotalMem()//limin
                    + this.assignedRequests.getReduceTotalMem();
        } else {
            netScheduledMapMem = (scheduledMaps + assignedMaps) * mapResourceReqt;
            netScheduledReduceMem = (scheduledReduces + assignedReduces) * reduceResourceReqt;
        }




        int finalMapMemLimit = 0;
        int finalReduceMemLimit = 0;

        // ramp up the reduces based on completed map percentage
        int totalMemLimit = getMemLimit();
        int idealReduceMemLimit =
                Math.min(
                (int) (completedMapPercent * totalMemLimit),
                (int) (maxReduceRampupLimit * totalMemLimit));
        int idealMapMemLimit = totalMemLimit - idealReduceMemLimit;

        // check if there aren't enough maps scheduled, give the free map capacity
        // to reduce
        if (idealMapMemLimit > netScheduledMapMem) {
            int unusedMapMemLimit = idealMapMemLimit - netScheduledMapMem;
            finalReduceMemLimit = idealReduceMemLimit + unusedMapMemLimit;
            finalMapMemLimit = totalMemLimit - finalReduceMemLimit;
        } else {
            finalMapMemLimit = idealMapMemLimit;
            finalReduceMemLimit = idealReduceMemLimit;
        }

        LOG.info("completedMapPercent " + completedMapPercent
                + " totalMemLimit:" + totalMemLimit
                + " finalMapMemLimit:" + finalMapMemLimit
                + " finalReduceMemLimit:" + finalReduceMemLimit
                + " netScheduledMapMem:" + netScheduledMapMem
                + " netScheduledReduceMem:" + netScheduledReduceMem);

        int rampUp = 0;
        if (this.scheduledRequests != null) {
            rampUp = (finalReduceMemLimit - netScheduledReduceMem)
                    / this.scheduledRequests.get_min_mem(PRIORITY_REDUCE);//todo: doublecheck
        } else {
            rampUp = (finalReduceMemLimit - netScheduledReduceMem) / reduceResourceReqt;//limin
        }


        if (rampUp > 0) {
            rampUp = Math.min(rampUp, numPendingReduces);
            LOG.info("Ramping up " + rampUp);
            rampUpReduces(rampUp);
        } else if (rampUp < 0) {
            int rampDown = -1 * rampUp;
            rampDown = Math.min(rampDown, scheduledReduces);
            LOG.info("Ramping down " + rampDown);
            rampDownReduces(rampDown);
        }
    }

    @Private
    public void scheduleAllReduces() {
        for (ContainerRequest req : pendingReduces) {
            scheduledRequests.addReduce(req);
        }
        pendingReduces.clear();
    }

    @Private
    public void rampUpReduces(int rampUp) {
        //more reduce to be scheduled
        for (int i = 0; i < rampUp; i++) {
            ContainerRequest request = pendingReduces.removeFirst();
            scheduledRequests.addReduce(request);
        }
    }

    @Private
    public void rampDownReduces(int rampDown) {
        //remove from the scheduled and move back to pending
        for (int i = 0; i < rampDown; i++) {
            ContainerRequest request = scheduledRequests.removeReduce();
            pendingReduces.add(request);
        }
    }

    @SuppressWarnings("unchecked")
    private List<Container> getResources() throws Exception {
        int headRoom = getAvailableResources() != null
                ? getAvailableResources().getMemory() : 0;//first time it would be null
        AllocateResponse response;
        /*
         * If contact with RM is lost, the AM will wait
         * MR_AM_TO_RM_WAIT_INTERVAL_MS milliseconds before aborting. During
         * this interval, AM will still try to contact the RM.
         */
        try {
            response = makeRemoteRequest();
            // Reset retry count if no exception occurred.
            retrystartTime = System.currentTimeMillis();
        } catch (Exception e) {
            // This can happen when the connection to the RM has gone down. Keep
            // re-trying until the retryInterval has expired.
            if (System.currentTimeMillis() - retrystartTime >= retryInterval) {
                LOG.error("Could not contact RM after " + retryInterval + " milliseconds.");
                eventHandler.handle(new JobEvent(this.getJob().getID(),
                        JobEventType.INTERNAL_ERROR));
                throw new YarnRuntimeException("Could not contact RM after "
                        + retryInterval + " milliseconds.");
            }
            // Throw this up to the caller, which may decide to ignore it and
            // continue to attempt to contact the RM.
            throw e;
        }
        if (response.getAMCommand() != null) {
            switch (response.getAMCommand()) {
                case AM_RESYNC:
                case AM_SHUTDOWN:
                    // This can happen if the RM has been restarted. If it is in that state,
                    // this application must clean itself up.
                    eventHandler.handle(new JobEvent(this.getJob().getID(),
                            JobEventType.JOB_AM_REBOOT));
                    throw new YarnRuntimeException("Resource Manager doesn't recognize AttemptId: "
                            + this.getContext().getApplicationID());
                default:
                    String msg =
                            "Unhandled value of AMCommand: " + response.getAMCommand();
                    LOG.error(msg);
                    throw new YarnRuntimeException(msg);
            }
        }
        int newHeadRoom = getAvailableResources() != null ? getAvailableResources().getMemory() : 0;
        List<Container> newContainers = response.getAllocatedContainers();
        // Setting NMTokens
        if (response.getNMTokens() != null) {
            for (NMToken nmToken : response.getNMTokens()) {
                NMTokenCache.setNMToken(nmToken.getNodeId().toString(),
                        nmToken.getToken());
            }
        }

        List<ContainerStatus> finishedContainers = response.getCompletedContainersStatuses();
        if (newContainers.size() + finishedContainers.size() > 0 || headRoom != newHeadRoom) {
            //something changed
            recalculateReduceSchedule = true;
            if (LOG.isDebugEnabled() && headRoom != newHeadRoom) {
                LOG.debug("headroom=" + newHeadRoom);
            }
        }

        if (LOG.isDebugEnabled()) {
            for (Container cont : newContainers) {
                LOG.debug("Received new Container :" + cont);
            }
        }

        //Called on each allocation. Will know about newly blacklisted/added hosts.
        computeIgnoreBlacklisting();

        handleUpdatedNodes(response);

        for (ContainerStatus cont : finishedContainers) {
            LOG.info("Received completed container " + cont.getContainerId());
            TaskAttemptId attemptID = assignedRequests.get(cont.getContainerId());
            if (attemptID == null) {
                LOG.error("Container complete event for unknown container id "
                        + cont.getContainerId());
            } else {
                assignedRequests.remove(attemptID);

                // send the container completed event to Task attempt
                eventHandler.handle(createContainerFinishedEvent(cont, attemptID));

                // Send the diagnostics
                String diagnostics = StringInterner.weakIntern(cont.getDiagnostics());
                eventHandler.handle(new TaskAttemptDiagnosticsUpdateEvent(attemptID,
                        diagnostics));
            }
        }
        return newContainers;
    }

    @VisibleForTesting
    public TaskAttemptEvent createContainerFinishedEvent(ContainerStatus cont,
            TaskAttemptId attemptID) {
        if (cont.getExitStatus() == ContainerExitStatus.ABORTED) {
            // killed by framework
            return new TaskAttemptEvent(attemptID,
                    TaskAttemptEventType.TA_KILL);
        } else {
            return new TaskAttemptEvent(attemptID,
                    TaskAttemptEventType.TA_CONTAINER_COMPLETED);
        }
    }

    @SuppressWarnings("unchecked")
    private void handleUpdatedNodes(AllocateResponse response) {
        // send event to the job about on updated nodes
        List<NodeReport> updatedNodes = response.getUpdatedNodes();
        if (!updatedNodes.isEmpty()) {

            // send event to the job to act upon completed tasks
            eventHandler.handle(new JobUpdatedNodesEvent(getJob().getID(),
                    updatedNodes));

            // act upon running tasks
            HashSet<NodeId> unusableNodes = new HashSet<NodeId>();
            for (NodeReport nr : updatedNodes) {
                NodeState nodeState = nr.getNodeState();
                if (nodeState.isUnusable()) {
                    unusableNodes.add(nr.getNodeId());
                }
            }
            for (int i = 0; i < 2; ++i) {
                HashMap<TaskAttemptId, Container> taskSet = i == 0 ? assignedRequests.maps
                        : assignedRequests.reduces;
                // kill running containers
                for (Map.Entry<TaskAttemptId, Container> entry : taskSet.entrySet()) {
                    TaskAttemptId tid = entry.getKey();
                    NodeId taskAttemptNodeId = entry.getValue().getNodeId();
                    if (unusableNodes.contains(taskAttemptNodeId)) {
                        LOG.info("Killing taskAttempt:" + tid
                                + " because it is running on unusable node:"
                                + taskAttemptNodeId);
                        eventHandler.handle(new TaskAttemptKillEvent(tid,
                                "TaskAttempt killed because it ran on unusable node"
                                + taskAttemptNodeId));
                    }
                }
            }
        }
    }

    @Private
    public int getMemLimit() {
        int headRoom = getAvailableResources() != null ? getAvailableResources().getMemory() : 0;
        int org = headRoom + assignedRequests.maps.size() * mapResourceReqt
                + assignedRequests.reduces.size() * reduceResourceReqt;//limin
        return headRoom + assignedRequests._map_total_mem
                + assignedRequests._reduce_total_mem;//limin

    }
    /*
     * private ScheduledRequests getScheduledRequests(){ return
     * this.scheduledRequests; } private AssignedRequests getAssignedRequests(){
     * return this.assignedRequests;
  }
     */

    private class ScheduledRequests {

        private final LinkedList<TaskAttemptId> earlierFailedMaps =
                new LinkedList<TaskAttemptId>();
        /**
         * Maps from a host to a list of Map tasks with data on the host
         */
        private final Map<String, LinkedList<TaskAttemptId>> mapsHostMapping =
                new HashMap<String, LinkedList<TaskAttemptId>>();
        private final Map<String, LinkedList<TaskAttemptId>> mapsRackMapping =
                new HashMap<String, LinkedList<TaskAttemptId>>();
        private final Map<TaskAttemptId, ContainerRequest> maps =
                new LinkedHashMap<TaskAttemptId, ContainerRequest>();
        private int _maps_total_mem = 0;//limin
        private final LinkedHashMap<TaskAttemptId, ContainerRequest> reduces =
                new LinkedHashMap<TaskAttemptId, ContainerRequest>();
        private int _reduce_total_mem = 0;//limin

        int getMapTotalMem() {
            return this._maps_total_mem;
        }

        int getReduceTotalMem() {
            return this._reduce_total_mem;
        }

        boolean remove(TaskAttemptId tId) {
            ContainerRequest req = null;
            if (tId.getTaskId().getTaskType().equals(TaskType.MAP)) {
                req = maps.remove(tId);
                this._maps_total_mem -= req.capability.getMemory();//limin
            } else {
                req = reduces.remove(tId);
                this._reduce_total_mem -= req.capability.getMemory();//limin
            }

            if (req == null) {
                return false;
            } else {
                decContainerReq(req);
                return true;
            }
        }

        ContainerRequest removeReduce() {
            Iterator<Entry<TaskAttemptId, ContainerRequest>> it = reduces.entrySet().iterator();
            if (it.hasNext()) {
                Entry<TaskAttemptId, ContainerRequest> entry = it.next();
                this._reduce_total_mem -= entry.getValue().capability.getMemory();//limin
                it.remove();
                decContainerReq(entry.getValue());
                return entry.getValue();
            }
            return null;
        }

        void addMap(ContainerRequestEvent event) {
            ContainerRequest request = null;

            if (event.getEarlierAttemptFailed()) {
                earlierFailedMaps.add(event.getAttemptID());
                request = new ContainerRequest(event, PRIORITY_FAST_FAIL_MAP);
                LOG.info("Added " + event.getAttemptID() + " to list of failed maps");
            } else {
                for (String host : event.getHosts()) {
                    LinkedList<TaskAttemptId> list = mapsHostMapping.get(host);
                    if (list == null) {
                        list = new LinkedList<TaskAttemptId>();
                        mapsHostMapping.put(host, list);
                    }
                    list.add(event.getAttemptID());
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("Added attempt req to host " + host);
                    }
                }
                for (String rack : event.getRacks()) {
                    LinkedList<TaskAttemptId> list = mapsRackMapping.get(rack);
                    if (list == null) {
                        list = new LinkedList<TaskAttemptId>();
                        mapsRackMapping.put(rack, list);
                    }
                    list.add(event.getAttemptID());
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("Added attempt req to rack " + rack);
                    }
                }
                request = new ContainerRequest(event, PRIORITY_MAP);
            }
            maps.put(event.getAttemptID(), request);
            this._maps_total_mem += event.getCapability().getMemory();//limin
            addContainerReq(request);
        }

        int get_min_mem(Priority priority) {
            int res = Integer.MAX_VALUE;
            Iterator it = null;
            if (PRIORITY_FAST_FAIL_MAP.equals(priority) || PRIORITY_MAP.equals(priority)) {
                it = maps.entrySet().iterator();
            } else {
                it = reduces.entrySet().iterator();
            }
            while (it.hasNext()) {
                Map.Entry<TaskAttemptId, ContainerRequest> pairs = (Map.Entry) it.next();
                int tmp = pairs.getValue().capability.getMemory();
                res = res < tmp ? res : tmp;
            }
            return res;
        }

        void addReduce(ContainerRequest req) {
            this._reduce_total_mem += req.capability.getMemory();//limin
            reduces.put(req.attemptID, req);
            addContainerReq(req);
        }

        // this method will change the list of allocatedContainers.
        private void assign(List<Container> allocatedContainers) {
            Iterator<Container> it = allocatedContainers.iterator();
            LOG.info("Got allocated containers " + allocatedContainers.size());
            containersAllocated += allocatedContainers.size();
            while (it.hasNext()) {
                Container allocated = it.next();
                

                // check if allocated container meets memory requirements 
                // and whether we have any scheduled tasks that need 
                // a container to be assigned
                boolean isAssignable = true;
                Priority priority = allocated.getPriority();
                int allocatedMemory = allocated.getResource().getMemory();
                if (PRIORITY_FAST_FAIL_MAP.equals(priority)
                        || PRIORITY_MAP.equals(priority)) {
                    int mem = this.get_min_mem(priority);
                    if (allocatedMemory < mem//mapResourceReqt
                            || maps.isEmpty()) {
                        LOG.info("Cannot assign container " + allocated
                                + " for a map as either "
                                + " container memory less than required " + mem//mapResourceReqt
                                + " or no pending map tasks - maps.isEmpty="
                                + maps.isEmpty());
                        isAssignable = false;
                    }
                } else if (PRIORITY_REDUCE.equals(priority)) {
                    int mem = this.get_min_mem(priority);
                    if (allocatedMemory < mem//reduceResourceReqt
                            || reduces.isEmpty()) {
                        LOG.info("Cannot assign container " + allocated
                                + " for a reduce as either "
                                + " container memory less than required " + mem//reduceResourceReqt
                                + " or no pending reduce tasks - reduces.isEmpty="
                                + reduces.isEmpty());
                        isAssignable = false;
                    }
                } else {
                    LOG.warn("Container allocated at unwanted priority: " + priority
                            + ". Returning to RM...");
                    isAssignable = false;
                }

                if (!isAssignable) {
                    // release container if we could not assign it 
                    containerNotAssigned(allocated);
                    it.remove();
                    continue;
                }

                // do not assign if allocated container is on a  
                // blacklisted host
                String allocatedHost = allocated.getNodeId().getHost();
                if (isNodeBlacklisted(allocatedHost)) {
                    // we need to request for a new container 
                    // and release the current one
                    LOG.info("Got allocated container on a blacklisted "
                            + " host " + allocatedHost
                            + ". Releasing container " + allocated);

                    // find the request matching this allocated container 
                    // and replace it with a new one 
                    ContainerRequest toBeReplacedReq =
                            getContainerReqToReplace(allocated);
                    if (toBeReplacedReq != null) {
                        LOG.info("Placing a new container request for task attempt "
                                + toBeReplacedReq.attemptID);
                        ContainerRequest newReq =
                                getFilteredContainerRequest(toBeReplacedReq);
                        decContainerReq(toBeReplacedReq);
                        if (toBeReplacedReq.attemptID.getTaskId().getTaskType()
                                == TaskType.MAP) {
                            maps.put(newReq.attemptID, newReq);
                            this._maps_total_mem += newReq.capability.getMemory();//limin
                        } else {
                            reduces.put(newReq.attemptID, newReq);
                            this._reduce_total_mem += newReq.capability.getMemory();//limin
                        }
                        addContainerReq(newReq);
                    } else {
                        LOG.info("Could not map allocated container to a valid request."
                                + " Releasing allocated container " + allocated);
                    }

                    // release container if we could not assign it 
                    containerNotAssigned(allocated);
                    it.remove();
                    continue;
                }
            }

            assignContainers(allocatedContainers);

            // release container if we could not assign it 
            it = allocatedContainers.iterator();
            while (it.hasNext()) {
                Container allocated = it.next();
                LOG.info("Releasing unassigned and invalid container "
                        + allocated + ". RM may have assignment issues");
                containerNotAssigned(allocated);
            }
        }

        @SuppressWarnings("unchecked")
        private void containerAssigned(Container allocated,
                ContainerRequest assigned) {
         
            if (LOG.isDebugEnabled()) {
                LOG.info("Assigned container (" + allocated + ") "
                        + " with capacity " + allocated.getResource().toString()
                        + " to task " + assigned.attemptID + " withCap " + assigned.capability.toString()
                        + " on node "
                        + allocated.getNodeId().toString());
            }
            // Update resource requests
            decContainerReq(assigned);

            // send the container-assigned event to task attempt
            eventHandler.handle(new TaskAttemptContainerAssignedEvent(
                    assigned.attemptID, allocated, applicationACLs));

            assignedRequests.add(allocated, assigned.attemptID);

            
        }

        private void containerNotAssigned(Container allocated) {
            containersReleased++;
            release(allocated.getId());
        }

        private ContainerRequest assignWithoutLocality(Container allocated) {
            ContainerRequest assigned = null;

            Priority priority = allocated.getPriority();
            if (PRIORITY_FAST_FAIL_MAP.equals(priority)) {
                LOG.info("Assigning container " + allocated + " to fast fail map");
                assigned = assignToFailedMap(allocated);
            } else if (PRIORITY_REDUCE.equals(priority)) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Assigning container " + allocated + " to reduce");
                }
                assigned = assignToReduce(allocated);
            }

            return assigned;
        }

        private void assignContainers(List<Container> allocatedContainers) {
            Iterator<Container> it = allocatedContainers.iterator();
            while (it.hasNext()) {
                Container allocated = it.next();
                ContainerRequest assigned = assignWithoutLocality(allocated);
                if (assigned != null) {
                    containerAssigned(allocated, assigned);
                    it.remove();
                }
            }

            assignMapsWithLocality(allocatedContainers);
        }

        private ContainerRequest getContainerReqToReplace(Container allocated) {
            LOG.info("Finding containerReq for allocated container: " + allocated);
            Priority priority = allocated.getPriority();
            ContainerRequest toBeReplaced = null;
            if (PRIORITY_FAST_FAIL_MAP.equals(priority)) {
                LOG.info("Replacing FAST_FAIL_MAP container " + allocated.getId());
                Iterator<TaskAttemptId> iter = earlierFailedMaps.iterator();
                while (toBeReplaced == null && iter.hasNext()) {
                    toBeReplaced = maps.get(iter.next());
                }
                LOG.info("Found replacement: " + toBeReplaced);
                return toBeReplaced;
            } else if (PRIORITY_MAP.equals(priority)) {
                LOG.info("Replacing MAP container " + allocated.getId());
                // allocated container was for a map
                String host = allocated.getNodeId().getHost();
                LinkedList<TaskAttemptId> list = mapsHostMapping.get(host);
                if (list != null && list.size() > 0) {
                    TaskAttemptId tId = list.removeLast();
                    if (maps.containsKey(tId)) {
                        toBeReplaced = maps.remove(tId);
                        this._maps_total_mem -= toBeReplaced.capability.getMemory();//limin
                    }
                } else {
                    TaskAttemptId tId = maps.keySet().iterator().next();
                    toBeReplaced = maps.remove(tId);
                    this._maps_total_mem -= toBeReplaced.capability.getMemory();//limin
                }
            } else if (PRIORITY_REDUCE.equals(priority)) {
                TaskAttemptId tId = reduces.keySet().iterator().next();
                toBeReplaced = reduces.remove(tId);
                this._reduce_total_mem -= toBeReplaced.capability.getMemory();//limin
            }
            LOG.info("Found replacement: " + toBeReplaced);
            return toBeReplaced;
        }

        //find the attempt that satisfy the container requirement;  
        @SuppressWarnings("unchecked")
        private ContainerRequest assignToFailedMap(Container allocated) {
                                        if (LOG.isDebugEnabled()) {
                    LOG.debug("Assigning container " + allocated.getId()
                            + " with priority " + allocated.getPriority() + " to NM "
                            + allocated.getNodeId());
                }
            //try to assign to earlierFailedMaps if present
            ContainerRequest assigned = null;
            while (assigned == null && earlierFailedMaps.size() > 0) {
                TaskAttemptId tId = earlierFailedMaps.removeFirst();       //todo:double check        
                if (maps.containsKey(tId)) {
                    assigned = maps.get(tId);
                    int mem = assigned.capability.getMemory();
                    int a_mem = allocated.getResource().getMemory();
                    if (mem <= a_mem && mem + 256 >= a_mem
                            && assigned.capability.getVirtualCores() == allocated.getResource().getVirtualCores()) {
                        maps.remove(tId);
                        this._maps_total_mem -= assigned.capability.getMemory();//limin
                        JobCounterUpdateEvent jce =
                                new JobCounterUpdateEvent(assigned.attemptID.getTaskId().getJobId());
                        jce.addCounterUpdate(JobCounter.OTHER_LOCAL_MAPS, 1);
                        eventHandler.handle(jce);
                        LOG.info("Assigned from earlierFailedMaps");
                        break;
                    }

                }
            }
            return assigned;
        }

        private ContainerRequest assignToReduce(Container allocated) {
                            if (LOG.isDebugEnabled()) {
                    LOG.debug("Assigning container " + allocated.getId()
                            + " with priority " + allocated.getPriority() + " to NM "
                            + allocated.getNodeId());
                }
            ContainerRequest assigned = null;
            //try to assign to reduces if present
            Iterator it = reduces.entrySet().iterator();
            while (assigned == null && it.hasNext()) {
                Map.Entry<TaskAttemptId, ContainerRequest> entry = (Map.Entry) it.next();
                assigned = entry.getValue();
                int mem = assigned.capability.getMemory();
                int a_mem = allocated.getResource().getMemory();
                if (mem <= a_mem && mem + 256 >= a_mem
                        && assigned.capability.getVirtualCores() == allocated.getResource().getVirtualCores()) {
                    reduces.remove(entry.getKey());
                    this._reduce_total_mem -= assigned.capability.getMemory();//limin
                }
            }
            return assigned;
        }

        @SuppressWarnings("unchecked")
        private void assignMapsWithLocality(List<Container> allocatedContainers) {
            // try to assign to all nodes first to match node local
            Iterator<Container> it = allocatedContainers.iterator();
            while (it.hasNext() && maps.size() > 0) {
                Container allocated = it.next();

                Priority priority = allocated.getPriority();
                assert PRIORITY_MAP.equals(priority);
                // "if (maps.containsKey(tId))" below should be almost always true.
                // hence this while loop would almost always have O(1) complexity
                String host = allocated.getNodeId().getHost();
                LinkedList<TaskAttemptId> list = mapsHostMapping.get(host);
                int i=0;
                //LinkedList<TaskAttemptId> tmplist=new LinkedList<TaskAttemptId>();
                while (list != null && i<list.size()) {
                    TaskAttemptId tId = list.get(i++);//list.removeFirst();//todo: double check                    
                    if (maps.containsKey(tId)) {
                        if (LOG.isDebugEnabled()) {
                            LOG.debug("Assigning container " + allocated.getId()
                                    + " with priority " + allocated.getPriority() + " to NM "
                                    + allocated.getNodeId());
                        }

                        ContainerRequest assigned;//= maps.remove(tId);
                        assigned = maps.get(tId);
                        int mem = assigned.capability.getMemory();
                        int a_mem = allocated.getResource().getMemory();
                        if (mem <= a_mem && mem + 256 >= a_mem
                                && assigned.capability.getVirtualCores() == allocated.getResource().getVirtualCores()) {
                            this._maps_total_mem -= assigned.capability.getMemory();//limin
                            containerAssigned(allocated, assigned);
                            list.remove(tId);
                            maps.remove(tId);
                            it.remove();
                            JobCounterUpdateEvent jce =
                                    new JobCounterUpdateEvent(assigned.attemptID.getTaskId().getJobId());
                            jce.addCounterUpdate(JobCounter.DATA_LOCAL_MAPS, 1);
                            eventHandler.handle(jce);
                            hostLocalAssigned++;
                            if (LOG.isDebugEnabled()) {
                                LOG.debug("Assigned based on host match " + host);
                            }
                            break;
                        }


                    }
                }
            }
            
            // try to match all rack local
            it = allocatedContainers.iterator();
            while (it.hasNext() && maps.size() > 0) {
                Container allocated = it.next();
                Priority priority = allocated.getPriority();
                assert PRIORITY_MAP.equals(priority);
                // "if (maps.containsKey(tId))" below should be almost always true.
                // hence this while loop would almost always have O(1) complexity
                String host = allocated.getNodeId().getHost();
                String rack = RackResolver.resolve(host).getNetworkLocation();
                LinkedList<TaskAttemptId> list = mapsRackMapping.get(rack);
                int i=0;
                while (list != null && i<list.size()) {
                    TaskAttemptId tId = list.get(i++);//list.removeFirst();
                    if (LOG.isDebugEnabled()) {
                               LOG.debug("Rack " + rack+" temptId "+tId.toString());
                    }
                    if (maps.containsKey(tId)) {
                        if (LOG.isDebugEnabled()) {
                            LOG.debug("Assigning container " + allocated.getId()
                                    + " with priority " + allocated.getPriority() + " to NM "
                                    + allocated.getNodeId());
                        }

                        ContainerRequest assigned;//= maps.remove(tId);
                        assigned = maps.get(tId);
                        int mem = assigned.capability.getMemory();
                        int a_mem = allocated.getResource().getMemory();
                        if (mem <= a_mem && mem + 256 >= a_mem
                                && assigned.capability.getVirtualCores() == allocated.getResource().getVirtualCores()) {
                            this._maps_total_mem -= assigned.capability.getMemory();//limin
                            containerAssigned(allocated, assigned);
                            maps.remove(tId);
                            list.remove(tId);
                            it.remove();
                            JobCounterUpdateEvent jce =
                                    new JobCounterUpdateEvent(assigned.attemptID.getTaskId().getJobId());
                            jce.addCounterUpdate(JobCounter.RACK_LOCAL_MAPS, 1);
                            eventHandler.handle(jce);
                            rackLocalAssigned++;
                            if (LOG.isDebugEnabled()) {
                                LOG.debug("Assigned based on rack match " + rack);
                            }
                            break;
                        }
                    }
                }
            }

            // assign remaining
            it = allocatedContainers.iterator();
            while (it.hasNext() && maps.size() > 0) {
                Container allocated = it.next();
                Priority priority = allocated.getPriority();
                assert PRIORITY_MAP.equals(priority);
                TaskAttemptId tId;// = maps.keySet().iterator().next();
                ContainerRequest assigned = null;// = maps.remove(tId);
                Iterator itm = maps.entrySet().iterator();
                while (assigned == null && itm.hasNext()) {//find a task; 
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("Assigning container " + allocated.getId()
                                + " with priority " + allocated.getPriority() + " to NM "
                                + allocated.getNodeId());
                    }

                    Map.Entry<TaskAttemptId, ContainerRequest> pairs = (Map.Entry) itm.next();
                    assigned = pairs.getValue();//maps.get(tId);
                    tId = pairs.getKey();
                    int mem = assigned.capability.getMemory();
                    int a_mem = allocated.getResource().getMemory();
                    if (mem <= a_mem && mem + 256 >= a_mem
                            && assigned.capability.getVirtualCores() == allocated.getResource().getVirtualCores()) {
                        this._maps_total_mem -= assigned.capability.getMemory();//limin
                        containerAssigned(allocated, assigned);
                        it.remove();
                        maps.remove(tId);
                        JobCounterUpdateEvent jce =
                                new JobCounterUpdateEvent(assigned.attemptID.getTaskId().getJobId());
                        jce.addCounterUpdate(JobCounter.OTHER_LOCAL_MAPS, 1);
                        eventHandler.handle(jce);
                        if (LOG.isDebugEnabled()) {
                            LOG.debug("Assigned based on * match");
                        }
                        break;
                    }
                }
            }//end offswitch
        }    //end assignMapWithLocality()
    }//end class

    private class AssignedRequests {

        private final Map<ContainerId, TaskAttemptId> containerToAttemptMap =
                new HashMap<ContainerId, TaskAttemptId>();
        private final LinkedHashMap<TaskAttemptId, Container> maps =
                new LinkedHashMap<TaskAttemptId, Container>();
        private int _map_total_mem = 0;//limin
        private final LinkedHashMap<TaskAttemptId, Container> reduces =
                new LinkedHashMap<TaskAttemptId, Container>();
        private int _reduce_total_mem = 0;//limin
        private final Set<TaskAttemptId> preemptionWaitingReduces =
                new HashSet<TaskAttemptId>();

        int getMapTotalMem() {
            return this._map_total_mem;
        }

        int getReduceTotalMem() {
            return this._reduce_total_mem;
        }

        void add(Container container, TaskAttemptId tId) {
            //LOG.info("Assigned container " + container.getId().toString() + " to " + tId);
            containerToAttemptMap.put(container.getId(), tId);
            if (tId.getTaskId().getTaskType().equals(TaskType.MAP)) {
                maps.put(tId, container);
                this._map_total_mem += container.getResource().getMemory();//limin
            } else {
                reduces.put(tId, container);
                this._reduce_total_mem += container.getResource().getMemory();//limin
            }
        }

        @SuppressWarnings("unchecked")
        void preemptReduce(int toPreempt) {
            List<TaskAttemptId> reduceList = new ArrayList<TaskAttemptId>(reduces.keySet());
            //sort reduces on progress
            Collections.sort(reduceList,
                    new Comparator<TaskAttemptId>() {

                        @Override
                        public int compare(TaskAttemptId o1, TaskAttemptId o2) {
                            float p = getJob().getTask(o1.getTaskId()).getAttempt(o1).getProgress()
                                    - getJob().getTask(o2.getTaskId()).getAttempt(o2).getProgress();
                            return p >= 0 ? 1 : -1;
                        }
                    });

            for (int i = 0; i < toPreempt && reduceList.size() > 0; i++) {
                TaskAttemptId id = reduceList.remove(0);//remove the one on top
                LOG.info("Preempting " + id);
                preemptionWaitingReduces.add(id);
                eventHandler.handle(new TaskAttemptEvent(id, TaskAttemptEventType.TA_KILL));
            }
        }

        boolean remove(TaskAttemptId tId) {
            ContainerId containerId = null;
            if (tId.getTaskId().getTaskType().equals(TaskType.MAP)) {
                Container cid = maps.remove(tId);//limin
                containerId = cid.getId();
                this._map_total_mem -= cid.getResource().getMemory();//limin
            } else {
                Container cid = reduces.remove(tId);//limin
                containerId = cid.getId();
                this._reduce_total_mem -= cid.getResource().getMemory();
                if (containerId != null) {
                    boolean preempted = preemptionWaitingReduces.remove(tId);
                    if (preempted) {
                        LOG.info("Reduce preemption successful " + tId);
                    }
                }
            }

            if (containerId != null) {
                containerToAttemptMap.remove(containerId);
                return true;
            }
            return false;
        }

        TaskAttemptId get(ContainerId cId) {
            return containerToAttemptMap.get(cId);
        }

        ContainerId get(TaskAttemptId tId) {
            Container taskContainer;
            if (tId.getTaskId().getTaskType().equals(TaskType.MAP)) {
                taskContainer = maps.get(tId);
            } else {
                taskContainer = reduces.get(tId);
            }

            if (taskContainer == null) {
                return null;
            } else {
                return taskContainer.getId();
            }
        }
    }

    private class ScheduleStats {

        int numPendingReduces;
        int numScheduledMaps;
        int numScheduledReduces;
        int numAssignedMaps;
        int numAssignedReduces;
        int numCompletedMaps;
        int numCompletedReduces;
        int numContainersAllocated;
        int numContainersReleased;

        public void updateAndLogIfChanged(String msgPrefix) {
            boolean changed = false;

            // synchronized to fix findbug warnings
            synchronized (RMContainerAllocatorWithCap.this) {
                changed |= (numPendingReduces != pendingReduces.size());
                numPendingReduces = pendingReduces.size();
                changed |= (numScheduledMaps != scheduledRequests.maps.size());
                numScheduledMaps = scheduledRequests.maps.size();
                changed |= (numScheduledReduces != scheduledRequests.reduces.size());
                numScheduledReduces = scheduledRequests.reduces.size();
                changed |= (numAssignedMaps != assignedRequests.maps.size());
                numAssignedMaps = assignedRequests.maps.size();
                changed |= (numAssignedReduces != assignedRequests.reduces.size());
                numAssignedReduces = assignedRequests.reduces.size();
                changed |= (numCompletedMaps != getJob().getCompletedMaps());
                numCompletedMaps = getJob().getCompletedMaps();
                changed |= (numCompletedReduces != getJob().getCompletedReduces());
                numCompletedReduces = getJob().getCompletedReduces();
                changed |= (numContainersAllocated != containersAllocated);
                numContainersAllocated = containersAllocated;
                changed |= (numContainersReleased != containersReleased);
                numContainersReleased = containersReleased;
            }

            if (changed) {
                log(msgPrefix);
            }
        }

        public void log(String msgPrefix) {
            LOG.info(msgPrefix + "PendingReds:" + numPendingReduces
                    + " ScheduledMaps:" + numScheduledMaps
                    + " ScheduledReds:" + numScheduledReduces
                    + " AssignedMaps:" + numAssignedMaps
                    + " AssignedReds:" + numAssignedReduces
                    + " CompletedMaps:" + numCompletedMaps
                    + " CompletedReds:" + numCompletedReduces
                    + " ContAlloc:" + numContainersAllocated
                    + " ContRel:" + numContainersReleased
                    + " HostLocal:" + hostLocalAssigned
                    + " RackLocal:" + rackLocalAssigned);
        }
    }
}
