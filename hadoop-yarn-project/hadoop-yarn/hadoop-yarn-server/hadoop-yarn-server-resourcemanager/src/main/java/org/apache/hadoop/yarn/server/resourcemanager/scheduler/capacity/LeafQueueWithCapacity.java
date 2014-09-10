/*
 * Copyright 2013 Apache Software Foundation.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authorize.AccessControlList;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.QueueACL;
import org.apache.hadoop.yarn.api.records.QueueInfo;
import org.apache.hadoop.yarn.api.records.QueueState;
import org.apache.hadoop.yarn.api.records.QueueUserACLInfo;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.api.records.Token;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainer;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainerEventType;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainerState;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ActiveUsersManager;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.NodeType;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.QueueMetrics;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.fica.FiCaSchedulerApp;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.fica.FiCaSchedulerAppWithCapacity;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.fica.FiCaSchedulerNode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.fica.FiCaSchedulerUtils;
import org.apache.hadoop.yarn.server.resourcemanager.security.RMContainerTokenSecretManager;
import org.apache.hadoop.yarn.server.utils.BuilderUtils;
import org.apache.hadoop.yarn.server.utils.Lock;
import org.apache.hadoop.yarn.server.utils.Lock.NoLock;
import org.apache.hadoop.yarn.util.resource.ResourceCalculator;
import org.apache.hadoop.yarn.util.resource.Resources;
/**
 *
 * @author limin
 */
public class LeafQueueWithCapacity extends LeafQueue{
    private static final Log LOG = LogFactory.getLog(LeafQueueWithCapacity.class);
    
    public LeafQueueWithCapacity(CapacitySchedulerContext cs, 
      String queueName, CSQueue parent, CSQueue old) {
        super(cs,queueName,parent,old);
    }
    private static final CSAssignment SKIP_ASSIGNMENT = new CSAssignment(true);
      private static final CSAssignment NULL_ASSIGNMENT =
      new CSAssignment(Resources.createResource(0, 0), NodeType.NODE_LOCAL);
    
    private CSAssignment assignContainersOnNodeCap(Resource clusterResource,
            FiCaSchedulerNode node, FiCaSchedulerAppWithCapacity application,
            Priority priority, Resource resourceCap,RMContainer reservedContainer) {

        Resource assigned = Resources.none();

        
        // Data-local
        ResourceRequest nodeLocalResourceRequest =application.getSingleResourceRequestCap(priority, node.getHostName(), resourceCap);
                //application.getResourceRequest(priority, node.getHostName());
        if (nodeLocalResourceRequest != null) {
            assigned =
                    assignNodeLocalContainers(clusterResource, nodeLocalResourceRequest,
                    node, application, priority, reservedContainer);
            if (Resources.greaterThan(resourceCalculator, clusterResource,
                    assigned, Resources.none())) {
                return new CSAssignment(assigned, NodeType.NODE_LOCAL);
            }
        }

        // Rack-local
        ResourceRequest rackLocalResourceRequest =application.getSingleResourceRequestCap(priority, node.getRackName(), resourceCap);
              //  application.getResourceRequest(priority, node.getRackName());
        if (rackLocalResourceRequest != null) {
            if (!rackLocalResourceRequest.getRelaxLocality()) {
                return SKIP_ASSIGNMENT;
            }

            assigned =
                    assignRackLocalContainers(clusterResource, rackLocalResourceRequest,
                    node, application, priority, reservedContainer);
            if (Resources.greaterThan(resourceCalculator, clusterResource,
                    assigned, Resources.none())) {
                return new CSAssignment(assigned, NodeType.RACK_LOCAL);
            }
        }

        // Off-switch
        ResourceRequest offSwitchResourceRequest =application.getSingleResourceRequestCap(priority, ResourceRequest.ANY, resourceCap);
                //application.getResourceRequest(priority, ResourceRequest.ANY);
        if (offSwitchResourceRequest != null) {
            if (!offSwitchResourceRequest.getRelaxLocality()) {
                return SKIP_ASSIGNMENT;
            }

            return new CSAssignment(
                    assignOffSwitchContainers(clusterResource, offSwitchResourceRequest,
                    node, application, priority, reservedContainer),
                    NodeType.OFF_SWITCH);
        }
        if (LOG.isDebugEnabled()) {
            LOG.debug("assignContainerOnNodeCap:  skip assignment"
                    + " priority " + priority.toString()
                    + " cap " + resourceCap.toString());
            if (reservedContainer != null) {
                LOG.debug("reserved container"
                        + " priority " + reservedContainer.getReservedPriority().toString()
                        + " cap " + reservedContainer.getReservedResource().toString());
            }
        }
        return SKIP_ASSIGNMENT;
    }
    /*private CSAssignment assignContainersOnNode(Resource clusterResource,
            FiCaSchedulerNode node, FiCaSchedulerAppWithCapacity application,
            Priority priority, RMContainer reservedContainer) {

        Resource assigned = Resources.none();

        // Data-local
        //ResourceRequest nodeLocalResourceRequest =
        //       application.getResourceRequest(priority, node.getHostName());
        Map<Resource, ResourceRequest> hm_req =
                application.getResourceRequestCap(priority, node.getHostName());
        if (hm_req != null) {
            for (ResourceRequest nodeLocalResourceRequest : hm_req.values()) {
                if (nodeLocalResourceRequest.getNumContainers() <= 0) {
                    continue;
                }
                assigned =
                        assignNodeLocalContainers(clusterResource, nodeLocalResourceRequest,
                        node, application, priority, reservedContainer);
                if (Resources.greaterThan(resourceCalculator, clusterResource,
                        assigned, Resources.none())) {
                    return new CSAssignment(assigned, NodeType.NODE_LOCAL);
                }
            }

        }

        // Rack-local
        Map<Resource, ResourceRequest> hm_rackreq =
                application.getResourceRequestCap(priority, node.getRackName());
        if (hm_rackreq != null) {
            for (ResourceRequest rackLocalResourceRequest : hm_rackreq.values()) {
                if (!rackLocalResourceRequest.getRelaxLocality()) {
                    return SKIP_ASSIGNMENT;
                }
                if (rackLocalResourceRequest.getNumContainers() <= 0) {
                    continue;
                }
                assigned =
                        assignRackLocalContainers(clusterResource, rackLocalResourceRequest,
                        node, application, priority, reservedContainer);
                if (Resources.greaterThan(resourceCalculator, clusterResource,
                        assigned, Resources.none())) {
                    return new CSAssignment(assigned, NodeType.RACK_LOCAL);
                }
            }
        }

        // Off-switch
        Map<Resource, ResourceRequest> hm_offSwitchreq =
                application.getResourceRequestCap(priority, ResourceRequest.ANY);
        if (hm_offSwitchreq != null) {
            for (ResourceRequest offSwitchResourceRequest : hm_offSwitchreq.values()) {
                if (!offSwitchResourceRequest.getRelaxLocality()) {
                    return SKIP_ASSIGNMENT;
                }
                if (offSwitchResourceRequest.getNumContainers() <= 0) {
                    continue;
                }
                return new CSAssignment(
                        assignOffSwitchContainers(clusterResource, offSwitchResourceRequest,
                        node, application, priority, reservedContainer),
                        NodeType.OFF_SWITCH);
            }
        }
        return SKIP_ASSIGNMENT;
    }  */
    
    
    //get the first one; 
    /*private CSAssignment assignContainersOnNodeCap(Resource clusterResource,
            FiCaSchedulerNode node, FiCaSchedulerAppWithCapacity application,
            Priority priority, Resource resourcecap,RMContainer reservedContainer) {

        Resource assigned = Resources.none();

        // Data-local
        ResourceRequest nodeLocalResourceRequest = application.getSingleResourceRequestCap(priority, node.getHostName(), resourcecap);
        //       application.getResourceRequest(priority, node.getHostName());

        assigned =
                assignNodeLocalContainers(clusterResource, nodeLocalResourceRequest,
                node, application, priority, reservedContainer);
        if (Resources.greaterThan(resourceCalculator, clusterResource,
                assigned, Resources.none())) {
            return new CSAssignment(assigned, NodeType.NODE_LOCAL);
        }


        

        // Rack-local
        Map<Resource, ResourceRequest> hm_rackreq =
                application.getResourceRequestCap(priority, node.getRackName());
        if (hm_rackreq != null) {
            for (ResourceRequest rackLocalResourceRequest : hm_rackreq.values()) {
                if (!rackLocalResourceRequest.getRelaxLocality()) {
                    return SKIP_ASSIGNMENT;
                }
                if (rackLocalResourceRequest.getNumContainers() <= 0) {
                    continue;
                }
                assigned =
                        assignRackLocalContainers(clusterResource, rackLocalResourceRequest,
                        node, application, priority, reservedContainer);
                if (Resources.greaterThan(resourceCalculator, clusterResource,
                        assigned, Resources.none())) {
                    return new CSAssignment(assigned, NodeType.RACK_LOCAL);
                }
            }
        }

        // Off-switch
        Map<Resource, ResourceRequest> hm_offSwitchreq =
                application.getResourceRequestCap(priority, ResourceRequest.ANY);
        if (hm_offSwitchreq != null) {
            for (ResourceRequest offSwitchResourceRequest : hm_offSwitchreq.values()) {
                if (!offSwitchResourceRequest.getRelaxLocality()) {
                    return SKIP_ASSIGNMENT;
                }
                if (offSwitchResourceRequest.getNumContainers() <= 0) {
                    continue;
                }
                return new CSAssignment(
                        assignOffSwitchContainers(clusterResource, offSwitchResourceRequest,
                        node, application, priority, reservedContainer),
                        NodeType.OFF_SWITCH);
            }
        }
        return SKIP_ASSIGNMENT;
    }*/

      //private Resource assignNodeLocalContainers(
    @Override
    protected Resource assignNodeLocalContainers(
            Resource clusterResource, ResourceRequest nodeLocalResourceRequest,
            FiCaSchedulerNode node, FiCaSchedulerApp application,
            Priority priority, RMContainer reservedContainer) {
        if (canAssignCap(application, priority,nodeLocalResourceRequest.getCapability(), node, NodeType.NODE_LOCAL,
                reservedContainer)) {
            FiCaSchedulerAppWithCapacity app = (FiCaSchedulerAppWithCapacity) application;
            int num = nodeLocalResourceRequest.getNumContainers();

            ResourceRequest rackreq=app.getSingleResourceRequestCap(priority, node.getRackName(), nodeLocalResourceRequest.getCapability());
            int num1 = (rackreq==null)?0:rackreq.getNumContainers();
            if (num1 < num||num==0) {
                return Resources.none();
            }
            ResourceRequest offswitchreq=app.getSingleResourceRequestCap(priority, ResourceRequest.ANY, nodeLocalResourceRequest.getCapability());
            int num2 = (offswitchreq==null)? 0:offswitchreq.getNumContainers();
            if (num2 < num) {
                return Resources.none();
            }
            //return super.assignContainer(clusterResource, node, application, priority,
             //       nodeLocalResourceRequest, NodeType.NODE_LOCAL, reservedContainer);
            return assignContainer(clusterResource, node, application, priority,
                    nodeLocalResourceRequest, NodeType.NODE_LOCAL, reservedContainer);
        }

        return Resources.none();
    }
    @Override
    protected Resource assignRackLocalContainers(
            //private Resource assignRackLocalContainers(
            Resource clusterResource, ResourceRequest rackLocalResourceRequest,
            FiCaSchedulerNode node, FiCaSchedulerApp application, Priority priority,
            RMContainer reservedContainer) {
        if (canAssignCap(application, priority,rackLocalResourceRequest.getCapability(), node, NodeType.RACK_LOCAL,
                reservedContainer)) {
            FiCaSchedulerAppWithCapacity app = (FiCaSchedulerAppWithCapacity) application;
            int num = rackLocalResourceRequest.getNumContainers();
            ResourceRequest offswitchreq = app.getSingleResourceRequestCap(priority, ResourceRequest.ANY, rackLocalResourceRequest.getCapability());
            int num2 = (offswitchreq == null) ? 0 : offswitchreq.getNumContainers();
            if (num2 < num||num==0) {
                return Resources.none();
            }
            return assignContainer(clusterResource, node, application, priority,
                    rackLocalResourceRequest, NodeType.RACK_LOCAL, reservedContainer);
        }

        return Resources.none();
    }
    
    @Override
    protected Resource assignOffSwitchContainers(
            Resource clusterResource, ResourceRequest offSwitchResourceRequest,
            FiCaSchedulerNode node, FiCaSchedulerApp application, Priority priority,
            RMContainer reservedContainer) {
        if (canAssignCap(application, priority,offSwitchResourceRequest.getCapability(), node, NodeType.OFF_SWITCH,
                reservedContainer)) {
            
            FiCaSchedulerAppWithCapacity app = (FiCaSchedulerAppWithCapacity) application;
            int num = offSwitchResourceRequest.getNumContainers();
            ResourceRequest offswitchreq = app.getSingleResourceRequestCap(priority, ResourceRequest.ANY, offSwitchResourceRequest.getCapability());
           int num2 = (offswitchreq == null) ? 0 : offswitchreq.getNumContainers();
            if(num2<num||num==0){
                  return Resources.none();
            }
            return assignContainer(clusterResource, node, application, priority,
                    offSwitchResourceRequest, NodeType.OFF_SWITCH, reservedContainer);
        }

        return Resources.none();
    }
    /*
    //@Override
    boolean canAssign_old(FiCaSchedulerApp app, Priority priority,
            FiCaSchedulerNode node, NodeType type, RMContainer reservedContainer) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("canAssign:  node type "
                    + type);
            if (reservedContainer != null) {
                LOG.debug("reserved container"
                        + " priority " + reservedContainer.getReservedPriority().toString()
                        + " cap " + reservedContainer.getReservedResource().toString());
            }
        }
        FiCaSchedulerAppWithCapacity application = (FiCaSchedulerAppWithCapacity) app;
        long requiredContainers_off;
        // Clearly we need containers for this application...
        if (type == NodeType.OFF_SWITCH) {            
            if (reservedContainer != null) {
                return true;
            }
            // 'Delay' off-switch
            Map<Resource, ResourceRequest> hm_offSwitchRequest =
                    application.getResourceRequestCap(priority, ResourceRequest.ANY);
            
            long missedOpportunities = application.getSchedulingOpportunities(priority);
            requiredContainers_off = FiCaSchedulerAppWithCapacity.getNumContainers(hm_offSwitchRequest);//offSwitchRequest.getNumContainers(); 
            if(hm_offSwitchRequest==null||requiredContainers_off<=0){
                return false;
            }
            float localityWaitFactor =
                    application.getLocalityWaitFactor(priority,
                    scheduler.getNumClusterNodes());

            return ((requiredContainers_off * localityWaitFactor) < missedOpportunities);
        }

        // Check if we need containers on this rack 
        long requiredContainers_rack;
        Map<Resource, ResourceRequest> hm_rackLocalRequest =
                application.getResourceRequestCap(priority, node.getRackName());
        requiredContainers_rack=FiCaSchedulerAppWithCapacity.getNumContainers(hm_rackLocalRequest) ;
        if (hm_rackLocalRequest == null
                || requiredContainers_rack<= 0) {
            return false;
        }

        // If we are here, we do need containers on this rack for RACK_LOCAL req
        if (type == NodeType.RACK_LOCAL) {
            // 'Delay' rack-local just a little bit...
            long missedOpportunities = application.getSchedulingOpportunities(priority);
            return (Math.min(scheduler.getNumClusterNodes(), getNodeLocalityDelay())
                    < missedOpportunities);
        }

        // Check if we need containers on this host
        if (type == NodeType.NODE_LOCAL) {
            // Now check if we need containers on this host...
            Map<Resource, ResourceRequest> hm_nodeLocalRequest =
                    application.getResourceRequestCap(priority, node.getHostName());
            if (hm_nodeLocalRequest != null) {
          
                return FiCaSchedulerAppWithCapacity.getNumContainers(hm_nodeLocalRequest) > 0;
            }
        }
        if (LOG.isDebugEnabled()) {
            LOG.debug("canAssign:  false "
                    +type);
        }
        return false;
    }*/
    
    boolean canAssignCap(FiCaSchedulerApp app, Priority priority, Resource resourceCap,
            FiCaSchedulerNode node, NodeType type, RMContainer reservedContainer) {
        FiCaSchedulerAppWithCapacity application = (FiCaSchedulerAppWithCapacity) app;
        // Clearly we need containers for this application...
        if (type == NodeType.OFF_SWITCH) {
            if (reservedContainer != null) {
                return true;
            }

            // 'Delay' off-switch
            ResourceRequest offSwitchRequest =application.getSingleResourceRequestCap(priority, ResourceRequest.ANY, resourceCap);
                    //application.getResourceRequest(priority, ResourceRequest.ANY);
            long missedOpportunities = application.getSchedulingOpportunities(priority);
            long requiredContainers = offSwitchRequest.getNumContainers();

            float localityWaitFactor =
                    application.getLocalityWaitFactor(priority,
                    scheduler.getNumClusterNodes());

            return ((requiredContainers * localityWaitFactor) < missedOpportunities);
        }

        // Check if we need containers on this rack 
        ResourceRequest rackLocalRequest =application.getSingleResourceRequestCap(priority, node.getRackName(), resourceCap);
               // application.getResourceRequest(priority, node.getRackName());
        if (rackLocalRequest == null || rackLocalRequest.getNumContainers() <= 0) {
            return false;
        }

        // If we are here, we do need containers on this rack for RACK_LOCAL req
        if (type == NodeType.RACK_LOCAL) {
            // 'Delay' rack-local just a little bit...
            long missedOpportunities = application.getSchedulingOpportunities(priority);
            return (Math.min(scheduler.getNumClusterNodes(), getNodeLocalityDelay())
                    < missedOpportunities);
        }

        // Check if we need containers on this host
        if (type == NodeType.NODE_LOCAL) {
            // Now check if we need containers on this host...
            ResourceRequest nodeLocalRequest =application.getSingleResourceRequestCap(priority, node.getHostName(), resourceCap);
                  //  application.getResourceRequest(priority, node.getHostName());
            if (nodeLocalRequest != null) {
                return nodeLocalRequest.getNumContainers() > 0;
            }
        }

        return false;
    }
    @Override
    public synchronized CSAssignment assignContainers(Resource clusterResource, FiCaSchedulerNode node) {

        if (LOG.isDebugEnabled()) {
            LOG.debug("assignContainers: node=" + node.getHostName()
                    + " #applications=" + activeApplications.size());
        }

        // Check for reserved resources
        RMContainer reservedContainer = node.getReservedContainer();
        if (reservedContainer != null) {
            FiCaSchedulerAppWithCapacity application =
                    (FiCaSchedulerAppWithCapacity) getApplication(reservedContainer.getApplicationAttemptId());
            synchronized (application) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("assignContainers:  reserve container"                            
                            +" priority " +reservedContainer.getReservedPriority()
                            +" capacity " +reservedContainer.getReservedResource().toString());
                }
                return assignReservedContainer(application, node, reservedContainer,
                        clusterResource);
            }
        }

        // Try to assign containers to applications in order
        for (FiCaSchedulerApp app : activeApplications) {
            FiCaSchedulerAppWithCapacity application = (FiCaSchedulerAppWithCapacity) app;
            if (LOG.isDebugEnabled()) {
                LOG.debug("pre-assignContainers for application "
                        + application.getApplicationId());
                application.showRequests();
            }

            synchronized (application) {
                // Check if this resource is on the blacklist
                if (FiCaSchedulerUtils.isBlacklisted(application, node, LOG)) {
                    continue;
                }

                // Schedule in priority order
                for (Priority priority : application.getPriorities()) {
                    // Required resource

                    Map<Resource, ResourceRequest> hm_required =
                            application.getResourceRequestCap(priority, ResourceRequest.ANY);
                    if (hm_required == null) {
                        continue;
                    }
                    boolean isbreak = false;
                    for (Resource required : hm_required.keySet()) {
                        if (!needContainersCap(application, priority, required)) {
                            continue;
                        }

                        // Compute user-limit & set headroom
                        // Note: We compute both user-limit & headroom with the highest 
                        //       priority request as the target. 
                        //       This works since we never assign lower priority requests
                        //       before all higher priority ones are serviced.
                        Resource userLimit =
                                computeUserLimitAndSetHeadroom(application, clusterResource,
                                required);

                        // Check queue max-capacity limit
                        if (!assignToQueue(clusterResource, required)) {
                            return NULL_ASSIGNMENT;
                        }

                        // Check user limit
                        if (!assignToUser(
                                clusterResource, application.getUser(), userLimit)) {
                            isbreak = true;
                            break;
                        }

                        // Inform the application it is about to get a scheduling opportunity
                        application.addSchedulingOpportunity(priority);

                        // Try to schedule
                        CSAssignment assignment =
                                assignContainersOnNodeCap(clusterResource, node, application, priority,required,
                                null);

                        // Did the application skip this node?
                        if (assignment.getSkipped()) {
                            // Don't count 'skipped nodes' as a scheduling opportunity!
                            application.subtractSchedulingOpportunity(priority);
                            continue;
                        }

                        // Did we schedule or reserve a container?
                        Resource assigned = assignment.getResource();
                        if (Resources.greaterThan(
                                resourceCalculator, clusterResource, assigned, Resources.none())) {

                            // Book-keeping 
                            // Note: Update headroom to account for current allocation too...
                            allocateResource(clusterResource, application, assigned);

                            // Don't reset scheduling opportunities for non-local assignments
                            // otherwise the app will be delayed for each non-local assignment.
                            // This helps apps with many off-cluster requests schedule faster.
                            if (assignment.getType() != NodeType.OFF_SWITCH) {
                                application.resetSchedulingOpportunities(priority);
                            }

                            // Done
                            if (LOG.isDebugEnabled()) {
                                LOG.debug("post-assignContainers for application "
                                        + application.getApplicationId());
                                application.showRequests();
                            }
                            return assignment;
                        } else {
                            // Do not assign out of order w.r.t priorities
                            isbreak = true;
                           // break;
                        }
                    }//end resource iteration
                    if (isbreak) {
                        break;//end priority iteration
                    }

                    // Do we need containers at this 'priority'?

                }
            }

            if (LOG.isDebugEnabled()) {
                LOG.debug("post-assignContainers for application "
                        + application.getApplicationId());
                application.showRequests();
            }
            
        }

        return NULL_ASSIGNMENT;

    }
    @Override
    protected synchronized CSAssignment           
  assignReservedContainer(FiCaSchedulerApp app, 
      FiCaSchedulerNode node, RMContainer rmContainer, Resource clusterResource) {
    // Do we still need this reservation?
    FiCaSchedulerAppWithCapacity application=(FiCaSchedulerAppWithCapacity) app;
    Priority priority = rmContainer.getReservedPriority();
    if (application.getTotalRequiredResources(priority) == 0) {
      // Release
        if (LOG.isDebugEnabled()) {
            LOG.debug("assignReservedContainer container need 0 resources");
        }
        return new CSAssignment(application, rmContainer);
    }

    // Try to assign if we have sufficient resources
    assignContainersOnNodeCap(clusterResource, node, application, priority, rmContainer.getReservedResource(),
        rmContainer);
    
    // Doesn't matter... since it's already charged for at time of reservation
    // "re-reservation" is *free*
    return new CSAssignment(Resources.none(), NodeType.NODE_LOCAL);
  }
   // @Override
  /*
    boolean needContainers_old(FiCaSchedulerApp app, Priority priority, Resource required) {
        FiCaSchedulerAppWithCapacity application = (FiCaSchedulerAppWithCapacity) app;
        int requiredContainers = application.getTotalRequiredResources(priority);
        int reservedContainers = application.getNumReservedContainers(priority);
        int starvation = 0;
        if (reservedContainers > 0) {
            float nodeFactor =
                    Resources.ratio(
                    resourceCalculator, required, getMaximumAllocation());

            // Use percentage of node required to bias against large containers...
            // Protect against corner case where you need the whole node with
            // Math.min(nodeFactor, minimumAllocationFactor)
            starvation =
                    (int) ((application.getReReservations(priority) / (float) reservedContainers)
                    * (1.0f - (Math.min(nodeFactor, getMinimumAllocationFactor()))));

            if (LOG.isDebugEnabled()) {
                LOG.debug("needsContainers:"
                        + " app.#re-reserve=" + application.getReReservations(priority)
                        + " reserved=" + reservedContainers
                        + " nodeFactor=" + nodeFactor
                        + " minAllocFactor=" + getMinimumAllocationFactor()
                        + " starvation=" + starvation);
            }
        }
        return (((starvation + requiredContainers) - reservedContainers) > 0);
    }*/
    boolean needContainersCap(FiCaSchedulerAppWithCapacity application, 
            Priority priority, Resource required) {
        ResourceRequest req = application.getSingleResourceRequestCap(priority,
                ResourceRequest.ANY, required);// application.getTotalRequiredResources(priority);
        int requiredContainers = (req == null) ? 0 : req.getNumContainers();
        int reservedContainers = application.getNumReservedContainers(priority,required);
        int starvation = 0;
        float nodeFactor=0;
        if (reservedContainers > 0) {
             nodeFactor =
                    Resources.ratio(
                    resourceCalculator, required, getMaximumAllocation());

            // Use percentage of node required to bias against large containers...
            // Protect against corner case where you need the whole node with
            // Math.min(nodeFactor, minimumAllocationFactor)
            starvation =
                    (int) ((application.getReReservations(priority) / (float) reservedContainers)
                    * (1.0f - (Math.min(nodeFactor, getMinimumAllocationFactor()))));
        }
        boolean res = (((starvation + requiredContainers) - reservedContainers) > 0);
        if (LOG.isDebugEnabled()&&reservedContainers > 0) {
            LOG.debug("needsContainers:"
                    + " required= " + required.toString()
                    + " app.#re-reserve=" + application.getReReservations(priority)
                    + " reserved=" + reservedContainers
                    + " nodeFactor=" + nodeFactor
                    + " minAllocFactor=" + getMinimumAllocationFactor()
                    + " starvation=" + starvation                    
                    +" result="+ res);
        }
        return res;
    }
    
    @Override
    protected Resource assignContainer(Resource clusterResource, FiCaSchedulerNode node,
            FiCaSchedulerApp application, Priority priority,
            ResourceRequest request, NodeType type, RMContainer rmContainer) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("assignContainers: node=" + node.getHostName()
                    + " application=" + application.getApplicationId().getId()
                    + " priority=" + priority.getPriority()
                    + " request=" + request + " type=" + type);
        }
        Resource capability = request.getCapability();

        Resource available = node.getAvailableResource();

        assert Resources.greaterThan(
                resourceCalculator, clusterResource, available, Resources.none());
        int availableContainers
                = resourceCalculator.computeAvailableContainers(available, capability);

        // Can we allocate a container on this node?
        if (availableContainers <= 0) {
            // Reserve by 'charging' in advance...
            //reserve(application, priority, node, rmContainer, container);
            LOG.info("AssignedContainer: no available container, not assigned "
                    + " application=" + application.getApplicationId()
                    + " resource=" + request.getCapability()
                    + " queue=" + this.toString()
                    + " usedCapacity=" + getUsedCapacity()
                    + " absoluteUsedCapacity=" + getAbsoluteUsedCapacity()
                    + " used=" + usedResources
                    + " cluster=" + clusterResource);
            if (LOG.isDebugEnabled()) {
                application.showRequests();
            }
            return Resources.none();
        }
      //  if (availableContainers > 0) {
        // Allocate...
        // Create the container if necessary
        Container container
                = getContainer(rmContainer, application, node, capability, priority);

        // something went wrong getting/creating the container 
        if (container == null) {
            LOG.warn("Couldn't get container for allocation!");
            return Resources.none();
        }
        // Did we previously reserve containers at this 'priority'?
        if (rmContainer != null) {
            unreserve(application, priority, node, rmContainer);
        }

        Token containerToken
                = createContainerToken(application, container);
        if (containerToken == null) {
            // Something went wrong...
            return Resources.none();
        }
        container.setContainerToken(containerToken);

        // Inform the application
        RMContainer allocatedContainer
                = application.allocate(type, node, priority, request, container);

        // Does the application need this resource?
        if (allocatedContainer == null) {
            return Resources.none();
        }

        // Inform the node
        node.allocateContainer(application.getApplicationId(),
                allocatedContainer);

        LOG.info("AssignedContainer"
                + " application=" + application.getApplicationId()
                + " container=" + container
                + " containerId=" + container.getId()
                + " queue=" + this
                + " usedCapacity=" + getUsedCapacity()
                + " absoluteUsedCapacity=" + getAbsoluteUsedCapacity()
                + " used=" + usedResources
                + " cluster=" + clusterResource);
        if (LOG.isDebugEnabled()) {
            application.showRequests();
        }
        return container.getResource();
        //     } 
    }
    private Container getContainer(RMContainer rmContainer,
            FiCaSchedulerApp application, FiCaSchedulerNode node,
            Resource capability, Priority priority) {
        return (rmContainer != null) ? rmContainer.getContainer()
                : createContainer(application, node, capability, priority);
    }
    private boolean unreserve(FiCaSchedulerApp application, Priority priority,
            FiCaSchedulerNode node, RMContainer rmContainer) {
        // Done with the reservation?
        if (application.unreserve(node, priority)) {
            node.unreserveResource(application);

            // Update reserved metrics
            getMetrics().unreserveResource(
                    application.getUser(), rmContainer.getContainer().getResource());
            return true;
        }
        return false;
    }

    private void reserve(FiCaSchedulerApp application, Priority priority,
            FiCaSchedulerNode node, RMContainer rmContainer, Container container) {
        // Update reserved metrics if this is the first reservation
        if (rmContainer == null) {
            getMetrics().reserveResource(
                    application.getUser(), container.getResource());
        }

        // Inform the application 
        rmContainer = application.reserve(node, priority, rmContainer, container);

        // Update the node
        node.reserveResource(application, priority, rmContainer);
    }
}
