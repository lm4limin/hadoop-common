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
package org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.fica;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Stable;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.server.resourcemanager.RMAuditLogger;
import org.apache.hadoop.yarn.server.resourcemanager.RMAuditLogger.AuditConstants;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptState;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainer;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainerEventType;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainerFinishedEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainerImpl;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainerReservedEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNodeCleanContainerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ActiveUsersManager;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.Allocation;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.AppSchedulingInfo;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.AppSchedulingInfoWithCapacity;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.NodeType;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.Queue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerApplication;
import org.apache.hadoop.yarn.util.resource.Resources;
import org.apache.hadoop.yarn.util.resource.ResourceCalculator;

import com.google.common.collect.HashMultiset;
import com.google.common.collect.Multiset;
import java.util.*;
/**
 *
 * @author limin
 */
public class FiCaSchedulerAppWithCapacity extends FiCaSchedulerApp {
    private static final Log LOG = LogFactory.getLog(FiCaSchedulerAppWithCapacity.class);    
    AppSchedulingInfoWithCapacity appSchedulingInfoWithCapacity;

    public FiCaSchedulerAppWithCapacity(ApplicationAttemptId applicationAttemptId,
            String user, Queue queue, ActiveUsersManager activeUsersManager,
            RMContext rmContext) {
        super(applicationAttemptId, user, queue, activeUsersManager, rmContext);
        //this.appSchedulingInfoWithCapacity =
                this.appSchedulingInfo =this.appSchedulingInfoWithCapacity=
                new AppSchedulingInfoWithCapacity(applicationAttemptId, user, queue,
                activeUsersManager);
                

    }
    
    /*
    @Override
      public synchronized void stop(RMAppAttemptState rmAppAttemptFinalState) {
    // Cleanup all scheduling information
    this.isStopped = true;
    this.appSchedulingInfoWithCapacity.stop(rmAppAttemptFinalState);
  }
      @Override
  public boolean isPending() {
    return this.appSchedulingInfoWithCapacity.isPending();
  }


@Override
  public String getQueueName() {
    return this.appSchedulingInfoWithCapacity.getQueueName();
  }
    @Override
  public boolean isBlacklisted(String resourceName) {
    return this.appSchedulingInfoWithCapacity.isBlacklisted(resourceName);
  }
  @Override    
  public int getNewContainerId() {
    return this.appSchedulingInfoWithCapacity.getNewContainerId();
  }
  @Override  
  public Collection<Priority> getPriorities() {
    return this.appSchedulingInfoWithCapacity.getPriorities();
  }    
  @Override  
  public ApplicationId getApplicationId() {
    return this.appSchedulingInfoWithCapacity.getApplicationId();
  }

  @Override
  public ApplicationAttemptId getApplicationAttemptId() {
    return this.appSchedulingInfoWithCapacity.getApplicationAttemptId();
  }
@Override
  public String getUser() {
    return this.appSchedulingInfoWithCapacity.getUser();
  }    */


    public Map<Resource, ResourceRequest> getResourceRequestCap(Priority priority, String resourceName) {
        return this.appSchedulingInfoWithCapacity.getResourceRequestCap(priority, resourceName);
    }
    public ResourceRequest getSingleResourceRequestCap(Priority priority,String resourceName,Resource resource){
        return this.appSchedulingInfoWithCapacity.getSingleResourceRequestCap(priority, resourceName, resource);
    }
      public Map<String, Map<Resource, ResourceRequest>> getResourceRequestsCap(Priority priority) {
    return this.appSchedulingInfoWithCapacity.getResourceRequestsCap(priority);
  }

    public List<Resource> getResourceCap(Priority priority) {
        return this.appSchedulingInfoWithCapacity.getResourceCap(priority);
    }
      //the algorithm is the same; 
   @Override
    public synchronized float getLocalityWaitFactor(
      Priority priority, int clusterNodes) {
    // Estimate: Required unique resources (i.e. hosts + racks)
    int requiredResources = 
        Math.max(this.getResourceRequestsCap(priority).size() - 1, 0);
    
    // waitFactor can't be more than '1' 
    // i.e. no point skipping more than clustersize opportunities
    return Math.min(((float)requiredResources / clusterNodes), 1.0f);
  }
   
    public static long getNumContainers(Map<Resource, ResourceRequest> hm_req) {
        long requiredContainers = 0;//offSwitchRequest.getNumContainers(); 
        if (hm_req != null) {
            for (ResourceRequest req : hm_req.values()) {
                requiredContainers += req.getNumContainers();
            }
        }
        return requiredContainers;
    }
    // return a single Resource capturing the overal amount of pending resources
   @Override 
  public Resource getTotalPendingRequests() {
    Resource ret = Resource.newInstance(0, 0);
    for (ResourceRequest rr : appSchedulingInfoWithCapacity.getAllResourceRequestsCap()) {
      // to avoid double counting we count only "ANY" resource requests
      if (ResourceRequest.isAnyLocation(rr.getResourceName())){
        Resources.addTo(ret,
            Resources.multiply(rr.getCapability(), rr.getNumContainers()));
      }
    }
    return ret;
  }    
    @Override 
    public synchronized int getTotalRequiredResources(Priority priority) {
        Map<Resource,ResourceRequest> hm=getResourceRequestCap(priority, ResourceRequest.ANY);
        return (int)FiCaSchedulerAppWithCapacity.getNumContainers(hm);
  }
    public synchronized int getNumReservedContainers(Priority priority, Resource cap) {
        Map<NodeId, RMContainer> reservedContainers_l =
                this.reservedContainers.get(priority);
        if(reservedContainers_l==null) return 0;
        int res = 0;
        Iterator it = reservedContainers_l.entrySet().iterator();
        //return (reservedContainers_l == null) ? 0 : reservedContainers_l.size();
        while (it.hasNext()) {
            Map.Entry<NodeId, RMContainer> pair = (Map.Entry<NodeId, RMContainer>) it.next();
            if (pair.getValue().getContainer().getResource() == cap) {
                res++;
            }
        }
        return res;
    }
  @Override
  public synchronized void updateResourceRequests(
      List<ResourceRequest> requests, 
      List<String> blacklistAdditions, List<String> blacklistRemovals) {
    if (!isStopped) {
      this.appSchedulingInfoWithCapacity.updateResourceRequests(requests, 
          blacklistAdditions, blacklistRemovals);
    }
  }    
    @Override
    synchronized public void showRequests() {
        if (LOG.isDebugEnabled()) {
            LOG.debug("showRequests:" + " application=" + getApplicationId()
                    + " headRoom=" + getHeadroom()
                    + " currentConsumption=" + currentConsumption.getMemory());
            for (Priority priority : getPriorities()) {
                Map<String, Map<Resource, ResourceRequest>> hm_requests = getResourceRequestsCap(priority);
                if (hm_requests != null) {
                    for (String resourcename : hm_requests.keySet()) {
                        Map<Resource, ResourceRequest> hm_request = hm_requests.get(resourcename);
                        LOG.debug("resource " + resourcename);
                        if (hm_request != null) {
                            for (ResourceRequest request : hm_request.values()) {
                                LOG.debug("showRequests:" + " application=" + getApplicationId()
                                        + " request=" + request);
                            }
                        }
                    }
                }
            }
        }
    }
}
