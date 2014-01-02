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
package org.apache.hadoop.yarn.server.resourcemanager.scheduler;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptState;
import org.apache.hadoop.yarn.util.resource.Resources;
/**
 *
 * @author limin
 */
public class AppSchedulingInfoWithCapacity extends AppSchedulingInfo{

  private static final Log LOG = LogFactory.getLog(AppSchedulingInfo.class);

  //requestsCap that have different capacities
  final Map<Priority, Map<String, Map<Resource, ResourceRequest>>> requestsCap = 
    new HashMap<Priority, Map<String, Map<Resource, ResourceRequest>>>();

  public AppSchedulingInfoWithCapacity(ApplicationAttemptId appAttemptId,
      String user, Queue queue, ActiveUsersManager activeUsersManager) {
      super(appAttemptId,user,queue,activeUsersManager);
 
  }
  /**
   * Clear any pending requests from this application.
   */
    
  private synchronized void clearRequests() {
    priorities.clear();
    requestsCap.clear();
    LOG.info("Application " + applicationId + " requests cleared");
  }

    
    public static int indexof(List<ResourceRequest> ls, ResourceRequest ask){
        if(ls==null||ask==null){
            return -1;
        }
        for(int i=0;i<ls.size();i++){
            ResourceRequest tmp=ls.get(i);
            if(tmp.getPriority()==ask.getPriority()){
                if(tmp.getResourceName().equals(ask.getResourceName())){
                    if(tmp.getCapability()==ask.getCapability()){
                        if(tmp.getRelaxLocality()==ask.getRelaxLocality()){
                            return i;
                        }                        
                    }
                }
            }
        }
        return -1;
    }
    
    @Override
    synchronized public void updateResourceRequests(
            List<ResourceRequest> ls_requests,
            List<String> blacklistAdditions, List<String> blacklistRemovals) {
                
        //====merge the request that are the same====
        ArrayList<ResourceRequest> requests=new ArrayList<ResourceRequest>();
        for (int i = 0; i < ls_requests.size(); i++) {
            ResourceRequest ask = ls_requests.get(i);
            int ind = indexof(requests, ask);
            if (ind == -1) {
                requests.add(ask);
            } else {
                ResourceRequest tmp = requests.get(ind);
                tmp.setNumContainers(tmp.getNumContainers() + ask.getNumContainers());
            }
        }           
        
        QueueMetrics metrics = queue.getMetrics();
        // Update resource requests
        for (ResourceRequest request : requests) {
            Priority priority = request.getPriority();
            String resourceName = request.getResourceName();
            boolean updatePendingResources = false;
            ResourceRequest lastRequest = null;

            if (resourceName.equals(ResourceRequest.ANY)) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("update:" + " application=" + applicationId + " request="
                            + request);
                }
                updatePendingResources = true;
                // Premature optimization?
                // Assumes that we won't see more than one priority request updated
                // in one call, reasonable assumption... however, it's totally safe
                // to activate same application more than once.
                // Thus we don't need another loop ala the one in decrementOutstanding()  
                // which is needed during deactivate.
                if (request.getNumContainers() > 0) {
                    super.activateApplication();
                }
            }

            //limin-begin
            Map<String, Map<Resource, ResourceRequest>> asksCap = this.requestsCap.get(priority);
            if (asksCap == null) {                
                asksCap = new HashMap<String, Map<Resource, ResourceRequest>>();
                this.priorities.add(priority);
                this.requestsCap.put(priority, asksCap);
            } 
            Map<Resource, ResourceRequest> hm = asksCap.get(resourceName);
            if (hm == null) {
                hm = new HashMap<Resource, ResourceRequest>();
                asksCap.put(resourceName, hm);
            }  else  if (updatePendingResources) {                
                lastRequest = asksCap.get(resourceName).get(request.getCapability());
            }            
            hm.put(request.getCapability(), request);                      

            if (updatePendingResources) {
                // Similarly, deactivate application?
                if (request.getNumContainers() <= 0) {
                    LOG.info("checking for deactivate... ");
                    checkForDeactivation();
                }
                int lastRequestContainers = lastRequest != null ? lastRequest.getNumContainers() : 0;
                Resource lastRequestCapability = lastRequest != null ? lastRequest.getCapability() : Resources.none();
                metrics.incrPendingResources(user, request.getNumContainers()
                        - lastRequestContainers, Resources.subtractFrom( // save a clone
                        Resources.multiply(request.getCapability(), request.getNumContainers()), Resources.multiply(lastRequestCapability,
                        lastRequestContainers)));
            }
        }
        
        //==== Update blacklist====        
        // Add to blacklist
        if (blacklistAdditions != null) {
            blacklist.addAll(blacklistAdditions);
        }
        // Remove from blacklist
        if (blacklistRemovals != null) {
            blacklist.removeAll(blacklistRemovals);
        }
    }


  synchronized public Map<String, Map<Resource,ResourceRequest>> getResourceRequestsCap(
      Priority priority) {
    return requestsCap.get(priority);
  }

  //values will return Collection<> which is not null. yet might be empty;
    synchronized public List<ResourceRequest> getAllResourceRequestsCap() {
        List<ResourceRequest> ret = new ArrayList<ResourceRequest>();
        for (Map<String, Map<Resource, ResourceRequest>> r : requestsCap.values()) {            
            for (Map<Resource, ResourceRequest> rr : r.values()) {
                ret.addAll(rr.values());
            }
        }
        return ret;
    }

  synchronized public Map<Resource,ResourceRequest>getResourceRequestCap(Priority priority,
      String resourceName) {
    Map<String, Map<Resource,ResourceRequest>> nodeRequests = requestsCap.get(priority);
    return (nodeRequests == null) ? null : nodeRequests.get(resourceName);
  }
  synchronized public ResourceRequest getSingleResourceRequestCap(Priority priority,String resourceName,Resource resource){
      Map<String, Map<Resource,ResourceRequest>> nodeRequests = requestsCap.get(priority);
     Map<Resource,ResourceRequest> hm=(nodeRequests == null) ? null : nodeRequests.get(resourceName);
     return (hm==null)? null:hm.get(resource);
  }
  
  public synchronized List<Resource> getResourceCap(Priority priority) {
    List<Resource> res=new ArrayList<Resource>();
      Map<Resource,ResourceRequest> request = getResourceRequestCap(priority, ResourceRequest.ANY);
    for(Resource resource : request.keySet()){
        res.add(resource);
    }
    return res;
  }




  /**
   * The {@link ResourceScheduler} is allocating data-local resources to the
   * application.
   * 
   * @param allocatedContainers
   *          resources allocated to the application
   */
  @Override
  synchronized protected void allocateNodeLocal( 
      SchedulerNode node, Priority priority, 
      ResourceRequest nodeLocalRequest, Container container) {
    // Update consumption and track allocations
    allocate(container);

    // Update future requirements
    nodeLocalRequest.setNumContainers(nodeLocalRequest.getNumContainers() - 1);
    if (nodeLocalRequest.getNumContainers() == 0) {      
      this.requestsCap.get(priority).get(node.getHostName()).remove(nodeLocalRequest.getCapability());              
    }

    ResourceRequest rackLocalRequest = requestsCap.get(priority).get(
        node.getRackName()).get(nodeLocalRequest.getCapability());
    rackLocalRequest.setNumContainers(rackLocalRequest.getNumContainers() - 1);
    if (rackLocalRequest.getNumContainers() == 0) {      
      this.requestsCap.get(priority).get(node.getRackName()).remove(nodeLocalRequest.getCapability());              
    }

    decrementOutstanding(requestsCap.get(priority).get(ResourceRequest.ANY).get(nodeLocalRequest.getCapability()));
  }

  /**
   * The {@link ResourceScheduler} is allocating data-local resources to the
   * application.
   * 
   * @param allocatedContainers
   *          resources allocated to the application
   */
  @Override
  synchronized protected void allocateRackLocal(
      SchedulerNode node, Priority priority,
      ResourceRequest rackLocalRequest, Container container) {

    // Update consumption and track allocations
    allocate(container);

    // Update future requirements
    rackLocalRequest.setNumContainers(rackLocalRequest.getNumContainers() - 1);
    if (rackLocalRequest.getNumContainers() == 0) {
      this.requestsCap.get(priority).get(node.getRackName()).remove(rackLocalRequest.getCapability());
    }

    decrementOutstanding(requestsCap.get(priority).get(ResourceRequest.ANY).get(rackLocalRequest.getCapability()));
  }

  /**
   * The {@link ResourceScheduler} is allocating data-local resources to the
   * application.
   * 
   * @param allocatedContainers
   *          resources allocated to the application
   */
  @Override
  synchronized protected void allocateOffSwitch(
      SchedulerNode node, Priority priority,
      ResourceRequest offSwitchRequest, Container container) {

    // Update consumption and track allocations
    allocate(container);

    // Update future requirements
    decrementOutstanding(offSwitchRequest);
  }


  //if any container number !=0; do not deactivate the application; 
  @Override
    synchronized protected void checkForDeactivation() {
        boolean deactivate = true;
        for (Priority priority : getPriorities()) {
            Map<Resource, ResourceRequest> hm_request = getResourceRequestCap(priority, ResourceRequest.ANY);
            if (hm_request == null) {
                continue;
            }
            for (ResourceRequest request : hm_request.values()) {
                if (request.getNumContainers() > 0) {
                    deactivate = false;
                    break;
                }
            }
            if (deactivate == false) {
                break;//break the outter loop;
            }
        }
        if (deactivate) {
            super.deactivateApplication();
        }
    }
  

@Override
  synchronized public void stop(RMAppAttemptState rmAppAttemptFinalState) {
    // clear pending resources metrics for the application
    QueueMetrics metrics = queue.getMetrics();
    
    for (Map<String, Map<Resource, ResourceRequest>> asks : requestsCap.values()) {
        Map<Resource, ResourceRequest> hm_request = asks.get(ResourceRequest.ANY);
        if (hm_request != null) {
            for (ResourceRequest request : hm_request.values()) {
                metrics.decrPendingResources(user, request.getNumContainers(),
                        Resources.multiply(request.getCapability(), request.getNumContainers()));
            }
        }
    }        
    metrics.finishApp(this, rmAppAttemptFinalState);    
    // Clear requests themselves
    clearRequests();
  }


}
