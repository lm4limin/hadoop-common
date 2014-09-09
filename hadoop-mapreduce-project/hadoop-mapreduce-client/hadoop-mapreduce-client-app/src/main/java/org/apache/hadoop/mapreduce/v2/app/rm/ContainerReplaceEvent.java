/*
 * Copyright 2014 Apache Software Foundation.
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

/**
 *
 * @author minli
 */
package org.apache.hadoop.mapreduce.v2.app.rm;
import org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptId;
import org.apache.hadoop.yarn.api.records.Resource;

public class ContainerReplaceEvent extends ContainerAllocatorEvent {
  private final Resource capability;
  private final String[] hosts;
  private final String[] racks;


  public ContainerReplaceEvent(TaskAttemptId attemptID, 
      Resource capability,
      String[] hosts, String[] racks) {
    super(attemptID, ContainerAllocator.EventType.CONTAINER_REPLACE);
    this.capability = capability;
    this.hosts = hosts;
    this.racks = racks;
  }
  

  

  public Resource getCapability() {
    return capability;
  }

  public String[] getHosts() {
    return hosts;
  }
  
  public String[] getRacks() {
    return racks;
  }
  
  
}
