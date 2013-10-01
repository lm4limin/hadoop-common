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
package org.apache.hadoop.mapreduce.v2.api.protocolrecords;


import org.apache.hadoop.mapreduce.v2.api.records.JobId;
import org.apache.hadoop.mapreduce.v2.api.records.ConfNamesValues;


/**
 *
 * @author limin
 */
public interface SetConfNamesValuesRequest {
    public abstract JobId getJobId();  
    public abstract void setJobId(JobId jobId);
    
    public abstract String getSource();      
    public abstract void setSource(String source);  
    
    public abstract ConfNamesValues getConfNamesValues();
    public abstract void setConfNamesValues(ConfNamesValues namesvalues);
  
}
