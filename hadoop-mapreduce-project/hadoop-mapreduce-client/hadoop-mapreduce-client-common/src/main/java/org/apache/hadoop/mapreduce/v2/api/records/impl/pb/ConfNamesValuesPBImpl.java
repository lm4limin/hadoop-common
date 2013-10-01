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
package org.apache.hadoop.mapreduce.v2.api.records.impl.pb;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import org.apache.hadoop.mapreduce.v2.api.records.ConfNamesValues;
import org.apache.hadoop.mapreduce.v2.api.records.ConfNameValue;
import org.apache.hadoop.mapreduce.v2.api.records.Counter;
import org.apache.hadoop.mapreduce.v2.proto.MRProtos.ConfNamesValuesProto;
import org.apache.hadoop.mapreduce.v2.proto.MRProtos.ConfNameValueProto;
import org.apache.hadoop.mapreduce.v2.proto.MRProtos.ConfNamesValuesProtoOrBuilder;
import org.apache.hadoop.yarn.api.records.ProtoBase;
/**
 *
 * @author limin
 */
public class ConfNamesValuesPBImpl extends ProtoBase<ConfNamesValuesProto> implements ConfNamesValues {
  ConfNamesValuesProto proto = ConfNamesValuesProto.getDefaultInstance();
  ConfNamesValuesProto.Builder builder = null;
  boolean viaProto = false;
  private HashMap<String, String> namesvalues = null;
  public ConfNamesValuesPBImpl() {
    builder = ConfNamesValuesProto.newBuilder();
  }

  public ConfNamesValuesPBImpl(ConfNamesValuesProto proto) {
    this.proto = proto;
    viaProto = true;
  }
    public ConfNamesValuesProto getProto() {
        mergeLocalToProto();
        proto = viaProto ? proto : builder.build();
        viaProto = true;
        return proto;
    }
    //todo: double check the correctness;
   private void addNamesValuesToProto() {
    maybeInitBuilder();
    builder.clearNamesValues();
    if (namesvalues == null)
      return;
    Iterable<ConfNameValueProto> iterable = new Iterable<ConfNameValueProto>() {
      
      @Override
      public Iterator<ConfNameValueProto> iterator() {
        return new Iterator<ConfNameValueProto>() {
          
          Iterator<String> keyIter = namesvalues.keySet().iterator();
          
          @Override
          public void remove() {
            throw new UnsupportedOperationException();
          }
          
          @Override
          public ConfNameValueProto next() {
            String key = keyIter.next();
            return ConfNameValueProto.newBuilder().setName(key).setValue(namesvalues.get(key)).build();
          }
          
          @Override
          public boolean hasNext() {
            return keyIter.hasNext();
          }
        };
      }
    };
    builder.addAllNamesValues(iterable);
  }   
  private void mergeLocalToBuilder() {
    if (this.namesvalues != null) {
      addNamesValuesToProto();
    }
  }

    private void mergeLocalToProto() {
        if (viaProto) {
            maybeInitBuilder();
        }
        mergeLocalToBuilder();
        proto = builder.build();
        viaProto = true;
    }
  
  
  
  private void maybeInitBuilder() {
    if (viaProto || builder == null) {
      builder = ConfNamesValuesProto.newBuilder(proto);
    }
    viaProto = false;
  }
  
 
 // private ConfNameValuePBImpl convertFromProtoFormat(ConfNameValueProto p) {
 //   return new ConfNameValuePBImpl(p);
 // }

  //private ConfNameValueProto convertToProtoFormat(Counter t) {
  //  return ((ConfNameValuePBImpl)t).getProto();
  //} 
 private void initNamesValues() {
    if (this.namesvalues != null) {
      return;
    }
    ConfNamesValuesProtoOrBuilder p = viaProto ? proto : builder;
    List<ConfNameValueProto> list = p.getNamesValuesList();
    this.namesvalues = new HashMap<String, String>();

    for (ConfNameValueProto c : list) {
      this.namesvalues.put(c.getName(), c.getValue());
    }
  }  
  @Override
  public  HashMap<String,String> getAllNamesValues(){
      this.initNamesValues();
    return this.namesvalues;
  }
  
  @Override
  public void setConfNameValue(String key, String val) {
    this.initNamesValues();
    this.namesvalues.put(key, val);
  }  
 /* @Override
  public ConfNameValue getConfNameValue(String key) {
   this.initNamesValues();
    return this.counters.get(key);
  }  */
  @Override
  public void addAllNamesValues(HashMap<String, String> pairs){
  if (namesvalues == null)
      return;
    initNamesValues();
    this.namesvalues.putAll(pairs);
  }
  @Override
  public void clearNamesValues(){
      this.initNamesValues();
      this.namesvalues.clear();
  }
    
  
   
}
