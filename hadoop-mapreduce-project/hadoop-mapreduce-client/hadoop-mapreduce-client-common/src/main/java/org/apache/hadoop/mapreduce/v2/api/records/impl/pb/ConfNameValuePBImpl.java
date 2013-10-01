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

import org.apache.hadoop.mapreduce.v2.api.records.ConfNameValue;
import org.apache.hadoop.mapreduce.v2.proto.MRProtos.ConfNameValueProto;
import org.apache.hadoop.mapreduce.v2.proto.MRProtos.ConfNameValueProtoOrBuilder;
import org.apache.hadoop.yarn.api.records.ProtoBase;

/**
 *
 * @author limin
 */
public class ConfNameValuePBImpl extends ProtoBase<ConfNameValueProto> implements ConfNameValue {
  ConfNameValueProto proto = ConfNameValueProto.getDefaultInstance();
  ConfNameValueProto.Builder builder = null;
  boolean viaProto = false;
  
  public ConfNameValuePBImpl() {
    builder = ConfNameValueProto.newBuilder();
  }

  public ConfNameValuePBImpl(ConfNameValueProto proto) {
    this.proto = proto;
    viaProto = true;
  }
    public ConfNameValueProto getProto() {
    proto = viaProto ? proto : builder.build();
    viaProto = true;
    return proto;
  }

  private void maybeInitBuilder() {
    if (viaProto || builder == null) {
      builder = ConfNameValueProto.newBuilder(proto);
    }
    viaProto = false;
  }
    
  
  @Override
  public String getName() {
    ConfNameValueProtoOrBuilder p = viaProto ? proto : builder;
    if (!p.hasName()) {
      return null;
    }
    return (p.getName());
  }

  @Override
  public void setName(String name) {
    maybeInitBuilder();
    if (name == null) {
      builder.clearName();
      return;
    }
    builder.setName((name));
  }
  @Override
  public String getValue() {
    ConfNameValueProtoOrBuilder p = viaProto ? proto : builder;
    return (p.getValue());
  }

  @Override
  public void setValue(String value) {
    maybeInitBuilder();
    builder.setValue((value));
  }
  
}