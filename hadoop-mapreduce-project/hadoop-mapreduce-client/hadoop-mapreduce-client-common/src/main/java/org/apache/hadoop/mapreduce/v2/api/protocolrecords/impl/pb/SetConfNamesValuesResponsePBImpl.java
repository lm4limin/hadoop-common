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
package org.apache.hadoop.mapreduce.v2.api.protocolrecords.impl.pb;
import org.apache.hadoop.mapreduce.v2.api.protocolrecords.SetConfNamesValuesResponse;
import org.apache.hadoop.mapreduce.v2.proto.MRServiceProtos.SetConfNamesValuesResponseProto;
import org.apache.hadoop.yarn.api.records.ProtoBase;
/**
 *
 * @author limin
 */
public class SetConfNamesValuesResponsePBImpl extends ProtoBase<SetConfNamesValuesResponseProto> implements SetConfNamesValuesResponse {
  SetConfNamesValuesResponseProto proto = SetConfNamesValuesResponseProto.getDefaultInstance();
  SetConfNamesValuesResponseProto.Builder builder = null;
  boolean viaProto = false;
  
  public SetConfNamesValuesResponsePBImpl() {
    builder = SetConfNamesValuesResponseProto.newBuilder();
  }

  public SetConfNamesValuesResponsePBImpl(SetConfNamesValuesResponseProto proto) {
    this.proto = proto;
    viaProto = true;
  }
  
  public SetConfNamesValuesResponseProto getProto() {
    proto = viaProto ? proto : builder.build();
    viaProto = true;
    return proto;
  }

  private void maybeInitBuilder() {
    if (viaProto || builder == null) {
      builder = SetConfNamesValuesResponseProto.newBuilder(proto);
    }
    viaProto = false;
  }
    
  
    
}
