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

import org.apache.hadoop.mapreduce.v2.api.records.ConfNamesValues;
import org.apache.hadoop.mapreduce.v2.api.records.JobId;

import org.apache.hadoop.mapreduce.v2.api.protocolrecords.SetConfNamesValuesRequest;
import org.apache.hadoop.mapreduce.v2.api.records.impl.pb.ConfNamesValuesPBImpl;
import org.apache.hadoop.mapreduce.v2.api.records.impl.pb.JobIdPBImpl;
import org.apache.hadoop.mapreduce.v2.proto.MRProtos.JobIdProto;
import org.apache.hadoop.mapreduce.v2.proto.MRProtos.ConfNamesValuesProto;
import org.apache.hadoop.mapreduce.v2.proto.MRServiceProtos.SetConfNamesValuesRequestProto;
import org.apache.hadoop.mapreduce.v2.proto.MRServiceProtos.SetConfNamesValuesRequestProtoOrBuilder;
import org.apache.hadoop.yarn.api.records.ProtoBase;

/**
 *
 * @author limin
 */
public class SetConfNamesValuesRequestPBImpl extends ProtoBase<SetConfNamesValuesRequestProto> implements SetConfNamesValuesRequest {

    SetConfNamesValuesRequestProto proto = SetConfNamesValuesRequestProto.getDefaultInstance();
    SetConfNamesValuesRequestProto.Builder builder = null;
    boolean viaProto = false;
    private JobId jobId = null;
    private String source = null;
    private ConfNamesValues confnamesvalues = null;

    public SetConfNamesValuesRequestPBImpl() {
        builder = SetConfNamesValuesRequestProto.newBuilder();
    }

    public SetConfNamesValuesRequestPBImpl(SetConfNamesValuesRequestProto proto) {
        this.proto = proto;
        viaProto = true;
    }

    public SetConfNamesValuesRequestProto getProto() {
        mergeLocalToProto();
        proto = viaProto ? proto : builder.build();
        viaProto = true;
        return proto;
    }

    private void mergeLocalToBuilder() {
        if (this.jobId != null) {
            builder.setJobId(convertToProtoFormat(this.jobId));
            builder.setSource(this.source);
            builder.setConfNamesValues(convertToProtoFormat(this.confnamesvalues));
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
            builder = SetConfNamesValuesRequestProto.newBuilder(proto);
        }
        viaProto = false;
    }

    @Override
    public JobId getJobId() {
        SetConfNamesValuesRequestProtoOrBuilder p = viaProto ? proto : builder;
        if (this.jobId != null) {
            return this.jobId;
        }
        if (!p.hasJobId()) {
            return null;
        }
        this.jobId = convertFromProtoFormat(p.getJobId());
        return this.jobId;
    }

    @Override
    public void setJobId(JobId jobId) {
        maybeInitBuilder();
        if (jobId == null) {
            builder.clearJobId();
        }
        this.jobId = jobId;
    }

    private JobIdPBImpl convertFromProtoFormat(JobIdProto p) {
        return new JobIdPBImpl(p);
    }

    private JobIdProto convertToProtoFormat(JobId t) {
        return ((JobIdPBImpl) t).getProto();
    }

    private ConfNamesValuesPBImpl convertFromProtoFormat(ConfNamesValuesProto p) {
        return new ConfNamesValuesPBImpl(p);
    }

    private ConfNamesValuesProto convertToProtoFormat(ConfNamesValues t) {
        return ((ConfNamesValuesPBImpl) t).getProto();
    }

    @Override
    public String getSource() {
        SetConfNamesValuesRequestProtoOrBuilder p = viaProto ? proto : builder;
        if (this.jobId != null && this.source != null) {
            return this.source;
        }
        if (!p.hasSource()) {
            return null;
        }
        this.source = p.getSource();
        return this.source;

    }

    @Override
    public void setSource(String source) {
        maybeInitBuilder();
        if (this.source == null) {
            builder.clearSource();
        }
        this.source = source;

    }

    @Override
    public ConfNamesValues getConfNamesValues() {
        SetConfNamesValuesRequestProtoOrBuilder p = viaProto ? proto : builder;
        if (this.confnamesvalues != null) {
            return this.confnamesvalues;
        }
        if (!p.hasConfNamesValues()) {
            return null;
        }
        this.confnamesvalues = convertFromProtoFormat(p.getConfNamesValues());
        return this.confnamesvalues;

    }

    @Override
    public void setConfNamesValues(ConfNamesValues namesvalues) {
        maybeInitBuilder();
        if (this.confnamesvalues == null) {
            builder.clearConfNamesValues();
        }
        this.confnamesvalues = namesvalues;
    }
}
