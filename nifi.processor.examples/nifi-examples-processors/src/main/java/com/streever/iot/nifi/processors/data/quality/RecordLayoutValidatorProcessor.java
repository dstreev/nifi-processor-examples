/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.streever.iot.nifi.processors.data.quality;

import com.streever.data.quality.RecordLayoutValidator;
import com.streever.parsers.FilePartByRegEx;
import org.apache.nifi.annotation.behavior.ReadsAttribute;
import org.apache.nifi.annotation.behavior.ReadsAttributes;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.*;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.InputStreamCallback;
import org.apache.nifi.processor.io.OutputStreamCallback;
import org.apache.nifi.processor.util.StandardValidators;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;

@Tags({"RegEx","File","Part","Validator","Data Quality"})
@CapabilityDescription("Test a files records against a RegEx.")
@SeeAlso({})
@ReadsAttributes({@ReadsAttribute(attribute="", description="")})
@WritesAttributes({@WritesAttribute(attribute="", description="")})
public class RecordLayoutValidatorProcessor extends AbstractProcessor {


    public static final PropertyDescriptor REGEX = new PropertyDescriptor
            .Builder().name("Expected record layout RegEx")
            .description("Expected record layout Regex")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor HAS_HEADER = new PropertyDescriptor
            .Builder().name("Has header")
            .description("Has header")
            .required(true)
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .build();


    public static final Relationship SUCCESS = new Relationship.Builder()
            .name("success")
            .description("Success Relationship")
            .build();

    public static final Relationship FAILURE = new Relationship.Builder()
            .name("failure")
            .description("Failure Relationship")
            .build();

    public static final Relationship ERRORS = new Relationship.Builder()
            .name("errors")
            .description("Errors Relationship")
            .build();

    private List<PropertyDescriptor> descriptors;

    private Set<Relationship> relationships;

    @Override
    protected void init(final ProcessorInitializationContext context) {
        final List<PropertyDescriptor> descriptors = new ArrayList<PropertyDescriptor>();
//        descriptors.add(OCCURRENCE);
        descriptors.add(REGEX);
        descriptors.add(HAS_HEADER);
        this.descriptors = Collections.unmodifiableList(descriptors);

        final Set<Relationship> relationships = new HashSet<Relationship>();
        relationships.add(SUCCESS);
        relationships.add(FAILURE);
        relationships.add(ERRORS);
        this.relationships = Collections.unmodifiableSet(relationships);
    }

    @Override
    public Set<Relationship> getRelationships() {
        return this.relationships;
    }

    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return descriptors;
    }

    @OnScheduled
    public void onScheduled(final ProcessContext context) {

    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        FlowFile flowfile = session.get();

        final String regex = context.getProperty(REGEX).getValue();
        final Boolean hasHeader = Boolean.parseBoolean(context.getProperty(HAS_HEADER).getValue());

//        final AtomicReference<String> partValue = new AtomicReference<String>();

        final RecordLayoutValidator fp = new RecordLayoutValidator();
        fp.setExpectedRecordFormatRegEx(regex);
        fp.setHasHeader(hasHeader);

        if (flowfile == null) {
            return;
        }

        session.read(flowfile, new InputStreamCallback() {
            @Override
            public void process(InputStream in) throws IOException {
                try {

                    fp.setInputStream(in);
                    fp.validate();

                } catch (Exception ex) {
                    ex.printStackTrace();
                }
            }
        });

        boolean valid = fp.isValid();
        if (valid) {
            flowfile = session.putAttribute(flowfile, "record.count", Long.toString(fp.getRecordCount()));
            session.transfer(flowfile, SUCCESS);
        } else {
            session.transfer(flowfile, FAILURE);

            // To write the results back out ot flow file
            flowfile = session.write(flowfile, new OutputStreamCallback() {

                @Override
                public void process(OutputStream out) throws IOException {
                    Map<Long, String> errorMap = fp.getErrors();
                    Set<Long> keys = errorMap.keySet();

                    for (Long key: keys) {
                        String badLine = errorMap.get(key);
                        StringBuilder sb = new StringBuilder();
                        sb.append(key);
                        sb.append("\t");
                        sb.append(badLine);
                        out.write(sb.toString().getBytes());
                    }
                }
            });

            session.transfer(flowfile, ERRORS);

        }


    }
}
