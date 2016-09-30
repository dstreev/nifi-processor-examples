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
package com.streever.iot.nifi.processors.examples;

import com.streever.parsers.FilePartByRegEx;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.*;
import org.apache.nifi.annotation.behavior.ReadsAttribute;
import org.apache.nifi.annotation.behavior.ReadsAttributes;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.InputStreamCallback;
import org.apache.nifi.processor.io.OutputStreamCallback;
import org.apache.nifi.processor.util.StandardValidators;

import java.io.*;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;

@Tags({"RegEx","File","Part","Parser"})
@CapabilityDescription("Extract content from file, based on RegEx Pattern")
@SeeAlso({})
@ReadsAttributes({@ReadsAttribute(attribute="", description="")})
@WritesAttributes({@WritesAttribute(attribute="", description="")})
public class FilePartByRegExProcessor extends AbstractProcessor {


    public static final PropertyDescriptor OCCURRENCE = new PropertyDescriptor
            .Builder().name("Value Occurrence Index")
            .description("Value Occurrence Index")
            .required(true)
            .addValidator(StandardValidators.INTEGER_VALIDATOR)
            .build();

    public static final PropertyDescriptor REGEX = new PropertyDescriptor
            .Builder().name("RegEx")
            .description("RegEx")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor REGEX_GROUP_SUPPORT = new PropertyDescriptor
            .Builder().name("RegEx Group Support")
            .description("RegEx Group Support")
            .required(true)
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .build();


    public static final Relationship SUCCESS = new Relationship.Builder()
            .name("SUCCESS")
            .description("Success Relationship")
            .build();

    private List<PropertyDescriptor> descriptors;

    private Set<Relationship> relationships;

    @Override
    protected void init(final ProcessorInitializationContext context) {
        final List<PropertyDescriptor> descriptors = new ArrayList<PropertyDescriptor>();
        descriptors.add(OCCURRENCE);
        descriptors.add(REGEX);
        descriptors.add(REGEX_GROUP_SUPPORT);
        this.descriptors = Collections.unmodifiableList(descriptors);

        final Set<Relationship> relationships = new HashSet<Relationship>();
        relationships.add(SUCCESS);
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
        final Boolean regexGroupSupport = Boolean.parseBoolean(context.getProperty(REGEX_GROUP_SUPPORT).getValue());
        final Integer occurrence = Integer.parseInt(context.getProperty(OCCURRENCE).getValue());

        final AtomicReference<String> partValue = new AtomicReference<String>();

        final FilePartByRegEx fp = new FilePartByRegEx();
        fp.setOccurrence(occurrence);
        fp.setRegexGroupSupport(regexGroupSupport);
        fp.setRegex(regex);

        if (flowfile == null) {
            return;
        }

        session.read(flowfile, new InputStreamCallback() {
            @Override
            public void process(InputStream in) throws IOException {
                try {
                    fp.setInputStream(in);

                    partValue.set(fp.getValue());

                } catch (Exception ex) {
                    ex.printStackTrace();
                }
            }
        });

        flowfile = session.putAttribute(flowfile, "file.part.value", partValue.get());

        session.transfer(flowfile, SUCCESS);

    }
}
