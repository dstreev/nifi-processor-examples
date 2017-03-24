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
package com.streever.iot.nifi.processors.data.generator;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.streever.data.generator.RecordGenerator;
import org.apache.nifi.annotation.behavior.DynamicProperty;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.Validator;
import org.apache.nifi.expression.AttributeExpression;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.*;
import org.apache.nifi.processor.io.OutputStreamCallback;
import org.apache.nifi.processor.util.StandardValidators;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;

@SupportsBatching
@Tags({"test", "record", "generate", "cdc"})
@InputRequirement(Requirement.INPUT_FORBIDDEN)

@CapabilityDescription("This processor creates FlowFiles with random data or custom content. GenerateFlowFile is useful" +
        "for load testing, configuration, and simulation.")
@DynamicProperty(name = "Generated FlowFile attribute name", value = "Generated FlowFile attribute value", supportsExpressionLanguage = true,
        description = "Specifies an attribute on generated FlowFiles defined by the Dynamic Property's key and value." +
                " If Expression Language is used, evaluation will be performed only once per batch of generated FlowFiles.")

public class GenerateRecordProcessor extends AbstractProcessor {

    private final AtomicReference<String> data = new AtomicReference<>();
    private RecordGenerator generator = null;

    // TODO: Need to pick up the configuration
    public static final PropertyDescriptor GENERATOR_RESOURCE = new PropertyDescriptor.Builder()
            .name("Generator Resource Descriptor")
            .description("A file that contains the Generator Schema")
            .required(true)
            .addValidator(createFilesExistValidator())
            .build();

    // TODO: Convert to Record Count
    public static final PropertyDescriptor RECORD_COUNT = new PropertyDescriptor.Builder()
            .name("Record Count")
            .description("Record Count for each invocation")
            .required(true)
            .defaultValue("1")
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .build();

    public static final Relationship SUCCESS = new Relationship.Builder()
            .name("success")
            .build();

    private List<PropertyDescriptor> descriptors;
    private Set<Relationship> relationships;

    @Override
    protected void init(final ProcessorInitializationContext context) {
        final List<PropertyDescriptor> descriptors = new ArrayList<>();
        descriptors.add(GENERATOR_RESOURCE);
        descriptors.add(RECORD_COUNT);
        this.descriptors = Collections.unmodifiableList(descriptors);

        final Set<Relationship> relationships = new HashSet<>();
        relationships.add(SUCCESS);
        this.relationships = Collections.unmodifiableSet(relationships);
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return descriptors;
    }

    @Override
    protected PropertyDescriptor getSupportedDynamicPropertyDescriptor(final String propertyDescriptorName) {
        return new PropertyDescriptor.Builder()
                .name(propertyDescriptorName)
                .required(false)
                .addValidator(StandardValidators.createAttributeExpressionLanguageValidator(AttributeExpression.ResultType.STRING, true))
                .addValidator(StandardValidators.ATTRIBUTE_KEY_PROPERTY_NAME_VALIDATOR)
                .expressionLanguageSupported(true)
                .dynamic(true)
                .build();
    }

    @Override
    public Set<Relationship> getRelationships() {
        return relationships;
    }

    @OnScheduled
    public void onScheduled(final ProcessContext context) {

    }

    @Override
    protected Collection<ValidationResult> customValidate(ValidationContext validationContext) {
        final List<ValidationResult> results = new ArrayList<>(1);

        ObjectMapper mapper = new ObjectMapper();

        JsonNode rootNode = null;
        try {
            rootNode = mapper.readValue(new File(validationContext.getProperty(GENERATOR_RESOURCE).getValue()), JsonNode.class);

            generator = new RecordGenerator(rootNode);

        } catch (IOException ioe) {
            results.add(new ValidationResult.Builder().subject("Generator Resource").valid(false).explanation("Couldn't read specified resource").build());
        }

        return results;
    }

    private String generateData(final ProcessContext context) {
        final int recordCount = context.getProperty(RECORD_COUNT).asInteger();
        StringBuilder sb = new StringBuilder();

        for (int i = 0; i < recordCount; i++) {
            sb.append(generator.next()).append("\n");
        }

        return sb.toString();
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) {
        final String data;

        data = generateData(context);

        Map<PropertyDescriptor, String> processorProperties = context.getProperties();
        Map<String, String> generatedAttributes = new HashMap<String, String>();
        for (final Map.Entry<PropertyDescriptor, String> entry : processorProperties.entrySet()) {
            PropertyDescriptor property = entry.getKey();
            if (property.isDynamic() && property.isExpressionLanguageSupported()) {
                String dynamicValue = context.getProperty(property).evaluateAttributeExpressions().getValue();
                generatedAttributes.put(property.getName(), dynamicValue);
            }
        }

        FlowFile flowFile = session.create();
        if (data.length() > 0) {
            flowFile = session.write(flowFile, new OutputStreamCallback() {
                @Override
                public void process(final OutputStream out) throws IOException {
                    out.write(data.getBytes());
                }
            });
        }
        flowFile = session.putAllAttributes(flowFile, generatedAttributes);

        session.getProvenanceReporter().create(flowFile);
        session.transfer(flowFile, SUCCESS);
    }

    /*
     * Validates that one or more files exist, as specified in a single property.
    */
    public static final Validator createFilesExistValidator() {
        return new Validator() {

            @Override
            public ValidationResult validate(String subject, String input, ValidationContext context) {
                final String[] files = input.split(",");
                for (String filename : files) {
                    try {
                        final File file = new File(filename.trim());
                        final boolean valid = file.exists() && file.isFile();
                        if (!valid) {
                            final String message = "File " + file + " does not exist or is not a file";
                            return new ValidationResult.Builder().subject(subject).input(input).valid(false).explanation(message).build();
                        }
                    } catch (SecurityException e) {
                        final String message = "Unable to access " + filename + " due to " + e.getMessage();
                        return new ValidationResult.Builder().subject(subject).input(input).valid(false).explanation(message).build();
                    }
                }
                return new ValidationResult.Builder().subject(subject).input(input).valid(true).build();
            }

        };
    }

}

