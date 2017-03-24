/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.streever.data.generator.fields;

import com.fasterxml.jackson.databind.JsonNode;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Iterator;
import java.util.Random;

public class StartStopFields {


    public class StartField extends AbstractFieldType implements FieldType<String> {
        public StartField() {
            super(null);
        }

        @Override
        public String getValue() {
            String rtn = df.format(start);
            init(1);
            return rtn;
        }

        @Override
        public String getPoolValue() {
            String rtn = df.format(start);
            init(1);
            return rtn;
        }
    }

    public class StopField extends AbstractFieldType implements FieldType<String> {
        public StopField() {
            super(null);
        }

        @Override
        public String getValue() {
            String rtn = df.format(stop);
            init(2);
            return rtn;
        }

        @Override
        public String getPoolValue() {
            String rtn = df.format(stop);
            init(2);
            return rtn;
        }
    }

    private FieldType[] fields = new FieldType[2]; // start.stop

    private Long min = 0l;
    private Long max = 100000l;
    private Long spreadMin = 0l;
    private Long spreadMax = 100000l;

    private Date minDt = new Date(0); // epoch
    private Date maxDt = new Date();

    private String format = "yyyy-MM-dd HH:mm:ss";
    private Long diff;
    private Long spreadDiff;

    private DateFormat df = null;

    private Date start;
    private Date stop;

    private int who = 0;
    private Random random = new Random();

    public StartStopFields(JsonNode node) {
        fields[0] = new StartField();
        fields[1] = new StopField();

        // Field Order
        if (node.has("order")) {
            // Will be a set of int.
            JsonNode setNode = node.get("order");

            int i=0;
            for (Iterator<JsonNode> it=setNode.elements();it.hasNext();) {
                JsonNode element = it.next();
                fields[i++].setOrder(element.asInt());
            }
        }
        // Date Range
        if (node.has("format")) {
            format = node.get("format").asText();
            System.out.println("Date Format Set: " + format);
        }
        if (df == null) {
            df = new SimpleDateFormat(format);
        }
        try {
            if (node.has("range")) {
                JsonNode rangeNode = node.get("range");
                if (rangeNode.has("min")) {
                    String minDtStr = rangeNode.get("min").asText();
                    minDt = df.parse(minDtStr);
                    System.out.println("Min Date: " + minDt.toString());
                    min = minDt.getTime();
                }
                if (rangeNode.has("max")) {
                    String maxDtStr = rangeNode.get("max").asText();
                    maxDt = df.parse(maxDtStr);
                    System.out.println("Max Date: " + maxDt.toString());
                    max = maxDt.getTime();
                }
            }
        } catch (ParseException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        }

        // Spread min and max
        if (node.has("spread")) {
            JsonNode spread = node.get("spread");
            spreadMin = spread.get("min").asLong();
            spreadMax = spread.get("max").asLong();
        }
        reset();
    }

    public FieldType getStartField() {
        return fields[0];
    }

    public FieldType getStopField() {
        return fields[1];
    }

    private void init(int who) {
        this.who = this.who | who;
        // Trigger reset when BOTH Start and Stop have been retrieved.
        if (this.who == 3) {
            reset();
        }
    }

    private void reset() {
        who = 0;
        double multiplierD = random.nextDouble();
        long dateValue = (Long) min + Math.round((Long) getDiff() * multiplierD);
        start = new Date(dateValue);

        multiplierD = random.nextDouble();
        long spread = (Long) spreadMin + Math.round((Long) getSpreadDiff() * multiplierD);

        stop = new Date(dateValue+spread);

    }

    private Number getDiff() {
        if (diff == null) {
            diff = max - min;
        }
        return diff;
    }

    private Number getSpreadDiff() {
        if (spreadDiff == null) {
            spreadDiff = spreadMax - spreadMin;
        }
        return spreadDiff;
    }


}
