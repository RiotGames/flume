/**
 * Licensed to Apache Software Foundation, Inc. under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ASF licenses this file
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
package com.cloudera.flume.hbase;

import com.cloudera.flume.conf.Context;
import com.cloudera.flume.conf.SinkFactory.SinkBuilder;
import com.cloudera.flume.core.Event;
import com.cloudera.flume.core.EventSink;
import com.cloudera.util.Pair;
import com.google.common.base.Preconditions;
import net.opentsdb.core.TSDB;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.hbase.async.HBaseClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.Map.Entry;

/**
 * README.txt for basic steps.
 */
public class OpenTsdbSink extends EventSink.Base {
    private static final Logger LOG = LoggerFactory.getLogger(OpenTsdbSink.class);
    private static final String TSDB_PROPERTIES_FILE = "tsdb.properties";
    private static final String DEFAULT_ZKQ = "localhost";
    private static final String DEFAULT_TABLE_NAME = "tsdb";
    private static final String DEFAULT_TABLE_UID_NAME = "tsdb-uid";
    public static final String USAGE = "usage: openTSDB(\"metricKey\", \"valueKey\", \"metricPrefix\",\"timestamp\",\"tag1\"[,\"tag2\"])";

    private String metricKey;
    private String valueKey;
    private String metricPrefix;
    private String timeStampKey;
    private List<String> tagsList;
    private TSDB tsdbClient;
    private Boolean isMetricKeyValuePresent;
    private Boolean isMetricPrefixPresent;
    private String zkq;
    private String table;
    private String uidtable;

    /**
     * Instantiates sink. See detailed explanation of parameters and their values
     * /*
     *
     * @param metricKey       The metric key which has metric-name that datapoint belongs to
     * @param valueKey        The value key which has value of the datapoint
     * @param metricPrefix    The prefix, whose matching keys and values correspond to the datapoints
     * @param timeStampKey    The timestamp of metric
     * @param tagsList        The list of tags associated with the metric
     */
    public OpenTsdbSink(String metricKey, String valueKey, String metricPrefix, String timeStampKey,
                        List<String> tagsList) {
        if (isKeyEmpty(metricKey) || isKeyEmpty(valueKey)) {
            Preconditions.checkNotNull(metricPrefix,
                    "Either metric key-value pair  or metricPrefix MUST be provided.");
            this.isMetricKeyValuePresent = false;
        } else {
            this.isMetricKeyValuePresent = true;
        }

        if (isKeyEmpty(metricPrefix)) {
            this.isMetricPrefixPresent = false;
        } else {
            this.isMetricPrefixPresent = true;
        }

        Preconditions.checkNotNull(timeStampKey,
                "timestamp MUST be provided.");
        Preconditions.checkNotNull(tagsList,
                "At least one Tag MUST be provided.");


        this.metricKey = metricKey;
        this.valueKey = valueKey;
        this.metricPrefix = metricPrefix;
        this.timeStampKey = timeStampKey;
        this.tagsList = tagsList;
        this.zkq = DEFAULT_ZKQ;
        this.table = DEFAULT_TABLE_NAME;
        this.uidtable = DEFAULT_TABLE_UID_NAME;

        PropertiesConfiguration tsdbConfig = new PropertiesConfiguration();
        try {
            tsdbConfig.load(TSDB_PROPERTIES_FILE);
            zkq = tsdbConfig.getString("zookeeper.quorum");
            table = tsdbConfig.getString("tsdb.table");
            uidtable = tsdbConfig.getString("tsdb.uidtable");
        } catch (ConfigurationException e) {
            LOG.error(" Cannot load the properties file: " + TSDB_PROPERTIES_FILE);
            LOG.info("Using default configuration for TSDB");
        }

    }

    Boolean isKeyEmpty(String key)
    {
       if (key == null || key.isEmpty() || key.equalsIgnoreCase("") ||
               key.equalsIgnoreCase(" "))
       {
           return true;
       }
        return false;
    }

    @Override
    public void append(Event e) throws IOException {
        long timeStamp = Long.valueOf(e.escapeString(timeStampKey));
        /*
        * Generate the map of tagNames, that is associated with *all* data-points in the event
        * TODO: More granularity in associating tag names.
        */
        Map<String, String> tagMap = new HashMap<String, String>();
        for (String k : tagsList) {
            NavigableMap<String, String> tagKV = new TreeMap<String, String>(e.getEscapeMapping(k));
            tagMap.put(tagKV.firstKey(), tagKV.get(tagKV.firstKey()));
        }

        if (isMetricKeyValuePresent) {
            /*
            * Add the data-point matching metricKey and corresponding valueKey
            */
            try {
                LOG.info("Putting" + timeStamp + " : " + e.escapeString(metricKey), " : " + e.escapeString(valueKey));
                tsdbClient.addPoint(e.escapeString(metricKey), timeStamp, Long.valueOf(e.escapeString(valueKey)), tagMap);
            } catch (NumberFormatException ne) {
                LOG.info(timeStamp + " : " + e.escapeString(metricKey) + " : " + e.escapeString(valueKey) + " resulted exception");
            }
        }
        if (isMetricPrefixPresent) {
            /*
            *  Add *all* data-points matching the "metricPrefix"
            */
            for (Entry<String, byte[]> a : e.getAttrs().entrySet()) {
                attemptToAddDataPoint(e, a, timeStamp, tagMap);
            }
        }
    }

    // Made as package-private for unit-testing
    // Entry here represents event's attribute: key is attribute name and value is
    // attribute value
    void attemptToAddDataPoint(Event e, Entry<String, byte[]> a, long timeStamp, Map tagMap) {
        String attrKey = a.getKey();
        if (attrKey.startsWith(metricPrefix)
                && attrKey.length() > metricPrefix.length()) {
            String metricKeyWithoutPrefix = attrKey.substring(metricPrefix.length());
            String valueKey = metricKeyWithoutPrefix + "_value";
            LOG.info("Putting" + timeStamp + " : " + e.escapeString(a.getKey()), " : " +  e.escapeString(valueKey));
            tsdbClient.addPoint(e.escapeString(a.getKey()), timeStamp, Long.valueOf(e.escapeString(valueKey)), tagMap);
        }
    }

    @Override
    public void close() throws IOException {
        tsdbClient.flush();
        tsdbClient.shutdown();
    }

    @Override
    public void open() throws IOException {

        // Connect using the zk
        final HBaseClient client = new HBaseClient(zkq);

        // Make sure we don't even start if we can't find out tables.
        try {
            client.ensureTableExists(table).joinUninterruptibly();
        } catch (Exception e) {
            LOG.error("Table : " + table + "does not exist");
        }

        try {
            client.ensureTableExists(uidtable).joinUninterruptibly();
        } catch (Exception e) {
            LOG.error("Table : " + uidtable + "does not exist");
        }

        /*
         * TODO: Move this to config/properties file
         */
        client.setFlushInterval((short) 1000);
        tsdbClient = new TSDB(client, table, uidtable);
    }

    public static SinkBuilder builder() {
        return new SinkBuilder() {

            @Override
            public EventSink build(Context context, String... argv) {
                Preconditions.checkArgument(argv.length >= 4, USAGE);
                String metricKey = argv[0];
                String valueKey = argv[1];
                String metricPrefix = argv[2];
                String timeStampKey = argv[3];

                /*
                 * The remaining parameters are tagNames
                 */
                List<String> tagsList = new ArrayList<String>();
                for (int i = 4; i < argv.length; i++) {
                    tagsList.add(argv[i]);
                }
                return new OpenTsdbSink(metricKey, valueKey, metricPrefix, timeStampKey, tagsList);
            }
        };
    }

    public static List<Pair<String, SinkBuilder>> getSinkBuilders() {
        return Arrays.asList(new Pair<String, SinkBuilder>(
                "openTSDB", builder()));
    }
}
