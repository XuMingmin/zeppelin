/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.zeppelin.beam.sqlcli;

import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.extensions.sql.schema.BeamRecordSqlType;
import org.apache.beam.sdk.extensions.sql.schema.kafka.BeamKafkaTable;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.BeamRecord;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import io.ebay.rheos.kafka.client.StreamConnectorConfig;
import io.ebay.rheos.schema.avro.RheosEventDeserializer;
import io.ebay.rheos.schema.avro.SchemaRegistryAwareAvroDeserializerHelper;
import io.ebay.rheos.schema.event.RheosEvent;

/**
 * JavaDoc.
 */
public class RheosSojournerTable extends BeamKafkaTable {
  protected RheosSojournerTable(BeamRecordSqlType beamSqlRowType, String bootstrapServers,
      List<String> topics) {
    super(beamSqlRowType, bootstrapServers, topics);
  }

  public static RheosSojournerTable create(String bootstrapServers, String consumerName,
      String clientId, String topicName) {
    Map<String, Object> consumerPara = new HashMap<String, Object>();
    // latest or earliest
    consumerPara.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
    consumerPara.put(ConsumerConfig.GROUP_ID_CONFIG, consumerName);
    consumerPara.put(ConsumerConfig.CLIENT_ID_CONFIG, clientId);

    return (RheosSojournerTable) new RheosSojournerTable(extractRowType(), bootstrapServers,
        Arrays.asList(topicName)).updateConsumerProperties(consumerPara);
  }

  private static BeamRecordSqlType extractRowType() {
    List<Integer> types = new ArrayList<Integer>();
    List<String> fields = new ArrayList<String>();

    types.add(Types.VARCHAR);
    fields.add("GUID");

    types.add(Types.VARCHAR);
    fields.add("CGUID");

    types.add(Types.TIMESTAMP);
    fields.add("EVENT_TIMESTAMP");

    types.add(Types.INTEGER);
    fields.add("PAGE_ID");

    types.add(Types.VARCHAR);
    fields.add("SITE_ID");

    types.add(Types.VARCHAR);
    fields.add("QUERY_STRING");

    return BeamRecordSqlType.create(fields, types);
  }

  @Override
  public PTransform<PCollection<KV<byte[], byte[]>>,
    PCollection<BeamRecord>> getPTransformForInput() {
    return new PTransform<PCollection<KV<byte[], byte[]>>, PCollection<BeamRecord>>() {
      @Override
      public PCollection<BeamRecord> expand(PCollection<KV<byte[], byte[]>> input) {
        return input.apply(ParDo.of(new DBStreamingRecordParser(beamSqlRowType)))
            .setCoder(beamSqlRowType.getRecordCoder());
      }
    };
  }

  @Override
  public PTransform<PCollection<BeamRecord>, PCollection<KV<byte[], byte[]>>>
  getPTransformForOutput() {
    throw new UnsupportedOperationException();
  }

  /**
   * JavaDoc.
   */
  public static class DBStreamingRecordParser extends DoFn<KV<byte[], byte[]>, BeamRecord> {
    private String metadataEndpoint = "https://rheos-services.qa.ebay.com";
    transient RheosEventDeserializer rheosDeserializer;
    transient SchemaRegistryAwareAvroDeserializerHelper<GenericRecord> deserializerHelper;

    private BeamRecordSqlType beamSqlRowType;

    public DBStreamingRecordParser(BeamRecordSqlType beamSqlRowType) {
      super();
      this.beamSqlRowType = beamSqlRowType;
    }

    @Setup
    public void setup() {
      this.rheosDeserializer = new RheosEventDeserializer();

      Map<String, Object> config = new HashMap<String, Object>();
      config.put(StreamConnectorConfig.RHEOS_SERVICES_URLS, metadataEndpoint);

      this.deserializerHelper = new SchemaRegistryAwareAvroDeserializerHelper<GenericRecord>(config,
          GenericRecord.class);
    }

    @ProcessElement
    public void onEachElement(ProcessContext context) {
      try {
        RheosEvent rhsEvent = rheosDeserializer.deserialize("", context.element().getValue());
        GenericRecord avroRecord = deserializerHelper.deserializeData(rhsEvent.getSchemaId(),
            context.element().getValue());

        context.output(new BeamRecord(beamSqlRowType, String.valueOf(avroRecord.get("guid")),
            String.valueOf(avroRecord.get("cguid")),
            new Date(Long.valueOf(((GenericRecord) avroRecord.get("rheosHeader"))
                .get("eventCreateTimestamp").toString())),
            avroRecord.get("pageId"), String.valueOf(avroRecord.get("siteId")),
            String.valueOf(avroRecord.get("urlQueryString"))));
      } catch (Throwable e) {
        e.printStackTrace();
      }
    }
  }

}
