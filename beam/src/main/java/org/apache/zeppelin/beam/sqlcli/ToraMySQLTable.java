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

import java.io.Serializable;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.BigEndianIntegerCoder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.extensions.sql.schema.BaseBeamTable;
import org.apache.beam.sdk.extensions.sql.schema.BeamIOType;
import org.apache.beam.sdk.extensions.sql.schema.BeamRecordSqlType;
import org.apache.beam.sdk.extensions.sql.schema.BeamSqlRecordHelper;
import org.apache.beam.sdk.io.jdbc.JdbcIO;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.BeamRecord;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;

/**
 * JavaDoc.
 */
public class ToraMySQLTable extends BaseBeamTable implements Serializable {
  public static final String DRIVER_CLASS = "com.mysql.jdbc.Driver";

  private String hostName;
  private int port = 3306;
  private String userName;
  private String password;
  private String database;
  private String tableName;

  private String selectQuery;
  private String insertQuery;

  protected ToraMySQLTable(BeamRecordSqlType beamSqlRowType) {
    super(beamSqlRowType);
  }

  public ToraMySQLTable(BeamRecordSqlType beamSqlRowType, String hostName, int port,
      String userName, String password, String database, String tableName) {
    this(beamSqlRowType);
    this.hostName = hostName;
    this.port = port;
    this.userName = userName;
    this.password = password;
    this.database = database;
    this.tableName = tableName;

    this.selectQuery = buildSelectQuery();
    this.insertQuery = buildInsertQuery();
  }

  private String buildInsertQuery() {
    StringBuffer sb = new StringBuffer();
    sb.append("INSERT INTO ").append(tableName).append("(");
    for (String f : beamSqlRowType.getFieldNames()) {
      sb.append(f.toUpperCase()).append(", ");
    }
    sb.deleteCharAt(sb.length() - 2);

    sb.append(") VALUES(");
    for (int idx = 0; idx < beamSqlRowType.getFieldCount(); ++idx) {
      sb.append("?, ");
    }
    sb.deleteCharAt(sb.length() - 2);
    sb.append(")");

    return sb.toString();
  }

  private String buildSelectQuery() {
    StringBuffer sb = new StringBuffer();
    sb.append("SELECT ");
    for (String f : beamSqlRowType.getFieldNames()) {
      sb.append(", ").append(f.toUpperCase());
    }
    sb.deleteCharAt(7);
    sb.append(" FROM ").append(tableName);

    return sb.toString();
  }

  @Override
  public BeamIOType getSourceType() {
    return BeamIOType.BOUNDED;
  }

  @Override
  public PCollection<BeamRecord> buildIOReader(Pipeline pipeline) {
    return pipeline.apply("ToraCassandraTable", JdbcIO.<BeamRecord>read()
        .withDataSourceConfiguration(JdbcIO.DataSourceConfiguration.create(DRIVER_CLASS,
            String.format("jdbc:mysql://%s:%s/%s?connectTimeout=1000", hostName, port, database))
            .withUsername(userName).withPassword(password))
        .withQuery(selectQuery).withRowMapper(new JdbcIO.RowMapper<BeamRecord>() {
          @Override
          public BeamRecord mapRow(ResultSet resultSet) throws Exception {
            List<Object> values = new ArrayList<>();
            for (int idx = 0; idx < beamSqlRowType.getFieldCount(); ++idx) {
              values.add(resultSet.getObject(idx + 1));
            }
            return new BeamRecord(beamSqlRowType, values);
          }
        }).withCoder(beamSqlRowType.getRecordCoder()));
  }

  @Override
  public PTransform<? super PCollection<BeamRecord>, PDone> buildIOWriter() {
    return JdbcIO.<BeamRecord>write()
        .withDataSourceConfiguration(JdbcIO.DataSourceConfiguration.create(DRIVER_CLASS,
            String.format("jdbc:mysql://%s:%s/%s?connectTimeout=1000", hostName, port, database))
            .withUsername(userName).withPassword(password))
        .withStatement(insertQuery)
        .withPreparedStatementSetter(new JdbcIO.PreparedStatementSetter<BeamRecord>() {
          @Override
          public void setParameters(BeamRecord element, PreparedStatement preparedStatement)
              throws Exception {
            for (int idx = 0; idx < element.getDataType().getFieldCount(); ++idx) {
              preparedStatement.setObject(idx + 1, element.getFieldValue(idx));
            }
          }
        });
  }

}
