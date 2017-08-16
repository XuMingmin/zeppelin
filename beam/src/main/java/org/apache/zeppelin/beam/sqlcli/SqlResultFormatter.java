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
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import org.apache.beam.sdk.coders.BeamRecordCoder;
import org.apache.beam.sdk.extensions.sql.schema.BeamRecordSqlType;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.BeamRecord;
import org.apache.beam.sdk.values.PCollection;

/**
 * JavaDoc.
 */
public class SqlResultFormatter extends PTransform<PCollection<BeamRecord>, PCollection<Void>> {

  @Override
  public PCollection<Void> expand(PCollection<BeamRecord> input) {
    final BeamRecordSqlType rowType = (BeamRecordSqlType) ((BeamRecordCoder) input.getCoder())
        .getRecordType();
    printLineHeader(rowType);

    return input.apply("formatter", ParDo.of(new DoFn<BeamRecord, Void>() {
      public final DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

      @ProcessElement
      public void onEachElement(ProcessContext context) {
        BeamRecord row = context.element();
        StringBuffer sb = new StringBuffer();
        for (int idx = 0; idx < rowType.getFieldCount(); ++idx) {
          switch (rowType.fieldTypes.get(idx)) {
            case Types.INTEGER:
              sb.append(String.format("|%-10s", row.getInteger(idx)));
              break;
            case Types.BIGINT:
              sb.append(String.format("|%-20s", row.getLong(idx)));
              break;
            case Types.VARCHAR:
              sb.append(String.format("|%-40s", row.getString(idx)));
              break;
            case Types.TIMESTAMP:
              sb.append(String.format("|%-20s", dateFormat.format(row.getDate(idx))));
              break;
            default:
              sb.append(String.format("|%-20s", row.getFieldValue(idx)));
              break;
          }
        }
        System.out.println(sb.append("|").toString());
      }
    }));
  }

  public void printLineHeader(BeamRecordSqlType rowType) {
    StringBuffer sb = new StringBuffer();
    for (int idx = 0; idx < rowType.getFieldCount(); ++idx) {
      switch (rowType.fieldTypes.get(idx)) {
        case Types.INTEGER:
          sb.append(String.format("|%-10s", rowType.getFieldNameByIndex(idx)));
          break;
        case Types.BIGINT:
          sb.append(String.format("|%-20s", rowType.getFieldNameByIndex(idx)));
          break;
        case Types.VARCHAR:
          sb.append(String.format("|%-40s", rowType.getFieldNameByIndex(idx)));
          break;
        case Types.TIMESTAMP:
          sb.append(String.format("|%-20s", rowType.getFieldNameByIndex(idx)));
          break;
        default:
          sb.append(String.format("|%-20s", rowType.getFieldNameByIndex(idx)));
          break;
      }
    }
    System.out.println(sb.append("|").toString());
  }

}
