/*
 * Copyright Copyright 2018 Google LLC. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package GoogleService.BigTable;

import com.google.cloud.bigtable.beam.CloudBigtableIO;
import com.google.cloud.bigtable.beam.CloudBigtableTableConfiguration;
import com.google.common.base.Preconditions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * 此函数用于从GCS上读取CSV文件写入bigtable
 *
 */
public class CsvImport {

  private static final byte[] FAMILY = Bytes.toBytes("csv");
  private static final Logger LOG = LoggerFactory.getLogger(CsvImport.class);

  static final DoFn<String, Mutation> MUTATION_TRANSFORM = new DoFn<String, Mutation>() {
    @ProcessElement
    public void processElement(ProcessContext c) throws Exception {
      try {
          String[] headers = c.getPipelineOptions().as(BigtableCsvOptions.class).getHeaders()
              .split(",");
          String[] values = c.element().split(",");
          Preconditions.checkArgument(headers.length == values.length);

          byte[] rowkey = Bytes.toBytes(values[0]);
          byte[][] headerBytes = new byte[headers.length][];
          for (int i = 0; i < headers.length; i++) {
              headerBytes[i] = Bytes.toBytes(headers[i]);
          }

          Put row = new Put(rowkey);
          //long timestamp = System.currentTimeMillis();
          for (int i = 1; i < values.length; i++) {

              if(values[i].trim().length()<1){
                values[i]="NONE";
              }

              row.addColumn(FAMILY, headerBytes[i],  Bytes.toBytes(values[i]));
          }
          c.output(row);
      } catch (Exception e) {
        LOG.error("Failed to process input {}", c.element(), e);
        //throw e;
      }

    }
  };

  public static interface BigtableCsvOptions extends CloudBigtableOptions {

    @Description("The headers for the CSV file.")
    String getHeaders();

    void setHeaders(String headers);

    @Description("The Cloud Storage path to the CSV file.")
    String getInputFile();

    void setInputFile(String location);
  }



  public static void main(String[] args) throws IllegalArgumentException {
    BigtableCsvOptions options =
        PipelineOptionsFactory.fromArgs(args).withValidation().as(BigtableCsvOptions.class);

    if (options.getInputFile().equals("")) {
      throw new IllegalArgumentException("Please provide value for inputFile.");
    }
    if (options.getHeaders().equals("")) {
      throw new IllegalArgumentException("Please provide value for headers.");
    }

    CloudBigtableTableConfiguration config =
        new CloudBigtableTableConfiguration.Builder()
            .withProjectId(options.getBigtableProjectId())
            .withInstanceId(options.getBigtableInstanceId())
            .withTableId(options.getBigtableTableId())
            .build();

    Pipeline p = Pipeline.create(options);

    p.apply("ReadMyFile", TextIO.read().from(options.getInputFile()))
        .apply("TransformParsingsToBigtable", ParDo.of(MUTATION_TRANSFORM))
        .apply("WriteToBigtable", CloudBigtableIO.writeToTable(config));

    p.run().waitUntilFinish();
  }
}
