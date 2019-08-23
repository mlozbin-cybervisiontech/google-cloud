/*
 * Copyright © 2018 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package io.cdap.plugin.gcp.spanner.sink;

import com.google.cloud.ByteArray;
import com.google.cloud.Date;
import com.google.cloud.Timestamp;
import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.DatabaseId;
import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.Spanner;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.format.UnexpectedFormatException;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.plugin.gcp.spanner.SpannerConstants;
import io.cdap.plugin.gcp.spanner.common.SpannerUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;
import java.lang.reflect.Array;
import java.time.LocalDate;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Spanner output format
 */
public class SpannerOutputFormat extends OutputFormat<NullWritable, StructuredRecord> {

  /**
   * Get properties from SparkSinkConfig and store them as properties in Configuration
   *
   * @param configuration the Hadoop configuration to set the properties in
   * @param config        the spanner configuration
   */
  public static void configure(Configuration configuration, SpannerSinkConfig config) {
    String projectId = config.getProject();
    configuration.set(SpannerConstants.PROJECT_ID, projectId);
    String serviceAccountFilePath = config.getServiceAccountFilePath();
    if (serviceAccountFilePath != null) {
      configuration.set(SpannerConstants.SERVICE_ACCOUNT_FILE_PATH, serviceAccountFilePath);
    }
    configuration.set(SpannerConstants.INSTANCE_ID, config.getInstance());
    configuration.set(SpannerConstants.DATABASE, config.getDatabase());
    configuration.set(SpannerConstants.TABLE_NAME, config.getTable());
    configuration.set(SpannerConstants.SPANNER_WRITE_BATCH_SIZE, String.valueOf(config.getBatchSize()));
    configuration.set(SpannerConstants.SCHEMA, config.getSchema().toString());
  }

  @Override
  public RecordWriter<NullWritable, StructuredRecord> getRecordWriter(TaskAttemptContext context)
    throws IOException {
    Configuration configuration = context.getConfiguration();
    String projectId = configuration.get(SpannerConstants.PROJECT_ID);
    String instanceId = configuration.get(SpannerConstants.INSTANCE_ID);
    String database = configuration.get(SpannerConstants.DATABASE);
    String serviceFilePath = configuration.get(SpannerConstants.SERVICE_ACCOUNT_FILE_PATH);
    String tableName = configuration.get(SpannerConstants.TABLE_NAME);
    Schema schema = Schema.parseJson(configuration.get(SpannerConstants.SCHEMA));
    Spanner spanner = SpannerUtil.getSpannerService(serviceFilePath, projectId);
    int batchSize = Integer.parseInt(configuration.get(SpannerConstants.SPANNER_WRITE_BATCH_SIZE));
    DatabaseId db = DatabaseId.of(projectId, instanceId, database);
    DatabaseClient client = spanner.getDatabaseClient(db);
    return new SpannerRecordWriter(spanner, tableName, client, batchSize, schema);
  }

  /**
   * Spanner record writer that buffers mutations and writes to spanner
   */
  protected static class SpannerRecordWriter extends RecordWriter<NullWritable, StructuredRecord> {
    private final Spanner spanner;
    private final String tableName;
    private final DatabaseClient databaseClient;
    private final List<Mutation> mutations;
    private final int batchSize;
    private final Schema schema;

    public SpannerRecordWriter(Spanner spanner, String tableName, DatabaseClient client, int batchSize,
                               Schema schema) {
      this.spanner = spanner;
      this.tableName = tableName;
      this.databaseClient = client;
      this.mutations = new ArrayList<>();
      this.batchSize = batchSize;
      this.schema = schema;
    }

    @Override
    public void write(NullWritable nullWritable, StructuredRecord record) {
      Mutation.WriteBuilder builder = Mutation.newInsertOrUpdateBuilder(tableName);
      List<Schema.Field> fields = schema.getFields();
      for (Schema.Field field : fields) {
        String name = field.getName();
        Schema fieldSchema = field.getSchema();
        fieldSchema = fieldSchema.isNullable() ? fieldSchema.getNonNullable() : fieldSchema;
        Schema.LogicalType logicalType = fieldSchema.getLogicalType();

        if (logicalType != null) {
          if (record.get(name) != null) {
            switch (logicalType) {
              case DATE:
                LocalDate date = record.getDate(name);
                Date spannerDate = Date.fromYearMonthDay(date.getYear(), date.getMonthValue(), date.getDayOfMonth());
                builder.set(name).to(spannerDate);
                break;
              case TIMESTAMP_MILLIS:
              case TIMESTAMP_MICROS:
                ZonedDateTime ts = record.getTimestamp(name);
                Timestamp spannerTs = Timestamp.ofTimeSecondsAndNanos(ts.toEpochSecond(), ts.getNano());
                builder.set(name).to(spannerTs);
                break;
              default:
                throw new IllegalStateException("Logical type '" + logicalType + "' is not supported.");
            }
          }
          continue;
        }

        Schema.Type type = fieldSchema.getType();
        switch (type) {
          case BOOLEAN:
            builder.set(name).to(record.<Boolean>get(name));
            break;
          case STRING:
            builder.set(name).to(record.<String>get(name));
            break;
          case INT:
            Integer intValue = record.<Integer>get(name);
            if (intValue == null) {
              builder.set(name).to((Long) null);
            } else {
              builder.set(name).to(intValue.longValue());
            }
            break;
          case LONG:
            builder.set(name).to(record.<Long>get(name));
            break;
          case FLOAT:
            Float floatValue = record.<Float>get(name);
            if (floatValue == null) {
              builder.set(name).to((Double) null);
            } else {
              builder.set(name).to(floatValue.doubleValue());
            }
            break;
          case DOUBLE:
            builder.set(name).to(record.<Double>get(name));
            break;
          case BYTES:
            byte[] byteArray = record.get(name);
            if (byteArray == null) {
              builder.set(name).to((ByteArray) null);
            } else {
              builder.set(name).to(ByteArray.copyFrom(byteArray));
            }
            break;
          case ARRAY:
            Schema componentSchema = fieldSchema.getComponentSchema();
            if (componentSchema == null) {
              throw new IllegalStateException("Component schema of field '" + name + "' is null");
            }
            writeArray(name, fieldSchema.getComponentSchema(), record.get(name), builder);
            break;
          default:
            throw new IllegalStateException(type.name() + " : Type currently not supported.");
        }
      }
      mutations.add(builder.build());
      if (mutations.size() > batchSize) {
        databaseClient.write(mutations);
        mutations.clear();
      }
    }

    @Override
    public void close(TaskAttemptContext taskAttemptContext) {
      if (mutations.size() > 0) {
        databaseClient.write(mutations);
        mutations.clear();
      }
      spanner.close();
    }
  }

  @Override
  public void checkOutputSpecs(JobContext jobContext) {
  }

  /**
   * No op output committer
   */
  @Override
  public OutputCommitter getOutputCommitter(TaskAttemptContext taskAttemptContext) {
    return new OutputCommitter() {
      @Override
      public void setupJob(JobContext jobContext) {

      }

      @Override
      public void setupTask(TaskAttemptContext taskAttemptContext) {

      }

      @Override
      public boolean needsTaskCommit(TaskAttemptContext taskAttemptContext) {
        return false;
      }

      @Override
      public void commitTask(TaskAttemptContext taskAttemptContext) {

      }

      @Override
      public void abortTask(TaskAttemptContext taskAttemptContext) {

      }
    };
  }

  private static void writeArray(String name, Schema componentSchema, Object values, Mutation.WriteBuilder builder) {
    Schema unionSchema = componentSchema.getUnionSchema(0);
    if (unionSchema == null) {
      throw new IllegalStateException("Union schema of '" + componentSchema.getRecordName() + "' is null");
    }

    Schema.LogicalType logicalType = unionSchema.getLogicalType();
    if (logicalType != null) {
      switch (logicalType) {
        case DATE:
          Collection<LocalDate> dateList = toCollection(name, logicalType.toString(), values);
          builder.set(name)
            .toDateArray(
              dateList.stream()
                .map(date -> Date.fromYearMonthDay(date.getYear(), date.getMonthValue(), date.getDayOfMonth()))
                .collect(Collectors.toList())
            );
          break;
        case TIMESTAMP_MILLIS:
        case TIMESTAMP_MICROS:
          Collection<ZonedDateTime> timestampList = toCollection(name, logicalType.toString(), values);
          builder.set(name)
            .toTimestampArray(
              timestampList.stream()
                .map(ts -> Timestamp.ofTimeSecondsAndNanos(ts.toEpochSecond(), ts.getNano()))
                .collect(Collectors.toList())
            );
          break;
        default:
          throw new IllegalStateException("Array of '" + logicalType + "' logical type currently not supported.");
      }
    } else {
      Schema.Type type = unionSchema.getType();
      switch (type) {
        case BOOLEAN:
          Collection<Boolean> listBoolean = toCollection(name, type.toString(), values);
          builder.set(name).toBoolArray(listBoolean);
          break;
        case STRING:
          Collection<String> listString = toCollection(name, type.toString(), values);
          builder.set(name).toStringArray(listString);
          break;
        case LONG:
          Collection<Long> listValue = toCollection(name, type.toString(), values);
          builder.set(name).toInt64Array(listValue);
          break;
        case DOUBLE:
          Collection<Double> listDouble = toCollection(name, type.toString(), values);
          builder.set(name).toFloat64Array(listDouble);
          break;
        case INT:
          Collection<Integer> integerList = toCollection(name, type.toString(), values);
          builder.set(name)
            .toInt64Array(
              integerList.stream()
                .map(Integer::longValue)
                .collect(Collectors.toList())
            );
          break;
        case FLOAT:
          Collection<Float> floatList = toCollection(name, type.toString(), values);
          builder.set(name)
            .toFloat64Array(
              floatList.stream()
                .map(Float::doubleValue)
                .collect(Collectors.toList())
            );
          break;
        case BYTES:
          Collection<byte[]> bytesList = toCollection(name, type.toString(), values);
          builder.set(name)
            .toBytesArray(
              bytesList.stream()
                .map(ByteArray::copyFrom)
                .collect(Collectors.toList())
            );
          break;
        default:
          throw new IllegalStateException("Array of '" + type.name() + "' type currently not supported.");
      }
    }
  }

  private static <T> Collection<T> toCollection(String fieldName, String fieldType, Object value) {
    Function<String, Collection> valueExtractor = name -> {
      throw new UnexpectedFormatException(
        String.format("Field '%s' of type '%s' has unexpected value '%s'", name, fieldType, value));
    };

    if (value instanceof Collection) {
      valueExtractor = name -> (Collection<?>) value;
    } else if (value.getClass().isArray()) {
      valueExtractor = name -> convertToObjectCollection(value);
    }
    return getValue(valueExtractor, fieldName, fieldType, Collection.class);
  }

  private static Collection<Object> convertToObjectCollection(Object array) {
    Class ofArray = array.getClass().getComponentType();
    if (ofArray.isPrimitive()) {
      List<Object> list = new ArrayList<>();
      int length = Array.getLength(array);
      for (int i = 0; i < length; i++) {
        list.add(Array.get(array, i));
      }
      return list;
    } else {
      return Arrays.asList((Object[]) array);
    }
  }

  private static <T> T getValue(Function<String, T> valueExtractor, String fieldName, String fieldType,
                                Class<T> clazz) {
    T value = valueExtractor.apply(fieldName);
    if (clazz.isAssignableFrom(value.getClass())) {
      return clazz.cast(value);
    }
    throw new UnexpectedFormatException(
      String.format("Field '%s' is not of expected type '%s'", fieldName, fieldType));
  }
}
