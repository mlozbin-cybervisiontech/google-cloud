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

package io.cdap.plugin.gcp.spanner.common;

import com.google.cloud.spanner.Spanner;
import com.google.cloud.spanner.SpannerOptions;
import com.google.common.collect.ImmutableSet;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.plugin.gcp.common.GCPUtils;

import java.io.IOException;
import java.util.Set;

/**
 * Spanner utility class to get spanner service
 */
public class SpannerUtil {
  private static final Set<Schema.LogicalType> SUPPORTED_LOGICAL_TYPES =
    ImmutableSet.of(Schema.LogicalType.DATE, Schema.LogicalType.TIMESTAMP_MICROS);

  /**
   * Construct and return the {@link Spanner} service for the provided credentials and projectId
   */
  public static Spanner getSpannerService(String serviceAccountFilePath, String projectId) throws IOException {
    SpannerOptions.Builder optionsBuilder = SpannerOptions.newBuilder();
    if (serviceAccountFilePath != null) {
      optionsBuilder.setCredentials(GCPUtils.loadServiceAccountCredentials(serviceAccountFilePath));
    }
    optionsBuilder.setProjectId(projectId);
    return optionsBuilder.build().getService();
  }

  /**
   * Validate that the schema is a supported one, compatible with Spanner.
   */
  public static void validateSchema(Schema schema, Set<Schema.Type> supportedTypes) {
    for (Schema.Field field : schema.getFields()) {
      Schema fieldSchema = field.getSchema();
      fieldSchema = fieldSchema.isNullable() ? fieldSchema.getNonNullable() : fieldSchema;

      Schema.LogicalType logicalType = fieldSchema.getLogicalType();
      if (logicalType != null && !SUPPORTED_LOGICAL_TYPES.contains(logicalType)) {
        throw new IllegalArgumentException(String.format("Schema logical type %s not supported.",
                                                         logicalType));
      }

      if (logicalType == null) {
        Schema.Type type = fieldSchema.getType();
        if (!supportedTypes.contains(type)) {
          throw new IllegalArgumentException(String.format("Schema type %s not supported.", type));
        }
      }
    }
  }

  /**
   * Converts schema to Spanner create statement
   *
   * @param tableName table name to be created
   * @param primaryKeys comma separated list of primary keys
   * @param schema schema of the table
   * @return Create statement
   */
  public static String convertSchemaToCreateStatement(String tableName, String primaryKeys, Schema schema) {
    StringBuilder createStmt = new StringBuilder();
    createStmt.append("CREATE TABLE ").append(tableName).append(" (");

    for (Schema.Field field : schema.getFields()) {
      String name = field.getName();
      Schema fieldSchema = field.getSchema();
      fieldSchema = fieldSchema.isNullable() ? fieldSchema.getNonNullable() : fieldSchema;
      Schema.LogicalType logicalType = fieldSchema.getLogicalType();
      String spannerType;

      if (logicalType != null) {
        switch (logicalType) {
          case DATE:
            spannerType = "DATE";
            break;
          case TIMESTAMP_MILLIS:
          case TIMESTAMP_MICROS:
            spannerType = "TIMESTAMP";
            break;
          default:
            // this should not happen
            throw new IllegalStateException("Logical type " + logicalType + " is not supported");
        }
        addColumn(createStmt, name, field.getSchema().isNullable(), spannerType);
        continue;
      }

      Schema.Type type = fieldSchema.getType();
      switch (type) {
        case BOOLEAN:
          spannerType = "BOOL";
          break;
        case STRING:
          spannerType = "STRING(MAX)";
          break;
        case INT:
        case LONG:
          spannerType = "INT64";
          break;
        case FLOAT:
        case DOUBLE:
          spannerType = "FLOAT64";
          break;
        case BYTES:
          spannerType = "BYTES(MAX)";
          break;
        case ARRAY:
          Schema componentSchema = fieldSchema.getComponentSchema();
          if (componentSchema == null) {
            throw new IllegalStateException("Component schema of field '" + name + "' is null");
          }
          spannerType = getArrayType(componentSchema);
          break;
        default:
          throw new IllegalStateException(type.name() + " : Type currently not supported.");
      }
      addColumn(createStmt, name, field.getSchema().isNullable(), spannerType);
    }

    // remove trailing ", "
    createStmt.deleteCharAt(createStmt.length() - 1)
      .deleteCharAt(createStmt.length() - 1)
      .append(")");

    createStmt.append(" PRIMARY KEY (").append(primaryKeys).append(")");

    return createStmt.toString();
  }

  private static String getArrayType(Schema componentSchema) {
    Schema unionSchema = componentSchema.getUnionSchema(0);
    if (unionSchema == null) {
      throw new IllegalStateException("Union schema of '" + componentSchema.getRecordName() + "' is null");
    }

    Schema.LogicalType logicalType = unionSchema.getLogicalType();

    if (logicalType != null) {
      switch (logicalType) {
        case DATE:
          return "ARRAY<DATE>";
        case TIMESTAMP_MILLIS:
        case TIMESTAMP_MICROS:
          return  "ARRAY<TIMESTAMP>";
        default:
          // this should not happen
          throw new IllegalStateException("Array of '" + logicalType + "' logical type currently not supported.");
      }
    }

    Schema.Type type = unionSchema.getType();
    switch (type) {
      case BOOLEAN:
        return "ARRAY<BOOL>";
      case STRING:
        return "ARRAY<STRING(MAX)>";
      case INT:
      case LONG:
        return "ARRAY<INT64>";
      case FLOAT:
      case DOUBLE:
        return "ARRAY<FLOAT64>";
      case BYTES:
        return "ARRAY<BYTES(MAX)>";
      default:
        throw new IllegalStateException("Array of '" + type.name() + "' type currently not supported.");
    }
  }

  /**
   * Add column to create statement with appropriate type.
   */
  private static void addColumn(StringBuilder createStmt, String name, boolean isNullable, String spannerType) {
    createStmt.append(name).append(" ").append(spannerType);
    if (!isNullable) {
      createStmt.append(" NOT NULL");
    }
    createStmt.append(", ");
  }
}
