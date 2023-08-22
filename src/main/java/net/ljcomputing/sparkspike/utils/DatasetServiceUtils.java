/*
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.

James G Willmore - LJ Computing - (C) 2023
*/
package net.ljcomputing.sparkspike.utils;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.explode;
import static org.apache.spark.sql.functions.expr;
import static org.apache.spark.sql.functions.from_unixtime;
import static org.apache.spark.sql.functions.to_date;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

/** Implementation of singleton service to manipulate a dataset. */
public enum DatasetServiceUtils {
    INSTANCE;

    /**
     * Convert the given columns in the given Dataset to a Date type. Assumes the columns are the
     * Long data type.
     *
     * @param dataset
     * @param columns
     */
    public static final Dataset<Row> timestampToDate(
            Dataset<Row> dataset, final String... columns) {
        for (final String column : columns) {
            // divide by 1000 to get seconds
            final String expression = String.format("%s / 1000", column);

            // reuse the dataset passed into the method
            dataset =
                    dataset.withColumn(column, expr(expression))
                            .withColumn(column, to_date(from_unixtime(col(column))));
        }

        // return the modified dataset - not doing so results in the original dataset left unchanged
        return dataset;
    }

    public static final Dataset<Row> explodeToNewDataset(
            Dataset<Row> dataset, final String columnToExplode, final String... additionalColumns) {
        final List<Column> addtlColumns = stringArrayToColumnList(additionalColumns);
        addtlColumns.add(explode(col(columnToExplode)));
        return dataset.select(columnListToColumnArray(addtlColumns))
                .select("*", "col.*")
                .drop("col");
    }

    public static final List<Column> stringArrayToColumnList(final String... columnNames) {
        return columnNames == null
                ? new ArrayList<>()
                : Arrays.asList(columnNames).stream()
                        .map(c -> new Column(c))
                        .collect(Collectors.toList());
    }

    public static final Column[] columnListToColumnArray(final List<Column> columns) {
        return columns.toArray(new Column[columns.size()]);
    }

    public static final Column[] stringArrayToColumnArray(final String... columnNames) {
        return columnListToColumnArray(stringArrayToColumnList(columnNames));
    }
}
