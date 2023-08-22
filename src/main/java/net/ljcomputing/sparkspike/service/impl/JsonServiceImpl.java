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
package net.ljcomputing.sparkspike.service.impl;

import net.ljcomputing.sparkspike.service.JsonService;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/** Implementation of a JSON service. */
@Component
public class JsonServiceImpl implements JsonService {
    @Autowired SparkSession sparkSession;

    /**
     * @inheritdoc
     */
    @Override
    public Dataset<Row> extractJsonToDataset(String path) {
        return sparkSession
                .read()
                .format("json")
                .option("inferSchema", "true")
                .option("multiline", "true")
                .load(path);
    }
}
