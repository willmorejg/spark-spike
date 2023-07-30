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

import static org.apache.spark.sql.functions.callUDF;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.current_date;
import static org.apache.spark.sql.functions.explode;

import java.io.File;
import java.util.Properties;
import lombok.extern.slf4j.Slf4j;
import net.ljcomputing.sparkspike.function.CalculateAge;
import net.ljcomputing.sparkspike.function.ToLower;
import net.ljcomputing.sparkspike.service.DataProcessingService;
import net.ljcomputing.sparkspike.struct.NameStruct;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
@Slf4j
public class DataProcessingServiceImpl implements DataProcessingService {
    @Autowired private SparkSession sparkSession;

    @Override
    public Dataset<Row> showDataset(File jsonFile) {
        log.debug("jsonFile: {}", jsonFile.toPath().toString());

        sparkSession.udf().register("toLower", new ToLower(), DataTypes.StringType);
        sparkSession
                .udf()
                .register(CalculateAge.UDF_NAME, new CalculateAge(), DataTypes.IntegerType);

        Dataset<Row> df =
                sparkSession
                        .read()
                        .option("multiline", true)
                        .schema(NameStruct.INSTANCE.getStructType())
                        .json(jsonFile.toPath().toString())
                        .select(col("name.*"))
                        .withColumn("givenName", callUDF("toLower", col("givenName")))
                        .withColumn(
                                "age",
                                callUDF(CalculateAge.UDF_NAME, col("birthdate"), current_date()))
                        .withColumnRenamed("givenName", "given_name")
                        .withColumnRenamed("middleName", "middle_name")
                        // .drop("middleName", "suffix")
                        .orderBy(new Column("surname"), new Column("given_name"));

        log.debug("df: {}", df);
        df.show();
        df.printSchema();
        return df;
    }

    @Override
    public Dataset<Row> policyToDf(File jsonFile) {
        Dataset<Row> df = null;
        log.debug("jsonFile: {}", jsonFile.toPath().toString());

        sparkSession
                .udf()
                .register(CalculateAge.UDF_NAME, new CalculateAge(), DataTypes.IntegerType);

        Dataset<Row> policyDf =
                sparkSession.read().option("multiline", true).json(jsonFile.toPath().toString());

        Dataset<Row> coverageDf =
                policyDf.withColumn("coverage", explode(col("coverage"))).select(col("coverage.*"));

        Dataset<Row> riskDf =
                policyDf.select("risks.*")
                        .withColumn("risk", explode(col("risk")))
                        .select(col("risk.*"));

        df = policyDf;

        log.debug("df: {}", df);
        df.show();
        df.printSchema();
        return df;
    }

    @Override
    public void persist(Dataset<Row> df) {
        Properties connectionProperties = new Properties();
        connectionProperties.put("user", "insured");
        connectionProperties.put("password", "insured");
        connectionProperties.put("truncate", "false");

        df.write()
                .mode(SaveMode.Append)
                .jdbc(
                        "jdbc:postgresql://localhost:5432/insurance",
                        "spinsured",
                        connectionProperties);
    }

    @Override
    public Dataset<Row> retrieve(String query) {
        return sparkSession
                .read()
                .format("jdbc")
                .option("url", "jdbc:postgresql://localhost:5432/insurance")
                .option("user", "insured")
                .option("password", "insured")
                .option("dbtable", query)
                .load();
    }
}
