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
package net.ljcomputing.sparkspike;

import static org.apache.spark.sql.functions.expr;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import net.ljcomputing.sparkspike.configuration.ApplicationContextProvider;
import net.ljcomputing.sparkspike.service.DataProcessingService;
import net.ljcomputing.sparkspike.service.JsonXmlService;
import net.ljcomputing.sparkspike.service.ProcessingService;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.junit.jupiter.api.MethodOrderer.OrderAnnotation;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Profile;

@SpringBootTest
@TestMethodOrder(OrderAnnotation.class)
@Profile("test")
class SparkSpikeApplicationTests {
    private static final Logger log = LoggerFactory.getLogger(SparkSpikeApplicationTests.class);
    @Autowired private String dataDirectory;
    @Autowired private ProcessingService processingService;
    @Autowired private DataProcessingService dataProcessingService;
    @Autowired private JsonXmlService jsonXmlService;

    @Test
    @Order(1)
    void contextLoads() {
        assertNotNull(ApplicationContextProvider.applicationContext());
        assertNotNull(dataDirectory);
        assertTrue(true);
    }

    @Test
    @Order(2)
    void dataDirectoryTest() {
        try {
            log.debug(
                    "dataDirectory: {}",
                    ApplicationContextProvider.applicationContext()
                            .getResource(dataDirectory)
                            .getFile()
                            .getAbsolutePath());
            log.debug(
                    "dataDirectory: {}",
                    Arrays.asList(
                            ApplicationContextProvider.applicationContext()
                                    .getResource(dataDirectory)
                                    .getFile()
                                    .list()));
        } catch (IOException e) {
            log.error("Error getting dataDirectory: ", e);
        }
    }

    @Test
    @Order(10)
    void wordCountTest() {
        String words = "a|b|c|b|a|a";
        List<String> wordList = Arrays.asList(words.split("\\|"));
        Map<String, Long> results = processingService.getCount(wordList);
        log.debug("results: {}", results);
        assertEquals(3, results.get("a").intValue());
        assertEquals(2, results.get("b").intValue());
        assertEquals(1, results.get("c").intValue());
    }

    @Test
    @Order(60)
    void dataProcessingJsonTest() throws Exception {
        try {
            File jsonFile =
                    ApplicationContextProvider.applicationContext()
                            .getResource(dataDirectory + "/test.json")
                            .getFile();

            Dataset<Row> df = dataProcessingService.showDataset(jsonFile);

            for (Row row : df.collectAsList()) {
                log.debug(
                        "row: {} {}",
                        row.get(row.fieldIndex("given_name")),
                        row.get(row.fieldIndex("surname")));
            }

            df.toDF()
                    .write()
                    .mode(SaveMode.Overwrite)
                    .option("delimiter", "|")
                    .option("header", "true")
                    .option("quoteAll", "true")
                    .csv("x.csv");
        } catch (Exception e) {
            log.error("ERROR: ", e);
        }
    }

    @Test
    @Order(65)
    void dataProcessingJdbcTest() throws Exception {
        File jsonFile =
                ApplicationContextProvider.applicationContext()
                        .getResource(dataDirectory + "/test.json")
                        .getFile();

        Dataset<Row> df = dataProcessingService.showDataset(jsonFile);
        dataProcessingService.persist(df);
    }

    @Test
    @Order(110)
    // @Disabled
    void xmlToJsonTest() throws Exception {
        File dataDirectoryFile =
                ApplicationContextProvider.applicationContext()
                        .getResource(dataDirectory)
                        .getFile();
        File jsonFile =
                new File(
                        "/home/jim/eclipse-workspace/net.ljcomputing/insurance-xml/src/test/resources/out/policy.xml");

        String json = jsonXmlService.xmlToJson(jsonFile);

        File policyJsonFile = new File(dataDirectoryFile, "/policy.json");
        log.debug("policyJsonFile: {}", policyJsonFile);

        FileWriter policyWriter = new FileWriter(policyJsonFile);
        policyWriter.write(json);
        policyWriter.close();

        log.debug("json: {}", json);

        Dataset<Row> df = dataProcessingService.policyToDf(policyJsonFile);

        for (Row row : df.collectAsList()) {
            log.debug("row: {}", ((GenericRowWithSchema) row.getAs("insured")).prettyJson());
        }

        log.debug("columns: {}", Arrays.asList(df.columns()));

        // df.toDF()
        //         .write()
        //         .mode(SaveMode.Overwrite)
        //         .option("delimiter", "|")
        //         .option("header", "true")
        //         .option("quoteAll", "true")
        //         .csv("policy.csv");
    }

    @Test
    @Order(160)
    void testRetrieve() {
        // String personQuery = "(SELECT * FROM person) as person";
        String addressQuery = "(SELECT * FROM address) as address";
        String addressTypeQuery = "(SELECT * FROM address_type) as addressType";

        // Dataset<Row> personDs = dataProcessingService.retrieve(personQuery);
        Dataset<Row> addressDs = dataProcessingService.retrieve(addressQuery);
        Dataset<Row> addressTypeDs = dataProcessingService.retrieve(addressTypeQuery);

        // rename the address_type id column to addr_type_id
        addressTypeDs = addressTypeDs.withColumnRenamed("id", "addr_type_id");

        Dataset<Row> df =
                addressDs
                        .join(
                                addressTypeDs,
                                addressDs.col("id").equalTo(addressTypeDs.col("addr_id")))
                        .drop("addr_id", "addr_type_id");

        df.show();

        df.collectAsList().stream().forEach(row -> log.debug("row: {}", row));

        Dataset<Row> addrTypesByState =
                df.groupBy("state").pivot("addr_type").count().sort("state").na().fill(0);
        addrTypesByState = addrTypesByState.withColumn("total", expr("physical+mailing+billing"));

        addrTypesByState.collectAsList().stream().forEach(row -> log.debug("row: {}", row));
    }
}
