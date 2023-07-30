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
package net.ljcomputing.sparkspike.configuration;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class SparkConfig {
    @Value("${spark.app-name}")
    private String appName;

    @Value("${spark.master}")
    private String masterUri;

    @Value("${spark.data-directory}")
    private String dataDirectory;

    @Bean
    public SparkConf conf() {
        return new SparkConf().setAppName(appName).setMaster(masterUri);
    }

    @Bean
    public String dataDirectory() {
        return dataDirectory;
    }

    @Bean
    public JavaSparkContext javaSparkContext() {
        return new JavaSparkContext(conf());
    }

    @Bean
    public SparkSession sparkSession() {
        return SparkSession.builder()
                .sparkContext(javaSparkContext().sc())
                .appName(appName + " - Session")
                .getOrCreate();
    }
}
