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
package net.ljcomputing.sparkspike.extractor;

import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import net.ljcomputing.sparkspike.factory.ExtractorLocatorBean;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class ExtractorLocatorFactory {
    private final Set<String> extractors = new LinkedHashSet<>();
    private final Map<String, Extrator> extractorLocatorBeans = new HashMap<>();

    ExtractorLocatorFactory(@Autowired List<ExtractorLocatorBean> extractorLocators) {
        extractorLocators.forEach(
                el -> {
                    extractors.add(el.getDatasetName());
                    extractorLocatorBeans.put(el.getDatasetName(), (Extrator) el);
                });
    }

    public final Set<String> getExtractors() {
        return extractors;
    }

    public final Extrator locate(final String datasetName) {
        return extractorLocatorBeans.get(datasetName);
    }

    public Dataset<Row> extract(final String datasetName, final Dataset<Row> source) {
        return locate(datasetName).extract(source);
    }
}
