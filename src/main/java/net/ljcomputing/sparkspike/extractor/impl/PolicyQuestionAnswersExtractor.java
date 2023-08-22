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
package net.ljcomputing.sparkspike.extractor.impl;

import net.ljcomputing.sparkspike.extractor.Extrator;
import net.ljcomputing.sparkspike.utils.DatasetServiceUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.springframework.stereotype.Component;

/** Policy Question Answers dataset extractor. */
@Component
public class PolicyQuestionAnswersExtractor implements Extrator {

    /** Fields specific to a Policy Question Answers Dataset. */
    public static final String[] FIELDS = {"policyNumber", "termEffectiveDate", "questionAnswers"};

    @Override
    public String getDatasetName() {
        return "PolicyQuestionAnswers";
    }

    @Override
    public Dataset<Row> extract(final Dataset<Row> source) {
        return DatasetServiceUtils.explodeToNewDataset(source, "questionAnswers", FIELDS)
                .drop("questionAnswers");
    }
}
