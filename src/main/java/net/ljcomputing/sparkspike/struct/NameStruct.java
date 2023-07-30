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
package net.ljcomputing.sparkspike.struct;

import java.util.ArrayList;
import java.util.List;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public enum NameStruct implements ConcreateStruct {
    INSTANCE;

    public final StructType getStructType() {
        List<StructField> nameFields = new ArrayList<>();
        nameFields.add(DataTypes.createStructField("givenName", DataTypes.StringType, false));
        nameFields.add(DataTypes.createStructField("middleName", DataTypes.StringType, true));
        nameFields.add(DataTypes.createStructField("surname", DataTypes.StringType, false));
        nameFields.add(DataTypes.createStructField("suffix", DataTypes.StringType, true));
        nameFields.add(DataTypes.createStructField("birthdate", DataTypes.DateType, true));
        nameFields.add(DataTypes.createStructField("age", DataTypes.IntegerType, true));
        StructField nameField =
                DataTypes.createStructField("name", DataTypes.createStructType(nameFields), true);
        List<StructField> nameType = new ArrayList<>();
        nameType.add(nameField);
        return DataTypes.createStructType(nameType);
    }
}
