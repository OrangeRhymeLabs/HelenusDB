/*
    Copyright 2015, Strategic Gains, Inc.

	Licensed under the Apache License, Version 2.0 (the "License");
	you may not use this file except in compliance with the License.
	You may obtain a copy of the License at

		http://www.apache.org/licenses/LICENSE-2.0

	Unless required by applicable law or agreed to in writing, software
	distributed under the License is distributed on an "AS IS" BASIS,
	WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
	See the License for the specific language governing permissions and
	limitations under the License.
 */
package com.orangerhymelabs.helenus.cassandra.table;


/**
 * HelenusDB supports four table types:
 * DOCUMENT (default, if not specified) - indexable schema-less BSON documents.
 * SCHEMA - indexable schema-oriented (Avro) tables.
 * COUNTER - Cassandra-based named counters (cannot be indexed).
 * TIME_SERIES - Cassandra-based time-series data.
 * 
 * @author tfredrich
 * @since May 2, 2015
 */
public enum TableType
{
	DOCUMENT,
	HISTORICAL,
	SCHEMA,
	COUNTER,	// Cannot index counter tables.
	TIME_SERIES;

	public static TableType from(String name)
    {
		switch(name.toLowerCase())
		{
			case "document": return DOCUMENT;
			case "historical" : return HISTORICAL;
			case "schema": return SCHEMA;
			case "counter": return COUNTER;
			case "time_series": return TIME_SERIES;
			default:
				throw new IllegalStateException("Invalid table type: " + name);
		}
    }
}
