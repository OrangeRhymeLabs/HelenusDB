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
package com.orangerhymelabs.orangedb.cassandra.index;

/**
 * @author tfredrich
 * @since Jun 8, 2015
 */
public enum FieldType
{
	TEXT("text"),
	UUID("uuid"),
	TIMESTAMP("timestamp"),
	INTEGER("int"),
	BIGINT("bigint"),
	FLOAT("float"),
	DOUBLE("double"),
	DECIMAL("decimal");

	private String cassandraType;

	FieldType(String typeName)
	{
		this.cassandraType = typeName;
	}

	public String cassandraType()
	{
		return cassandraType;
	}

	public static FieldType from(String name)
    {
		switch(name.toLowerCase())
		{
			case "text": return TEXT;
			case "uuid": return UUID;
			case "timestamp": return TIMESTAMP;
			case "integer": return INTEGER;
			case "bigint": return BIGINT;
			case "float": return FLOAT;
			case "double": return DOUBLE;
			case "decimal": return DECIMAL;
			default:
				throw new IllegalStateException("Invalid type: " + name);
		}
    }
}
