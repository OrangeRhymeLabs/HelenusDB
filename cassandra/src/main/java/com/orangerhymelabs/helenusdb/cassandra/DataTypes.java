package com.orangerhymelabs.helenusdb.cassandra;

import java.math.BigDecimal;
import java.util.Date;
import java.util.UUID;

import com.datastax.driver.core.BoundStatement;
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


/**
 * @author tfredrich
 * @since Jun 8, 2015
 */
public enum DataTypes
{
	TEXT("text"),
	UUID("uuid"),
	TIMEUUID("timeuuid"),
	TIMESTAMP("timestamp"),
	INTEGER("int"),
	BIGINT("bigint"),
	FLOAT("float"),
	DOUBLE("double"),
	DECIMAL("decimal");

	private String cassandraType;

	DataTypes(String typeName)
	{
		this.cassandraType = typeName;
	}

	public String cassandraType()
	{
		return cassandraType;
	}

	public void bindTo(BoundStatement bs, int bsIndex, Object value)
    {
		switch(this)
		{
			case BIGINT: bs.setLong(bsIndex, (Long) value);
			break;
			case DECIMAL: bs.setDecimal(bsIndex, (BigDecimal) value);
			break;
			case DOUBLE: bs.setDouble(bsIndex, (Double) value);
			break;
			case FLOAT: bs.setFloat(bsIndex, (Float) value);
			break;
			case INTEGER: bs.setInt(bsIndex, (Integer) value);
			break;
			case TEXT: bs.setString(bsIndex, (String) value);
			break;
			case TIMESTAMP: bs.setDate(bsIndex, (Date) value);
			break;
			case TIMEUUID:
			case UUID: bs.setUUID(bsIndex, (UUID) value);
			break;
		}
    }

	public static DataTypes from(String name)
    {
		switch(name.toLowerCase())
		{
			case "varchar":
			case "text": return TEXT;
			case "uuid": return UUID;
			case "timeuuid": return TIMEUUID;
			case "timestamp": return TIMESTAMP;
			case "int":
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
