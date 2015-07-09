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
package com.orangerhymelabs.helenusdb.cassandra;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Date;
import java.util.UUID;

import com.datastax.driver.core.BoundStatement;

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
	DECIMAL("decimal"),
	BOOLEAN("boolean");

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
			case BOOLEAN: bs.setBool(bsIndex, (Boolean) value);
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

	public Long asLong(Object value)
	{
		switch(this)
		{
			case BIGINT:
				return (Long) value;
			case BOOLEAN:
				return (((Boolean) value).booleanValue() ? 1L : 0L);
			case DECIMAL:
				return ((BigDecimal) value).longValue();
			case DOUBLE:
				return ((Double) value).longValue();
			case FLOAT:
				return ((Float) value).longValue();
			case INTEGER:
				return ((Integer) value).longValue();
			case TEXT:
				String s = (String) value;
				byte[] b = s.substring(0, Math.min(8, s.length())).getBytes();

				if (b.length < 8)
				{
					b = Arrays.copyOf(b, 8);
				}

				return ByteBuffer.wrap(b).getLong();
			case TIMESTAMP:
				return ((Date) value).getTime();
			case TIMEUUID:
				return ((UUID) value).timestamp();
			case UUID:
				return ((UUID) value).getMostSignificantBits();
			default:
				return null;
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
			case "long":
			case "bigint": return BIGINT;
			case "bool":
			case "boolean": return BOOLEAN;
			case "float": return FLOAT;
			case "double": return DOUBLE;
			case "decimal": return DECIMAL;
			default:
				throw new IllegalStateException("Invalid type: " + name);
		}
    }
}
