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

import static org.junit.Assert.assertEquals;

import java.math.BigDecimal;
import java.util.Date;
import java.util.UUID;

import org.junit.Test;

import com.datastax.driver.core.utils.UUIDs;

/**
 * @author toddf
 * @since Jul 7, 2015
 */
public class DataTypesTest
{
	@Test
	public void testTimestamp()
	{
		assertEquals("timestamp", DataTypes.TIMESTAMP.cassandraType());
		Date now = new Date();
		assertEquals((Long) now.getTime(), DataTypes.TIMESTAMP.asLong(now));
	}

	@Test
	public void testBigInt()
	{
		assertEquals("bigint", DataTypes.BIGINT.cassandraType());
		assertEquals((Long) Long.MAX_VALUE, DataTypes.BIGINT.asLong(Long.MAX_VALUE));
		assertEquals((Long) Long.MIN_VALUE, DataTypes.BIGINT.asLong(Long.MIN_VALUE));
	}

	@Test
	public void testDecimal()
	{
		assertEquals("decimal", DataTypes.DECIMAL.cassandraType());
		BigDecimal max = new BigDecimal(Double.MAX_VALUE);
		assertEquals((Long) max.unscaledValue().longValue(), DataTypes.DECIMAL.asLong(max));

		BigDecimal min = new BigDecimal(Double.MIN_VALUE);
		assertEquals((Long) min.unscaledValue().longValue(), DataTypes.DECIMAL.asLong(min));
	}

	@Test
	public void testDouble()
	{
		assertEquals("double", DataTypes.DOUBLE.cassandraType());
		assertEquals(Double.MAX_VALUE, DataTypes.DOUBLE.asLong(Double.MAX_VALUE).doubleValue(), 0.0d);
		assertEquals(Double.MIN_VALUE, DataTypes.DOUBLE.asLong(Double.MIN_VALUE).doubleValue(), 0.0d);
	}

	@Test
	public void testFloat()
	{
		assertEquals("float", DataTypes.FLOAT.cassandraType());
		assertEquals(Float.MAX_VALUE, DataTypes.FLOAT.asLong(Float.MAX_VALUE).floatValue(), 0.0d);
		assertEquals(Float.MIN_VALUE, DataTypes.FLOAT.asLong(Float.MIN_VALUE).floatValue(), 0.0d);
	}

	@Test
	public void testInteger()
	{
		assertEquals("int", DataTypes.INTEGER.cassandraType());
		assertEquals(Integer.MAX_VALUE, DataTypes.INTEGER.asLong(Integer.MAX_VALUE).intValue());
		assertEquals(Integer.MIN_VALUE, DataTypes.INTEGER.asLong(Integer.MIN_VALUE).intValue());
	}

	@Test
	public void testText()
	{
		assertEquals("text", DataTypes.TEXT.cassandraType());
		String text = "The quick, brown fox jumped over the lazy ol' dog!";
		assertEquals(text.substring(0, 8).getBytes(), DataTypes.TEXT.asLong(text));
	}

	@Test
	public void testTimeUuid()
	{
		assertEquals("timeuuid", DataTypes.TIMEUUID.cassandraType());
		UUID uuid = UUIDs.timeBased();
		assertEquals((Long) uuid.timestamp(), DataTypes.TIMEUUID.asLong(uuid));
	}

	@Test
	public void testUuid()
	{
		assertEquals("uuid", DataTypes.UUID.cassandraType());
		UUID uuid = UUIDs.random();
		assertEquals((Long) uuid.getMostSignificantBits(), DataTypes.UUID.asLong(uuid));
	}
}
