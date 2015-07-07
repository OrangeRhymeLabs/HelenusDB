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
import java.math.BigInteger;
import java.nio.ByteBuffer;
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
		assertEquals(now.getTime(), ByteBuffer.wrap(DataTypes.TIMESTAMP.toByteBuffer(now).array()).getLong());
	}

	@Test
	public void testBigInt()
	{
		assertEquals("bigint", DataTypes.BIGINT.cassandraType());
		assertEquals(Long.MAX_VALUE, ByteBuffer.wrap(DataTypes.BIGINT.toByteBuffer(Long.MAX_VALUE).array()).getLong());
		assertEquals(Long.MIN_VALUE, ByteBuffer.wrap(DataTypes.BIGINT.toByteBuffer(Long.MIN_VALUE).array()).getLong());
	}

	@Test
	public void testDecimal()
	{
		assertEquals("decimal", DataTypes.DECIMAL.cassandraType());
		BigDecimal max = new BigDecimal(Double.MAX_VALUE);
		assertEquals(max, newBigDecimal(DataTypes.DECIMAL.toByteBuffer(max)));

		BigDecimal min = new BigDecimal(Double.MIN_VALUE);
		assertEquals(min, newBigDecimal(DataTypes.DECIMAL.toByteBuffer(min)));
	}

	@Test
	public void testDouble()
	{
		assertEquals("double", DataTypes.DOUBLE.cassandraType());
		assertEquals(Double.MAX_VALUE, ByteBuffer.wrap(DataTypes.DOUBLE.toByteBuffer(Double.MAX_VALUE).array()).getDouble(), 0.0d);
		assertEquals(Double.MIN_VALUE, ByteBuffer.wrap(DataTypes.DOUBLE.toByteBuffer(Double.MIN_VALUE).array()).getDouble(), 0.0d);
	}

	@Test
	public void testFloat()
	{
		assertEquals("float", DataTypes.FLOAT.cassandraType());
		assertEquals(Float.MAX_VALUE, ByteBuffer.wrap(DataTypes.FLOAT.toByteBuffer(Float.MAX_VALUE).array()).getFloat(), 0.0d);
		assertEquals(Float.MIN_VALUE, ByteBuffer.wrap(DataTypes.FLOAT.toByteBuffer(Float.MIN_VALUE).array()).getFloat(), 0.0d);
	}

	@Test
	public void testInteger()
	{
		assertEquals("int", DataTypes.INTEGER.cassandraType());
		assertEquals(Integer.MAX_VALUE, ByteBuffer.wrap(DataTypes.INTEGER.toByteBuffer(Integer.MAX_VALUE).array()).getInt());
		assertEquals(Integer.MIN_VALUE, ByteBuffer.wrap(DataTypes.INTEGER.toByteBuffer(Integer.MIN_VALUE).array()).getInt());
	}

	@Test
	public void testText()
	{
		assertEquals("text", DataTypes.TEXT.cassandraType());
		String text = "The quick, brown fox jumped over the lazy ol' dog!";
		assertEquals(text, new String(DataTypes.TEXT.toByteBuffer(text).array()));
	}

	@Test
	public void testTimeUuid()
	{
		assertEquals("timeuuid", DataTypes.TIMEUUID.cassandraType());
		UUID uuid = UUIDs.timeBased();
		ByteBuffer bb = DataTypes.UUID.toByteBuffer(uuid);
		assertEquals(uuid, new UUID(bb.getLong(), bb.getLong()));
	}

	@Test
	public void testUuid()
	{
		assertEquals("uuid", DataTypes.UUID.cassandraType());
		UUID uuid = UUIDs.random();
		ByteBuffer bb = DataTypes.UUID.toByteBuffer(uuid);
		assertEquals(uuid, new UUID(bb.getLong(), bb.getLong()));
	}

	private BigDecimal newBigDecimal(ByteBuffer bb)
	{
		byte[] bytes = new byte[bb.capacity() - Integer.BYTES];
		bb.get(bytes);
		int scale = bb.getInt();
		BigInteger unscaled = new BigInteger(bytes);
		return new BigDecimal(unscaled, scale);
	}
}
