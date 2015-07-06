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
package com.orangerhymelabs.helenusdb.cassandra.itable;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

import org.junit.Test;

import com.orangerhymelabs.helenusdb.cassandra.DataTypes;

/**
 * @author tfredrich
 * @since Jul 6, 2015
 */
public class SimpleBucketIndexerTest
{
	@Test
	public void testDistributionString()
	{
		SimpleBucketIndexer indexer = new SimpleBucketIndexer();
		Long bucket1 = indexer.getBucketFor("adam", DataTypes.TEXT);
		Long bucket2 = indexer.getBucketFor("adams", DataTypes.TEXT);
		assertEquals(bucket1, bucket2);
		Long bucket3 = indexer.getBucketFor("apple", DataTypes.TEXT);
		assertEquals(bucket3, bucket2);
		Long bucket4 = indexer.getBucketFor("zuul", DataTypes.TEXT);
		assertNotEquals(bucket4, bucket3);
		Long bucket5 = indexer.getBucketFor("zed", DataTypes.TEXT);
		assertEquals(bucket4, bucket5);
		Long bucket6 = indexer.getBucketFor("xray", DataTypes.TEXT);
		assertNotEquals(bucket5, bucket6);
		Long bucket7 = indexer.getBucketFor("yankee", DataTypes.TEXT);
		assertNotEquals(bucket7, bucket6);
		Long bucket8 = indexer.getBucketFor("yak", DataTypes.TEXT);
		assertEquals(bucket8, bucket7);
	}

	@Test
	public void testDistributionInteger()
	{
		SimpleBucketIndexer indexer = new SimpleBucketIndexer();
		Long bucket1 = indexer.getBucketFor(Integer.MAX_VALUE, DataTypes.INTEGER);
		assertEquals(Long.valueOf(0), bucket1);
		Long bucket2 = indexer.getBucketFor(Integer.MIN_VALUE, DataTypes.INTEGER);
		assertEquals(bucket1, bucket2);
		Long bucket3 = indexer.getBucketFor(0, DataTypes.INTEGER);
		assertEquals(bucket1, bucket3);
		Long bucket4 = indexer.getBucketFor(100000, DataTypes.INTEGER);
		assertNotEquals(Long.valueOf(0), bucket4);
		assertNotEquals(bucket3, bucket4);
	}

	@Test
	public void testDistributionDate()
	{
//		SimpleBucketIndexer indexer = new SimpleBucketIndexer();
//		Calendar cal = new GregorianCalendar();
//		cal.set(Calendar.YEAR, 1964);
//		cal.set(Calendar.MONTH, Calendar.DECEMBER);
//		cal.set(Calendar.DAY_OF_MONTH, 17);
//
//		Long bucket1 = indexer.getBucketFor(cal.getTime(), DataTypes.TIMESTAMP);
//		assertNotEquals(Long.valueOf(0), bucket1);
//		Long bucket2 = indexer.getBucketFor(cal.getTime(), DataTypes.TIMESTAMP);
//		assertEquals(bucket1, bucket2);
//		cal.set(Calendar.YEAR, 1999);
//		Long bucket3 = indexer.getBucketFor(cal.getTime(), DataTypes.TIMESTAMP);
//		assertEquals(bucket3, bucket2);
//		cal.set(Calendar.MONTH, Calendar.JUNE);
//		Long bucket4 = indexer.getBucketFor(cal.getTime(), DataTypes.TIMESTAMP);
//		assertNotEquals(bucket4, bucket3);
//		cal.set(Calendar.YEAR, 1944);
//		Long bucket5 = indexer.getBucketFor(cal.getTime(), DataTypes.TIMESTAMP);
//		assertEquals(bucket4, bucket5);
	}

	@Test
	public void testDistributionDouble()
	{
		SimpleBucketIndexer indexer = new SimpleBucketIndexer();
		Long bucket1 = indexer.getBucketFor(Double.MAX_VALUE, DataTypes.DOUBLE);
		assertEquals(Long.valueOf(0), bucket1);
//		Long bucket2 = indexer.getBucketFor(Double.MIN_VALUE, DataTypes.DOUBLE);
//		assertEquals(bucket1, bucket2);
//		Long bucket3 = indexer.getBucketFor(10000000.1, DataTypes.DOUBLE);
//		assertNotEquals(bucket3, bucket2);
	}

	@Test
	public void testShortStringKeysDifferent()
	{
		SimpleBucketIndexer indexer = new SimpleBucketIndexer();
		Long bucket1 = indexer.getBucketFor("aaaaa", DataTypes.TEXT);
		assertNotEquals(Long.valueOf(0), bucket1);
		Long bucket2 = indexer.getBucketFor("zzzzz", DataTypes.TEXT);
		assertNotEquals(Long.valueOf(0), bucket2);
		assertNotEquals(bucket2, bucket1);
	}

	@Test
	public void testLongStringKeysEqual()
	{
		SimpleBucketIndexer indexer = new SimpleBucketIndexer();
		Long bucket1 = indexer.getBucketFor("aaaaazzz", DataTypes.TEXT);
		assertNotEquals(Long.valueOf(0), bucket1);
		Long bucket2 = indexer.getBucketFor("aaaaazzzzzzz", DataTypes.TEXT);
		assertNotEquals(Long.valueOf(0), bucket2);
		assertEquals(bucket2, bucket1);
	}

	@Test
	public void testShortStringKeysEquals()
	{
		SimpleBucketIndexer indexer = new SimpleBucketIndexer();
		Long bucket1 = indexer.getBucketFor("a", DataTypes.TEXT);
		assertNotEquals(Long.valueOf(0), bucket1);
		Long bucket2 = indexer.getBucketFor("a", DataTypes.TEXT);
		assertNotEquals(Long.valueOf(0), bucket2);
		assertEquals(bucket2, bucket1);
	}

	@Test
	public void testEmptyStringKeysEquals()
	{
		SimpleBucketIndexer indexer = new SimpleBucketIndexer();
		Long bucket1 = indexer.getBucketFor("", DataTypes.TEXT);
		assertEquals(Long.valueOf(0), bucket1);
		Long bucket2 = indexer.getBucketFor("", DataTypes.TEXT);
		assertEquals(Long.valueOf(0), bucket2);
	}
}
