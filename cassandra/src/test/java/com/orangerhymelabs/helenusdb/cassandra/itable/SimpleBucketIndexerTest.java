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
import static org.junit.Assert.assertNotNull;

import java.nio.ByteBuffer;
import java.util.Date;
import java.util.List;

import org.junit.Test;

import com.orangerhymelabs.helenusdb.cassandra.DataTypes;

/**
 * @author tfredrich
 * @since Jul 6, 2015
 */
public class SimpleBucketIndexerTest
{
	private static final int BUCKET_COUNT = 100;
	private static final long BUCKET_FACTOR = Long.MAX_VALUE / BUCKET_COUNT;

	@Test
	public void testBucketLookup()
	{
		SimpleBucketIndexer indexer = new SimpleBucketIndexer(BUCKET_COUNT);
		List<Long> buckets = indexer.buckets();

		assertEquals(0, indexer.lookupBucket(Long.valueOf(0)));
		assertEquals(0, indexer.lookupBucket(Long.valueOf(1)));
		assertEquals(0, indexer.lookupBucket(buckets.get(1) - 1l));
		assertEquals(1, indexer.lookupBucket(buckets.get(1)));
		assertEquals(1, indexer.lookupBucket(buckets.get(1) + 1l));
		assertEquals(48, indexer.lookupBucket(buckets.get(49) - 1l));
		assertEquals(49, indexer.lookupBucket(buckets.get(49)));
		assertEquals(49, indexer.lookupBucket(buckets.get(49) + 1l));
		assertEquals(98, indexer.lookupBucket(buckets.get(99) - 1l));
		assertEquals(99, indexer.lookupBucket(buckets.get(99)));
		assertEquals(99, indexer.lookupBucket(buckets.get(99) + 1l));
		assertEquals(99, indexer.lookupBucket(Long.valueOf(Long.MAX_VALUE)));
	}

	@Test
	public void testBucketInitialization()
	{
		SimpleBucketIndexer indexer = new SimpleBucketIndexer(BUCKET_COUNT);
		List<Long> buckets = indexer.buckets();
		assertNotNull(buckets);
		assertEquals(100, buckets.size());
		assertEquals(Long.valueOf(0), buckets.get(0));
		assertEquals(Long.valueOf((BUCKET_FACTOR) - 1), buckets.get(1));
		assertEquals(Long.valueOf((BUCKET_FACTOR * 49) - 1), buckets.get(49));
		assertEquals(Long.valueOf((BUCKET_FACTOR * 98) - 1), buckets.get(98));
		assertEquals(Long.valueOf((BUCKET_FACTOR * 99) - 1), buckets.get(99));
		assertNotEquals(Long.valueOf(Long.MAX_VALUE), buckets.get(99));
	}

	@Test
	public void testMaxHash()
	{
		byte[] maxBytes = {Byte.MAX_VALUE, Byte.MAX_VALUE, Byte.MAX_VALUE, Byte.MAX_VALUE, Byte.MAX_VALUE, Byte.MAX_VALUE, Byte.MAX_VALUE, Byte.MAX_VALUE};
		Long maxHash = Math.abs(ByteBuffer.wrap(maxBytes).getLong());
		SimpleBucketIndexer indexer = new SimpleBucketIndexer(BUCKET_COUNT);
		int index = indexer.lookupBucket(maxHash);
		assertEquals(99, index);
	}

	@Test
	public void testMiddleHash()
	{
		byte[] bytes = {0, 0, 0, 0, 0, 0, 0, 0};
		Long hash = Math.abs(ByteBuffer.wrap(bytes).getLong());
		SimpleBucketIndexer indexer = new SimpleBucketIndexer(BUCKET_COUNT);
		int index = indexer.lookupBucket(hash);
		assertEquals(0, index);
	}

	@Test
	public void testMinHash()
	{
		byte[] minBytes = {Byte.MIN_VALUE, Byte.MIN_VALUE, Byte.MIN_VALUE, Byte.MIN_VALUE, Byte.MIN_VALUE, Byte.MIN_VALUE, Byte.MIN_VALUE, Byte.MIN_VALUE};
		Long minHash = Math.abs(ByteBuffer.wrap(minBytes).getLong());
		SimpleBucketIndexer indexer = new SimpleBucketIndexer(BUCKET_COUNT);
		int index = indexer.lookupBucket(minHash);
		assertEquals(99, index);
	}

	@Test
	public void testStringDistribution()
	{
		SimpleBucketIndexer indexer = new SimpleBucketIndexer(BUCKET_COUNT);
		List<Long> buckets = indexer.buckets();
		Long bucket = indexer.locateBucket("", DataTypes.TEXT);
		assertEquals(buckets.get(0), bucket);
		bucket = indexer.locateBucket("!", DataTypes.TEXT);
		assertEquals(buckets.get(25), bucket);
		bucket = indexer.locateBucket("!!!!!!!!", DataTypes.TEXT);
		assertEquals(buckets.get(25), bucket);
		bucket = indexer.locateBucket("--------", DataTypes.TEXT);
		assertEquals(buckets.get(35), bucket);
		bucket = indexer.locateBucket("AAAAAAAA", DataTypes.TEXT);
		assertEquals(buckets.get(50), bucket);
		bucket = indexer.locateBucket("aaaaaaaa", DataTypes.TEXT);
		assertEquals(buckets.get(76), bucket);
		bucket = indexer.locateBucket("apple", DataTypes.TEXT);
		assertEquals(buckets.get(76), bucket);
		bucket = indexer.locateBucket("zuul", DataTypes.TEXT);
		assertEquals(buckets.get(95), bucket);
		bucket = indexer.locateBucket("zed", DataTypes.TEXT);
		assertEquals(buckets.get(95), bucket);
		bucket = indexer.locateBucket("xray", DataTypes.TEXT);
		assertEquals(buckets.get(94), bucket);
		bucket = indexer.locateBucket("yankee", DataTypes.TEXT);
		assertEquals(buckets.get(94), bucket);
		bucket = indexer.locateBucket("yak", DataTypes.TEXT);
		assertEquals(buckets.get(94), bucket);
		bucket = indexer.locateBucket("zzzzzzzz", DataTypes.TEXT);
		assertEquals(buckets.get(95), bucket);
		bucket = indexer.locateBucket("ZZZZZZZZ", DataTypes.TEXT);
		assertEquals(buckets.get(70), bucket);
	}

	@Test
	public void testIntegerDistribution()
	{
		SimpleBucketIndexer indexer = new SimpleBucketIndexer(BUCKET_COUNT);
		List<Long> buckets = indexer.buckets();
		Long bucket1 = indexer.locateBucket(Integer.MAX_VALUE, DataTypes.INTEGER);
		assertEquals(buckets.get(99), bucket1);
		Long bucket2 = indexer.locateBucket(Integer.MIN_VALUE, DataTypes.INTEGER);
		assertEquals(bucket1, bucket2);
		Long bucket3 = indexer.locateBucket(0, DataTypes.INTEGER);
		assertEquals(buckets.get(0), bucket3);
		Long bucket4 = indexer.locateBucket(100000, DataTypes.INTEGER);
		assertEquals(bucket3, bucket4);
	}

	@Test
	public void testDateDistribution()
	{
		SimpleBucketIndexer indexer = new SimpleBucketIndexer(BUCKET_COUNT);
		List<Long> buckets = indexer.buckets();
		Long bucket = indexer.locateBucket(new Date(0), DataTypes.TIMESTAMP);
		assertEquals(buckets.get(0), bucket);
		bucket = indexer.locateBucket(new Date(1436302258041l), DataTypes.TIMESTAMP);
		assertEquals(buckets.get(0), bucket);
		bucket = indexer.locateBucket(new Date(-1436302258041l), DataTypes.TIMESTAMP);
		assertEquals(buckets.get(0), bucket);
	}

	@Test
	public void testDoubleDistribution()
	{
		SimpleBucketIndexer indexer = new SimpleBucketIndexer(BUCKET_COUNT);
		List<Long> buckets = indexer.buckets();
		Long bucket = indexer.locateBucket(Double.MAX_VALUE, DataTypes.DOUBLE);
		assertEquals(buckets.get(99), bucket);
		bucket = indexer.locateBucket(Double.MAX_VALUE / 2.0d, DataTypes.DOUBLE);
		assertEquals(buckets.get(99), bucket);
		bucket = indexer.locateBucket(Double.MIN_VALUE, DataTypes.DOUBLE);
		assertEquals(buckets.get(0), bucket);
		bucket = indexer.locateBucket(0.0d, DataTypes.DOUBLE);
		assertEquals(buckets.get(0), bucket);
	}

	@Test
	public void testShortStringKeysDifferent()
	{
		SimpleBucketIndexer indexer = new SimpleBucketIndexer(BUCKET_COUNT);
		List<Long> buckets = indexer.buckets();
		Long bucket = indexer.locateBucket("aaaaa", DataTypes.TEXT);
		assertEquals(buckets.get(76), bucket);
		bucket = indexer.locateBucket("zzzzz", DataTypes.TEXT);
		assertEquals(buckets.get(95), bucket);
	}

	@Test
	public void testLongStringKeysEqual()
	{
		SimpleBucketIndexer indexer = new SimpleBucketIndexer(BUCKET_COUNT);
		List<Long> buckets = indexer.buckets();
		Long bucket1 = indexer.locateBucket("aaaaazzz", DataTypes.TEXT);
		assertEquals(buckets.get(76), bucket1);
		Long bucket2 = indexer.locateBucket("aaaaazzzzzzz", DataTypes.TEXT);
		assertEquals(buckets.get(76), bucket2);
	}
}
