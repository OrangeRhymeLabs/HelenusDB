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

import static org.junit.Assert.*;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

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
		assertTrue(buckets.get(99) < Long.valueOf(Long.MAX_VALUE));
	}

	@Test
	public void testComputeBucketIndex()
	{
		SimpleBucketIndexer indexer = new SimpleBucketIndexer(BUCKET_COUNT);
		List<Long> buckets = indexer.buckets();

		assertEquals(0, indexer.computeBucketIndex(Long.valueOf(0)));
		assertEquals(0, indexer.computeBucketIndex(Long.valueOf(1)));
		assertEquals(0, indexer.computeBucketIndex(buckets.get(1) - 1l));
		assertEquals(1, indexer.computeBucketIndex(buckets.get(1)));
		assertEquals(1, indexer.computeBucketIndex(buckets.get(1) + 1l));
		assertEquals(48, indexer.computeBucketIndex(buckets.get(49) - 1l));
		assertEquals(49, indexer.computeBucketIndex(buckets.get(49)));
		assertEquals(49, indexer.computeBucketIndex(buckets.get(49) + 1l));
		assertEquals(98, indexer.computeBucketIndex(buckets.get(99) - 1l));
		assertEquals(99, indexer.computeBucketIndex(buckets.get(99)));
		assertEquals(99, indexer.computeBucketIndex(buckets.get(99) + 1l));
		assertEquals(99, indexer.computeBucketIndex(Long.valueOf(Long.MAX_VALUE)));
	}

	@Test
	public void testMaxHash()
	{
		byte[] maxBytes = {Byte.MAX_VALUE, Byte.MAX_VALUE, Byte.MAX_VALUE, Byte.MAX_VALUE, Byte.MAX_VALUE, Byte.MAX_VALUE, Byte.MAX_VALUE, Byte.MAX_VALUE};
		Long maxHash = Math.abs(ByteBuffer.wrap(maxBytes).getLong());
		SimpleBucketIndexer indexer = new SimpleBucketIndexer(BUCKET_COUNT);
		int index = indexer.computeBucketIndex(maxHash);
		assertEquals(99, index);
	}

	@Test
	public void testMiddleHash()
	{
		byte[] bytes = {0, 0, 0, 0, 0, 0, 0, 0};
		Long hash = Math.abs(ByteBuffer.wrap(bytes).getLong());
		SimpleBucketIndexer indexer = new SimpleBucketIndexer(BUCKET_COUNT);
		int index = indexer.computeBucketIndex(hash);
		assertEquals(0, index);
	}

	@Test
	public void testMinHash()
	{
		byte[] minBytes = {Byte.MIN_VALUE, Byte.MIN_VALUE, Byte.MIN_VALUE, Byte.MIN_VALUE, Byte.MIN_VALUE, Byte.MIN_VALUE, Byte.MIN_VALUE, Byte.MIN_VALUE};
		Long minHash = Math.abs(ByteBuffer.wrap(minBytes).getLong());
		SimpleBucketIndexer indexer = new SimpleBucketIndexer(BUCKET_COUNT);
		int index = indexer.computeBucketIndex(minHash);
		assertEquals(99, index);
	}

	@Test
	public void testStringDistribution()
	{
		SimpleBucketIndexer indexer = new SimpleBucketIndexer(BUCKET_COUNT);
		List<Long> buckets = indexer.buckets();
		Long bucket = indexer.locateBucket("", DataTypes.TEXT);
		assertEquals(buckets.get(98), bucket);
		bucket = indexer.locateBucket("!", DataTypes.TEXT);
		assertEquals(buckets.get(91), bucket);
		bucket = indexer.locateBucket("!!!!!!!!", DataTypes.TEXT);
		assertEquals(buckets.get(87), bucket);
		bucket = indexer.locateBucket("--------", DataTypes.TEXT);
		assertEquals(buckets.get(76), bucket);
		bucket = indexer.locateBucket("A", DataTypes.TEXT);
		assertEquals(buckets.get(31), bucket);
		bucket = indexer.locateBucket("AA", DataTypes.TEXT);
		assertEquals(buckets.get(86), bucket);
		bucket = indexer.locateBucket("AAA", DataTypes.TEXT);
		assertEquals(buckets.get(84), bucket);
		bucket = indexer.locateBucket("AAAA", DataTypes.TEXT);
		assertEquals(buckets.get(7), bucket);
		bucket = indexer.locateBucket("AAAAA", DataTypes.TEXT);
		assertEquals(buckets.get(99), bucket);
		bucket = indexer.locateBucket("AAAAAA", DataTypes.TEXT);
		assertEquals(buckets.get(98), bucket);
		bucket = indexer.locateBucket("AAAAAAA", DataTypes.TEXT);
		assertEquals(buckets.get(79), bucket);
		bucket = indexer.locateBucket("AAAAAAAA", DataTypes.TEXT);
		assertEquals(buckets.get(63), bucket);
		bucket = indexer.locateBucket("a", DataTypes.TEXT);
		assertEquals(buckets.get(10), bucket);
		bucket = indexer.locateBucket("aaaaaaaa", DataTypes.TEXT);
		assertEquals(buckets.get(48), bucket);
		bucket = indexer.locateBucket("I", DataTypes.TEXT);
		assertEquals(buckets.get(74), bucket);
		bucket = indexer.locateBucket("II", DataTypes.TEXT);
		assertEquals(buckets.get(82), bucket);
		bucket = indexer.locateBucket("III", DataTypes.TEXT);
		assertEquals(buckets.get(82), bucket);
		bucket = indexer.locateBucket("IIII", DataTypes.TEXT);
		assertEquals(buckets.get(82), bucket);
		bucket = indexer.locateBucket("IIIII", DataTypes.TEXT);
		assertEquals(buckets.get(82), bucket);
		bucket = indexer.locateBucket("IIIIII", DataTypes.TEXT);
		assertEquals(buckets.get(82), bucket);
		bucket = indexer.locateBucket("IIIIIII", DataTypes.TEXT);
		assertEquals(buckets.get(82), bucket);
		bucket = indexer.locateBucket("IIIIIIII", DataTypes.TEXT);
		assertEquals(buckets.get(82), bucket);
		bucket = indexer.locateBucket("i", DataTypes.TEXT);
		assertEquals(buckets.get(67), bucket);
		bucket = indexer.locateBucket("ii", DataTypes.TEXT);
		assertEquals(buckets.get(99), bucket);
		bucket = indexer.locateBucket("iii", DataTypes.TEXT);
		assertEquals(buckets.get(99), bucket);
		bucket = indexer.locateBucket("iiii", DataTypes.TEXT);
		assertEquals(buckets.get(99), bucket);
		bucket = indexer.locateBucket("iiiii", DataTypes.TEXT);
		assertEquals(buckets.get(99), bucket);
		bucket = indexer.locateBucket("iiiiii", DataTypes.TEXT);
		assertEquals(buckets.get(99), bucket);
		bucket = indexer.locateBucket("iiiiiii", DataTypes.TEXT);
		assertEquals(buckets.get(99), bucket);
		bucket = indexer.locateBucket("iiiiiiii", DataTypes.TEXT);
		assertEquals(buckets.get(99), bucket);
		bucket = indexer.locateBucket("V", DataTypes.TEXT);
		assertEquals(buckets.get(74), bucket);
		bucket = indexer.locateBucket("VVVVVVVV", DataTypes.TEXT);
		assertEquals(buckets.get(82), bucket);
		bucket = indexer.locateBucket("v", DataTypes.TEXT);
		assertEquals(buckets.get(67), bucket);
		bucket = indexer.locateBucket("vvvvvvvv", DataTypes.TEXT);
		bucket = indexer.locateBucket("Z", DataTypes.TEXT);
		assertEquals(buckets.get(74), bucket);
		bucket = indexer.locateBucket("ZZ", DataTypes.TEXT);
		assertEquals(buckets.get(82), bucket);
		bucket = indexer.locateBucket("ZZZ", DataTypes.TEXT);
		assertEquals(buckets.get(82), bucket);
		bucket = indexer.locateBucket("ZZZZ", DataTypes.TEXT);
		assertEquals(buckets.get(82), bucket);
		bucket = indexer.locateBucket("ZZZZZ", DataTypes.TEXT);
		assertEquals(buckets.get(82), bucket);
		bucket = indexer.locateBucket("ZZZZZZ", DataTypes.TEXT);
		assertEquals(buckets.get(82), bucket);
		bucket = indexer.locateBucket("ZZZZZZZ", DataTypes.TEXT);
		assertEquals(buckets.get(82), bucket);
		bucket = indexer.locateBucket("ZZZZZZZZ", DataTypes.TEXT);
		assertEquals(buckets.get(82), bucket);
		bucket = indexer.locateBucket("z", DataTypes.TEXT);
		assertEquals(buckets.get(67), bucket);
		bucket = indexer.locateBucket("zz", DataTypes.TEXT);
		assertEquals(buckets.get(99), bucket);
		bucket = indexer.locateBucket("zzz", DataTypes.TEXT);
		assertEquals(buckets.get(99), bucket);
		bucket = indexer.locateBucket("zzzz", DataTypes.TEXT);
		assertEquals(buckets.get(99), bucket);
		bucket = indexer.locateBucket("zzzzz", DataTypes.TEXT);
		assertEquals(buckets.get(99), bucket);
		bucket = indexer.locateBucket("zzzzzz", DataTypes.TEXT);
		assertEquals(buckets.get(99), bucket);
		bucket = indexer.locateBucket("zzzzzzz", DataTypes.TEXT);
		assertEquals(buckets.get(99), bucket);
		bucket = indexer.locateBucket("zzzzzzzz", DataTypes.TEXT);
		assertEquals(buckets.get(99), bucket);
	}

	@Test
	public void testIntegerDistribution()
	{
		SimpleBucketIndexer indexer = new SimpleBucketIndexer(BUCKET_COUNT);
		List<Long> buckets = indexer.buckets();
		Long bucket = indexer.locateBucket(Integer.MAX_VALUE, DataTypes.INTEGER);
		assertEquals(buckets.get(0), bucket);
		bucket = indexer.locateBucket(Integer.MAX_VALUE / 2, DataTypes.INTEGER);
		assertEquals(buckets.get(0), bucket);
		bucket = indexer.locateBucket(Integer.MIN_VALUE, DataTypes.INTEGER);
		assertEquals(buckets.get(0), bucket);
		bucket = indexer.locateBucket(0, DataTypes.INTEGER);
		assertEquals(buckets.get(0), bucket);
		bucket = indexer.locateBucket(100000, DataTypes.INTEGER);
		assertEquals(buckets.get(0), bucket);
		bucket = indexer.locateBucket(2000000000, DataTypes.INTEGER);
		assertEquals(buckets.get(0), bucket);
	}

	@Test
	public void testDateDistribution()
	{
		SimpleBucketIndexer indexer = new SimpleBucketIndexer(BUCKET_COUNT);
		List<Long> buckets = indexer.buckets();

		// Wed Dec 31 17:00:00 MST 1969
		Long bucket = indexer.locateBucket(new Date(0), DataTypes.TIMESTAMP);
		assertEquals(buckets.get(0), bucket);

		// Thu Dec 31 17:00:00 MST 1970
		bucket = indexer.locateBucket(new Date(31536000000L), DataTypes.TIMESTAMP);
		assertEquals(buckets.get(68), bucket);

		// Tue Dec 31 17:00:00 MST 1968
		bucket = indexer.locateBucket(new Date(-31536000000L), DataTypes.TIMESTAMP);
		assertEquals(buckets.get(68), bucket);

		// Tue Jul 07 14:50:58 MDT 2015
		bucket = indexer.locateBucket(new Date(1436302258041L), DataTypes.TIMESTAMP);
		assertEquals(buckets.get(83), bucket);

		// Thu Jun 26 20:09:01 MST 1924
		bucket = indexer.locateBucket(new Date(-1436302258041L), DataTypes.TIMESTAMP);
		assertEquals(buckets.get(83), bucket);
	}

	@Test
	public void testDoubleDistribution()
	{
		SimpleBucketIndexer indexer = new SimpleBucketIndexer(BUCKET_COUNT);
		List<Long> buckets = indexer.buckets();
		Long bucket = indexer.locateBucket(Double.MAX_VALUE, DataTypes.DOUBLE);
		assertEquals(buckets.get(99), bucket);

		// TODO: fix this distribution problem.
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
		Long bucket1 = indexer.locateBucket("aaaaa", DataTypes.TEXT);
		Long bucket2 = indexer.locateBucket("zzzzz", DataTypes.TEXT);
		assertNotEquals(bucket1, bucket2);
	}

	@Test
	public void testLongStringKeysEqual()
	{
		SimpleBucketIndexer indexer = new SimpleBucketIndexer(BUCKET_COUNT);
//		List<Long> buckets = indexer.buckets();
		Long bucket1 = indexer.locateBucket("aaaaazzz", DataTypes.TEXT);
//		assertEquals(buckets.get(23), bucket1);
		Long bucket2 = indexer.locateBucket("aaaaazzzzzzz", DataTypes.TEXT);
//		assertEquals(buckets.get(23), bucket2);
		assertEquals(bucket1, bucket2);
	}
}
