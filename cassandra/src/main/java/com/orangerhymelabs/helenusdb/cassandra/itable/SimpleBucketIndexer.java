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

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import com.orangerhymelabs.helenusdb.cassandra.DataTypes;

/**
 * The BucketIndexer implements a bucket hash based on the most significant bits
 * of the most significant key in an index. In the end, each bucket is stored on
 * its own node in the Cassandra cluster and each indexed row in a wide row on
 * that node.
 * 
 * Buckets are equally-distributed across the size for values of long. Depending
 * on the distribution of the data (that most-significant key), we could end up
 * with hot-spots if a bucket is very big and gets queried/updated a lot.
 * 
 * @author toddf
 * @since Jul 5, 2015
 */
// TODO: consider utilizing an extensible hash indexing scheme.
public class SimpleBucketIndexer
{
	private static final int DEFAULT_SIZE = 100;

	private List<Long> buckets;
	private int size;

	public SimpleBucketIndexer()
	{
		this(DEFAULT_SIZE);
	}

	public SimpleBucketIndexer(int size)
	{
		super();
		this.size = size;
		initializeBuckets();
	}

	private void initializeBuckets()
	{
		buckets = new ArrayList<Long>(size());

		for (int i = 0; i < size(); i++)
		{
			buckets.add(computeBucketValue(i));
		}
	}

	private Long computeBucketValue(int pos)
	{
		if (pos != 0)
		{
			return ((Long.MAX_VALUE / size) * pos) - 1;
		}

		return 0l;
	}

	public int size()
	{
		return size;
	}

	public long getBucketFor(Object key, DataTypes type)
	{
		Long hash = getHashValue(key, type);
		int index = Collections.binarySearch(buckets, hash);

		if (index < 0)
		{
			index = (index + 1) * -1;
		}

		index = index % size();
		return buckets.get(index);
	}

	/**
	 * Hashes the first 8 bytes of the key into a long. If the key is less-than
	 * eight bytes in length, the backing byte array is padded with
	 * Byte.MIN_VALUE before being converted into a long via
	 * ByteBuffer.getLong() and the absolute value returned.
	 * 
	 * @param key
	 * @param type
	 * @return
	 */
	private long getHashValue(Object key, DataTypes type)
	{
		ByteBuffer bb = type.toByteBuffer(key);
		byte[] dst = new byte[8];

		if (bb.limit() >= 8)
		{
			bb.get(dst, 0, 8);
		}
		else
		{
			bb.get(dst, 0, bb.limit());
			Arrays.fill(dst, bb.limit(), 8, Byte.MIN_VALUE);
		}

		ByteBuffer out = ByteBuffer.wrap(dst);
		long hash = Math.abs(out.getLong());
		return hash;
	}
}
