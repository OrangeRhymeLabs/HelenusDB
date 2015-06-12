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

import com.orangerhymelabs.orangedb.cassandra.index.bucket.BucketIndexEngine;
import com.orangerhymelabs.orangedb.cassandra.index.geo.GeospatialIndexEngine;
import com.orangerhymelabs.orangedb.exception.StorageException;

/**
 * @author tfredrich
 * @since Jun 10, 2015
 */
public class IndexEngineFactory
{
	public IndexEngine create(IndexEngineType engine)
	{
		switch(engine)
		{
			case BUCKET_INDEXER:
				return new BucketIndexEngine();
			case GEOSPACIAL_INDEXER:
				return new GeospatialIndexEngine();
			default:
				throw new StorageException("Invalid IndexEngineType: " + engine.toString());
		}
	}
}
