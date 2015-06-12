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
package com.orangerhymelabs.orangedb.cassandra.index.geo;

import java.util.List;

import com.google.common.util.concurrent.FutureCallback;
import com.orangerhymelabs.orangedb.cassandra.document.Document;
import com.orangerhymelabs.orangedb.cassandra.document.Location;
import com.orangerhymelabs.orangedb.cassandra.index.Index;
import com.orangerhymelabs.orangedb.exception.StorageException;

/**
 * @author tfredrich
 * @since Jun 10, 2015
 */
public class GeospatialIndexRepository
{
	public List<Document> isWithin(int miles, Location location)
	{
		throw new StorageException("Not Implemented Yet");
	}

	public void isWithinAsync(int miles, Location location, FutureCallback<Index> callback)
	{
		callback.onFailure(new StorageException("Not Implemented Yet"));
	}
}
