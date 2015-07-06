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

import com.orangerhymelabs.helenusdb.cassandra.document.Document;
import com.orangerhymelabs.helenusdb.cassandra.index.Index;

/**
 * The BucketIndexer utilizes an extensible hash indexing scheme.
 * 
 * @author toddf
 * @since Jul 5, 2015
 */
public class BucketIndexer
{
	public long getBucketFor(Index index, Document document)
	{
		return 0;
	}
}
