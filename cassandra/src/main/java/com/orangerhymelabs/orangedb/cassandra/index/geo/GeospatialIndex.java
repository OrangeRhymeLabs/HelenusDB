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

import java.util.ArrayList;
import java.util.List;

import com.orangerhymelabs.orangedb.cassandra.index.AbstractIndex;
import com.orangerhymelabs.orangedb.cassandra.index.IndexEngineType;
import com.orangerhymelabs.orangedb.cassandra.index.IndexField;

/**
 * @author tfredrich
 * @since Jun 8, 2015
 */
public class GeospatialIndex
extends AbstractIndex
{
	private static final String INDEX_NAME = "geo_loc";
	private static List<IndexField> FIELD_SPECS = new ArrayList<IndexField>(2);

	static
	{
		FIELD_SPECS.add(new IndexField("lattitude:DOUBLE"));
		FIELD_SPECS.add(new IndexField("longitude:DOUBLE"));
	}

	public GeospatialIndex()
    {
		super();
		engineType(IndexEngineType.GEOSPACIAL_INDEXER);
    }

	@Override
    public String name()
	{
		return INDEX_NAME;
	}

	@Override
    protected List<IndexField> getFieldSpecs()
    {
		return FIELD_SPECS;
    }
}
