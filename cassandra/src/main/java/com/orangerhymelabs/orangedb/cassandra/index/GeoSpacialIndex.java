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

import java.util.ArrayList;
import java.util.List;

import com.orangerhymelabs.orangedb.cassandra.table.Table;
import com.orangerhymelabs.orangedb.persistence.AbstractEntity;
import com.orangerhymelabs.orangedb.persistence.Identifier;
import com.strategicgains.syntaxe.annotation.ChildValidation;
import com.strategicgains.syntaxe.annotation.Required;

/**
 * @author tfredrich
 * @since Jun 8, 2015
 */
public class GeoSpacialIndex
extends AbstractEntity
{
	private static List<IndexField> FIELD_SPECS = new ArrayList<IndexField>(2);

	static
	{
		FIELD_SPECS.add(new IndexField("lattitude:DOUBLE"));
		FIELD_SPECS.add(new IndexField("longitude:DOUBLE"));
	}

	@Required
	@ChildValidation
	private IndexTableReference table;

	@Required("Index Engine")
	private IndexEngine engine = IndexEngine.GEO_INDEXER;

	@Override
	public Identifier getId()
	{
		return new Identifier(table.database(), table.name(), name());
	}

	public String name()
	{
		return "geolocation";
	}

	public String databaseName()
	{
		return (table == null ? null : table.database());
	}

	public String tableName()
	{
		return (table == null ? null : table.name());
	}

	public void table(String databaseName, String tableName)
	{
		//TODO: what IS the actual type of the indexed field?
		this.table = new IndexTableReference(databaseName, tableName, FieldType.DOUBLE);
	}

	public Table table()
	{
		if (table == null) return null;

		Table t = table.asObject();
		t.idType(idType());
		return t;
	}

	public void table(Table table)
	{
		this.table = new IndexTableReference(table);
	}

	public FieldType idType()
	{
		return table.idType();
	}

	public String toDbTable()
    {
		return getId().toDbName();
    }

	public String toColumnDefs()
    {
		StringBuilder sb = new StringBuilder();
		boolean isFirst = true;

		for (IndexField field : FIELD_SPECS)
		{
			if (!isFirst)
			{
				sb.append(", ");
			}

			sb.append(field.name());
			sb.append(" ");
			sb.append(field.type().cassandraType());
			isFirst = false;
		}

		return sb.toString();
    }

	public String toPkDefs()
    {
		StringBuilder sb = new StringBuilder();
		boolean isFirst = true;

		for (IndexField field : FIELD_SPECS)
		{
			if (!isFirst)
			{
				sb.append(", ");
			}

			sb.append(field.name());
			isFirst = false;
		}

		return sb.toString();
    }
}
