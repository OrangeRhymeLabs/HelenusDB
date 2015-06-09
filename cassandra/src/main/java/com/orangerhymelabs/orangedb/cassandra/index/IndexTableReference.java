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

import com.orangerhymelabs.orangedb.cassandra.table.Table;
import com.orangerhymelabs.orangedb.cassandra.table.TableReference;
import com.strategicgains.syntaxe.annotation.Required;

/**
 * @author toddf
 * @since Jun 8, 2015
 */
public class IndexTableReference
extends TableReference
{
	@Required("ID Type")
	private FieldType idType = FieldType.UUID;

	/**
	 * @param database
	 * @param table
	 */
	public IndexTableReference(String database, String table, FieldType idType)
	{
		super(database, table);
		this.idType = idType;
	}

	/**
	 * @param table
	 */
	public IndexTableReference(Table table)
	{
		this(table.databaseName(), table.name(), table.idType());
	}

	public FieldType idType()
	{
		return idType;
	}

	public Table asObject()
	{
		Table t = super.asObject();
		t.idType(idType);
		return t;
	}
}
