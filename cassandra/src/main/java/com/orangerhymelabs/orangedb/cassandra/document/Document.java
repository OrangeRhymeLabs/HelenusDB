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
package com.orangerhymelabs.orangedb.cassandra.document;

import org.bson.BSONObject;

import com.orangerhymelabs.orangedb.cassandra.table.Table;
import com.orangerhymelabs.orangedb.cassandra.table.TableReference;
import com.orangerhymelabs.orangedb.persistence.AbstractEntity;
import com.orangerhymelabs.orangedb.persistence.Identifier;
import com.strategicgains.syntaxe.annotation.ChildValidation;
import com.strategicgains.syntaxe.annotation.Required;

/**
 * @author tfredrich
 * @since Jun 8, 2015
 */
public class Document
extends AbstractEntity
{
	private Object id;

	@Required("Table")
	@ChildValidation
	private TableReference table;

	// The BSON document.
	private BSONObject bson;

	// Optional location information.
	@ChildValidation
	private Location location;

	public Document()
	{
	}

	@Override
	public Identifier getId()
	{
		return new Identifier(databaseName(), tableName(), id, updatedAt());
	}

	public Object id()
	{
		return id;
	}

	public void id(Object id)
	{
		this.id = id;
	}

	public boolean hasTable()
	{
		return (table != null);
	}

	public Table table()
	{
		return (hasTable() ? table.asObject() : null);
	}

	public void table(String database, String table)
	{
		this.table = new TableReference(database, table);
	}

	public void table(Table table)
	{
		this.table = (table != null ? new TableReference(table) : null);
	}

	public String tableName()
	{
		return (hasTable() ? table.name() : null);
	}

	public String databaseName()
	{
		return (hasTable() ? table.database() : null);
	}

	public boolean hasObject()
	{
		return (bson != null);
	}

	public BSONObject object()
	{
		return bson;
	}

	public void object(BSONObject bson)
	{
		this.bson = bson;
	}

	@Override
	public String toString()
	{
		return "Document{" + "id=" + id + ", table=" + table + ", object=" + object() + '}';
	}

}
