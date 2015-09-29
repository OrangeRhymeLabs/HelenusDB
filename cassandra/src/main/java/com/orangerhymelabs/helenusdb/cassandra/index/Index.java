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
package com.orangerhymelabs.helenusdb.cassandra.index;

import com.orangerhymelabs.helenusdb.cassandra.Constants;
import com.orangerhymelabs.helenusdb.cassandra.DataTypes;
import com.orangerhymelabs.helenusdb.cassandra.table.Table;
import com.orangerhymelabs.helenusdb.cassandra.table.TableReference;
import com.orangerhymelabs.helenusdb.persistence.AbstractEntity;
import com.orangerhymelabs.helenusdb.persistence.Identifier;
import com.strategicgains.syntaxe.annotation.ChildValidation;
import com.strategicgains.syntaxe.annotation.RegexValidation;
import com.strategicgains.syntaxe.annotation.Required;

/**
 * @author tfredrich
 * @since Jun 8, 2015
 */
public abstract class Index
extends AbstractEntity
{
	@RegexValidation(name = "Index Name", nullable = false, pattern = Constants.NAME_PATTERN, message = Constants.NAME_MESSAGE)
	private String name;
	private String description;

	@Required("Table Reference")
	@ChildValidation
	private TableReference table;

	@Required("Index Engine")
	private IndexEngine engine;

	public Index()
    {
		super();
    }

	protected Index(IndexEngine engine)
	{
		super();
		this.engine = engine;
	}

	public IndexEngine engine()
	{
		return engine;
	}

	public boolean isLucene()
	{
		return IndexEngine.LUCENE.equals(engine);
	}

	public boolean isSolr()
	{
		return IndexEngine.SOLR.equals(engine);
	}

	public boolean isElasticSearch()
	{
		return IndexEngine.ELASTIC_SEARCH.equals(engine);
	}

	/**
	 * Returns true if the index engine is external (not native Cassandra).
	 * For example, Lucene, ElasticSearch, SOLR, etc.
	 * 
	 * @return true if the index engine is external.
	 */
	public boolean isExternal()
	{
		return (isLucene() || isSolr() || isElasticSearch());
	}

	public boolean isBucketedView()
	{
		return IndexEngine.BUCKETED_VIEW.equals(engine);
	}

	@Override
	public Identifier getIdentifier()
	{
		return new Identifier(table.database(), table.name(), name());
	}

	public String description()
	{
		return description;
	}

	public void description(String description)
	{
		this.description = description;
	}

	public String name()
	{
		return name;
	}

	public void name(String name)
	{
		this.name = name;
	}

	public String databaseName()
	{
		return (table == null ? null : table.database());
	}

	public String tableName()
	{
		return (table == null ? null : table.name());
	}

	public void table(String databaseName, String tableName, DataTypes docIdType)
	{
		this.table = new TableReference(databaseName, tableName, docIdType);
	}

	public Table table()
	{
		if (table == null) return null;

		Table t = table.toTable();
		t.idType(idType());
		return t;
	}

	public DataTypes idType()
	{
		return table.idType();
	}

	public void table(Table table)
	{
		this.table = new TableReference(table);
	}

	public String toDbTable()
	{
		return getIdentifier().toDbName();
	}
}
