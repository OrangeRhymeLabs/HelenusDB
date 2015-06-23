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
import java.util.Collections;
import java.util.List;

import com.orangerhymelabs.orangedb.cassandra.Constants;
import com.orangerhymelabs.orangedb.cassandra.FieldType;
import com.orangerhymelabs.orangedb.cassandra.table.Table;
import com.orangerhymelabs.orangedb.cassandra.table.TableReference;
import com.orangerhymelabs.orangedb.persistence.AbstractEntity;
import com.orangerhymelabs.orangedb.persistence.Identifier;
import com.strategicgains.syntaxe.annotation.ChildValidation;
import com.strategicgains.syntaxe.annotation.RegexValidation;
import com.strategicgains.syntaxe.annotation.Required;

/**
 * @author tfredrich
 * @since Jun 8, 2015
 */
public class Index
extends AbstractEntity
{
	@RegexValidation(name = "Index Name", nullable = false, pattern = Constants.NAME_PATTERN, message = Constants.NAME_MESSAGE)
	private String name;
	private String description;

	@Required("Index Fields")
	@ChildValidation
	private List<String> fields;
	private transient List<IndexField> fieldSpecs;
	private boolean isUnique;

	@Required
	@ChildValidation
	private TableReference table;

	@Required("Index Engine")
	private IndexEngineType engineType;

	public Index()
    {
		super();
		engineType(IndexEngineType.BUCKET_INDEXER);
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

	public List<String> fields()
	{
		return (fields == null ? Collections.emptyList() : Collections.unmodifiableList(fields));
	}

	public void fields(List<String> fields)
	{
		this.fields = new ArrayList<String>(fields);
	}

	public boolean isUnique()
	{
		return isUnique;
	}

	public void isUnique(boolean isUnique)
	{
		this.isUnique = isUnique;
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

	public IndexEngineType engineType()
	{
		return engineType;
	}

	protected void engineType(IndexEngineType engineType)
	{
		this.engineType = engineType;
	}

	public String tableName()
	{
		return (table == null ? null : table.name());
	}

	public void table(String databaseName, String tableName, FieldType docIdType)
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

	public void table(Table table)
	{
		this.table = new TableReference(table);
	}

	public FieldType idType()
	{
		return table.idType();
	}

	public String toDbTable()
	{
		return getIdentifier().toDbName();
	}

	public String toColumnDefs()
	{
		StringBuilder sb = new StringBuilder();
		boolean isFirst = true;

		for (IndexField field : getFieldSpecs())
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

		for (IndexField field : getFieldSpecs())
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

    protected List<IndexField> getFieldSpecs()
	{
		if (fields == null) return Collections.emptyList();

		if (fieldSpecs == null)
		{
			this.fieldSpecs = new ArrayList<IndexField>(fields.size());
			
			for (String field : fields)
			{
				fieldSpecs.add(new IndexField(field));
			}
		}

		return fieldSpecs;
	}
}
