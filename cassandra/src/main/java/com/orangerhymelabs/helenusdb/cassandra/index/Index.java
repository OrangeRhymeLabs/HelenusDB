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

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.bson.BSONObject;

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
	private boolean isCaseSensitive = true;

	/**
	 *  Optional. If present, defines a subset of the fields in the primary table that are carried in the index.
	 *  If absent, all data elements from the primary table are carried in the index.
	 */
	private List<String> containsOnly;

	@Required
	@ChildValidation
	private TableReference table;

	public Index()
    {
		super();
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

	public int size()
	{
		return (fields == null ? 0 : fields.size());
	}

	public List<String> containsOnly()
	{
		return containsOnly;
	}

	public void containsOnly(List<String> containsProperties)
	{
		this.containsOnly = (containsProperties == null ? null : new ArrayList<String>(containsProperties));
	}

	public boolean isUnique()
	{
		return isUnique;
	}

	public void isUnique(boolean isUnique)
	{
		this.isUnique = isUnique;
	}

	public boolean isCaseSensitive()
	{
		return isCaseSensitive;
	}

	public void isCaseSensitive(boolean isCaseSensitive)
	{
		this.isCaseSensitive = isCaseSensitive;
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

	public void table(Table table)
	{
		this.table = new TableReference(table);
	}

	public DataTypes idType()
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

		for (IndexField field : fieldSpecs())
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

		for (IndexField field : fieldSpecs())
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

	public String toClusterOrderings()
	{
		StringBuilder sb = new StringBuilder();
		boolean isFirst = true;

		for (IndexField field : fieldSpecs())
		{
			if (!isFirst)
			{
				sb.append(", ");
			}

			sb.append(field.name());

			if (field.isAscending())
			{
				sb.append(" ASC");
			}
			else
			{
				sb.append(" DESC");
			}

			isFirst = false;
		}

		return sb.toString();
	}

    public List<IndexField> fieldSpecs()
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

	public Map<String, Object> extractBindings(BSONObject bsonObject)
    {
		if (bsonObject == null || bsonObject.keySet().isEmpty()) return Collections.emptyMap();

		Map<String, Object> bindings = new LinkedHashMap<String, Object>(size());

	    for (IndexField indexKey : fieldSpecs())
		{
			Object value = bsonObject.get(indexKey.name());

			if (value != null)
			{
				bindings.put(indexKey.name(), value);
			}
		}

	    return bindings;
    }
}
