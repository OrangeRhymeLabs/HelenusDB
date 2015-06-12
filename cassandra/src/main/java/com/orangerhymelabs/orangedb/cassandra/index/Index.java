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
import com.strategicgains.syntaxe.annotation.ChildValidation;
import com.strategicgains.syntaxe.annotation.RegexValidation;
import com.strategicgains.syntaxe.annotation.Required;

/**
 * @author tfredrich
 * @since Jun 8, 2015
 */
public class Index
extends AbstractIndex
{
	@RegexValidation(name = "Index Name", nullable = false, pattern = Constants.NAME_PATTERN, message = Constants.NAME_MESSAGE)
	private String name;
	private String description;

	@Required("Index Fields")
	@ChildValidation
	private List<String> fields;
	private transient List<IndexField> fieldSpecs;
	private boolean isUnique;

	public Index()
    {
		super();
		engineType(IndexEngineType.BUCKET_INDEXER);
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

	@Override
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
