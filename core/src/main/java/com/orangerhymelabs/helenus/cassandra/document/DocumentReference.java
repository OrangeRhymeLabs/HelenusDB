/*
 Copyright 2016, Strategic Gains, Inc.

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
package com.orangerhymelabs.helenus.cassandra.document;

import com.orangerhymelabs.helenus.persistence.Identifier;

/**
 * @author tfredrich
 * @since 19 Sep 2016
 */
public class DocumentReference
{
	private Identifier id;

	public DocumentReference(Identifier identifier)
	{
		super();
		identifier(identifier);
	}

	public DocumentReference(Document document)
	{
		this(document.identifier());
	}

	public Identifier identifier()
	{
		return (id != null ? new Identifier(id) : null);
	}

	public void identifier(Identifier identifier)
	{
		this.id = (identifier != null ? new Identifier(identifier) : null);
	}

	@Override
	public int hashCode()
	{
		int hash = 5;
		hash = 59 * hash + id.hashCode();
		return hash;
	}

	@Override
	public boolean equals(Object that)
	{
		if (that == null)
		{
			return false;
		}

		if (getClass() != that.getClass())
		{
			return false;
		}

		final DocumentReference other = (DocumentReference) that;

		return (id.compareTo(other.id) == 0);
	}

	@Override
	public String toString()
	{
		return "DocumentReference{id: " + id.toString() +  '}';
	}
}
