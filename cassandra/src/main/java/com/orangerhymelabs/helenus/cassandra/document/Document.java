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
package com.orangerhymelabs.helenus.cassandra.document;

import org.bson.BSONObject;

import com.orangerhymelabs.helenus.persistence.AbstractEntity;
import com.orangerhymelabs.helenus.persistence.Identifier;
import com.strategicgains.syntaxe.annotation.Required;

/**
 * @author tfredrich
 * @since Jun 8, 2015
 */
public class Document
extends AbstractEntity
{
	@Required("Document ID")
	private Object id;

	// The BSON document.
	private BSONObject bson;

	public Document()
	{
		super();
	}

	public Document(BSONObject bson)
	{
		this();
		object(bson);
	}

	@Override
	public Identifier getIdentifier()
	{
		return new Identifier(id);
	}

	public Object id()
	{
		return id;
	}

	public void id(Object id)
	{
		this.id = id;
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
		return "Document{" + "id=" + id + ", object=" + object() + '}';
	}

}
