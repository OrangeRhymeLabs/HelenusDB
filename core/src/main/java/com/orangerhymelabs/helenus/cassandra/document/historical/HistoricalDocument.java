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
package com.orangerhymelabs.helenus.cassandra.document.historical;

import java.util.Date;

import org.bson.BSONObject;

import com.orangerhymelabs.helenus.cassandra.document.Document;

/**
 * A HistoricalDocument is different than a regular Document in that they are never updated in place.
 * An entirely new column in a wide-row is written instead. The history is retrievable. Also, HistoricalDocument
 * instances are not deleted. Only marked as such along with a timestamp of the deletion.
 * 
 * @author tfredrich
 * @since Jun 8, 2015
 */
public class HistoricalDocument
extends Document
{
	private boolean isDeleted;
	private Date deletedAt;

	public HistoricalDocument()
	{
		super();
	}

	public HistoricalDocument(BSONObject bson)
	{
		this();
		object(bson);
	}

	public boolean isDeleted()
	{
		return isDeleted;
	}

	public void isDeleted(boolean isDeleted)
	{
		this.isDeleted = isDeleted;
	}

	public Date deletedAt()
	{
		return deletedAt;
	}

	public void deletedAt(Date deletedAt)
	{
		this.deletedAt = deletedAt;
	}
}
