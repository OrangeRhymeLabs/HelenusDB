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

import java.util.Date;
import java.util.UUID;

import com.orangerhymelabs.orangedb.persistence.ObservableState;
import com.orangerhymelabs.orangedb.persistence.Observer;

/**
 * Sets the UUID identifier for the document on create, as well as the createdAt and updatedAt
 * timestamps on create and update, respectively.
 * 
 * @author toddf
 * @since May 8, 2015
 */
public class DocumentObserver
implements Observer<Document>
{
	@Override
	public void observe(ObservableState state, Document document)
	{
		switch(state)
		{
			case BEFORE_CREATE:
				document.setUuid(UUID.randomUUID());
				Date now = new Date();
				document.createdAt(now);
				document.updatedAt(now);
				break;
			case BEFORE_UPDATE:
				document.updatedAt(new Date());
				break;
			default:
				break;
		}
	}
}
