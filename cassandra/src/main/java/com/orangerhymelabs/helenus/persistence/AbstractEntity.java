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
package com.orangerhymelabs.helenus.persistence;

import java.util.Date;
import java.util.Objects;

/**
 * @author toddf
 * @since May 5, 2015
 */
public abstract class AbstractEntity
implements Identifiable, Timestamped
{
	private Date createdAt;
	private Date updatedAt;

	@Override
	public Date createdAt()
	{
		return createdAt;
	}

	@Override
    public void createdAt(Date date)
    {
		this.createdAt = date;
    }

	@Override
	public Date updatedAt()
	{
		return updatedAt;
	}

	@Override
    public void updatedAt(Date date)
    {
		this.updatedAt = date;
    }

	@Override
	public boolean equals(Object object)
	{
		AbstractEntity that = (AbstractEntity) object;
		if (that == null) return false;

		if (!Objects.equals(this.createdAt, that.createdAt))
		{
			return false;
		}

		if (!Objects.equals(this.updatedAt, that.updatedAt))
		{
			return false;
		}

		return true;
	}
}
