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

/**
 * @author toddf
 * @since May 5, 2015
 */
public interface Timestamped
{
	public Date createdAt();
	public void createdAt(Date date);
	public Date updatedAt();
	public void updatedAt(Date date);
}
