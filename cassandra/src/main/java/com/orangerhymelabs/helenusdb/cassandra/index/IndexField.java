package com.orangerhymelabs.helenusdb.cassandra.index;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.orangerhymelabs.helenusdb.cassandra.DataTypes;
import com.strategicgains.syntaxe.ValidationException;

public class IndexField
{
	private static final Pattern FIELD_PATTERN = Pattern.compile("^[\\+-]?(\\w+):(\\w+)");

	private String name;
	private DataTypes type;
	private boolean isAscending = true;

	public IndexField(String field)
	{
		super();
		Matcher m = FIELD_PATTERN.matcher(field.trim());

		if (m.matches())
		{
			name = m.group(1);
			type = DataTypes.from(m.group(2));

			if (field.trim().startsWith("-"))
			{
				isAscending = false;
			}
		}
		else
		{
			throw new ValidationException("Invalid field specification: " + field);
		}
	}

	public String name()
	{
		return name;
	}

	public DataTypes type()
	{
		return type;
	}

	public boolean isAscending()
	{
		return isAscending;
	}
}