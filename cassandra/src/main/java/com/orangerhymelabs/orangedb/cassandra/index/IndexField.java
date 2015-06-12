package com.orangerhymelabs.orangedb.cassandra.index;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.orangerhymelabs.orangedb.cassandra.FieldType;
import com.strategicgains.syntaxe.ValidationException;

public class IndexField
{
	private static final Pattern FIELD_PATTERN = Pattern.compile("^[\\+-]?(\\w+):(\\w+)");

	private String name;
	private FieldType type;
	private boolean isAscending = true;

	public IndexField(String field)
	{
		super();
		Matcher m = FIELD_PATTERN.matcher(field.trim());

		if (m.matches())
		{
			name = m.group(1);
			type = FieldType.from(m.group(2));

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

	public FieldType type()
	{
		return type;
	}

	public boolean isAscending()
	{
		return isAscending;
	}
}