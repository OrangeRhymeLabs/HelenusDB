package com.orangerhymelabs.helenus.cassandra.meta;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.Base64;

public class MD5
{
	private final byte[] md5;

	public MD5(byte[] md5)
	{
		this.md5 = md5;
	}

	/**
	 * Factory method converts String into MD5 object.
	 */
	public static MD5 ofString(String str)
	{
		try
		{
			MessageDigest md = MessageDigest.getInstance("MD5");
			md.update(str.getBytes());
			return new MD5(md.digest());
		}
		catch (NoSuchAlgorithmException e)
		{
			throw new RuntimeException(e);
		}
	}

	@Override
	public int hashCode()
	{
		return Arrays.hashCode(this.md5);
	}

	@Override
	public boolean equals(Object o)
	{
		if (this == o)
		{
			return true;
		}

		if (o == null)
		{
			return false;
		}

		if (!(o instanceof MD5))
		{
			return false;
		}

		MD5 otherMd5 = (MD5) o;
		return Arrays.equals(this.md5, otherMd5.md5);
	}

	/**
	 * returns a Base64 encoded version of the MD5 digest. 
	 */
	public String asBase64()
	{
		return Base64.getEncoder().encodeToString(this.md5);
	}
}