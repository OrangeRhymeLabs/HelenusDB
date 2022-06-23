/**
 * 
 */
package com.orangerhymelabs.helenus.persistence;

import com.datastax.driver.core.PreparedStatement;

/**
 * @author tfredrich
 *
 */
public interface StatementFactory
{
	PreparedStatement create();
	PreparedStatement delete();
	PreparedStatement exists();
	PreparedStatement update();
	PreparedStatement read();
	PreparedStatement readAll();
}
