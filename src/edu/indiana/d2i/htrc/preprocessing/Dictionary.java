package edu.indiana.d2i.htrc.preprocessing;

public interface Dictionary {

	/**
	 * Given a token, checks whether the token is contained in the dictionary
	 * 
	 * @param token
	 * @return
	 */
	public boolean contains(String token);
}
