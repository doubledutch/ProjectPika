package me.doubledutch.pikadb;

interface PageCache{
	/**
	 * Sets an item in the cache
	 * @param id id of cached item
	 * @return whether or not item is cached
	 */
	boolean Set(int id);
}