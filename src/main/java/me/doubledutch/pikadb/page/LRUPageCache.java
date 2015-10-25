package me.doubledutch.pikadb.page;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Map;

class LRUPageCache implements PageCache{
	private int size;
	private Map<Integer, Page> pageMap;
	private Deque<Integer> pageCache;

	public LRUPageCache(Map<Integer, Page> map, int size){
		this.size=size;
		this.pageMap=map;
		this.pageCache=new ArrayDeque<Integer>();
	}

	public boolean Set(int id){
		if (!pageMap.containsKey(id)){
			return false;
		}
		boolean cached=false;
		if (pageCache.contains(id)){
			pageCache.remove(id);
			cached=true;
		}
		pageCache.addFirst(id);
		if (pageCache.size() > size){
			Integer evictID=pageCache.removeLast();
			Page evict=pageMap.get(evictID);
			evict.unloadRawData();
		}
		return cached;
	}
}	