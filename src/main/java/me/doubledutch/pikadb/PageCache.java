package me.doubledutch.pikadb;

import java.io.*;
import java.nio.channels.*;
import java.util.*;

interface PageCache{
	void Set(int id);
}

class NoopPageCache implements PageCache{
	public void Set(int id) {
		// Noop
	}
}

class LRUPageCache implements PageCache{
	private int size;
	private Map<Integer, Page> pageMap;
	private Deque<Page> pageCache;

	public LRUPageCache(Map<Integer, Page> map, int size){
		this.size = size;
		this.pageMap = map;
		this.pageCache = new LinkedList<Page>();
	}

	public void Set(int id){
		if (!pageMap.containsKey(id)){
			return;
		}
		Page page=pageMap.get(id);
		if (pageCache.contains(page)){
			pageCache.remove(page);
		}
		pageCache.addFirst(page);
		if (pageCache.size() > size){
			Page evict=pageCache.removeLast();
			evict.unloadRawData();
		}
	}
}

class SLRUPageCache implements PageCache{
	private int size;
	private Map<Integer, Page> pageMap;
	private Deque<Page> lOne;
	private Deque<Page> lTwo;

	public SLRUPageCache(Map<Integer, Page> map, int size){
		this.size = size;
		this.pageMap = map;
		this.lOne = new LinkedList<Page>();
		this.lTwo = new LinkedList<Page>();
	}

	public void Set(int id){
		if (!pageMap.containsKey(id)){
			return;
		}
		// If the page exists in L1, move it to L2
		// If L2 at size, move end to back of L1
		Page page=pageMap.get(id);
		if (lOne.contains(page)){
			lOne.remove(page);
			lTwo.addLast(page);
			if (lTwo.size() > size){
				Page toOne=lTwo.removeLast();
				lOne.addFirst(toOne);
			}
			return;
		}
		// If page exists in L2, move to L2 front
		if (lTwo.contains(page)){
			lTwo.remove(page);
			lTwo.addFirst(page);
			return;
		}
		// If L1 size is maximum, move back to L2 front
		// Put at front of L1 cache
		lOne.addFirst(page);
		if (lOne.size() > size){
			Page evict=lOne.removeLast();
			evict.unloadRawData();
		}
	}
}