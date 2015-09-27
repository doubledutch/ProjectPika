package me.doubledutch.pikadb;

import java.io.*;
import java.nio.channels.*;
import java.util.*;

interface PageCache{
	/**
	 * Sets an item in the cache
	 * @param id id of cached item
	 * @return whether or not item is cached
	 */
	boolean Set(int id);
}

class NoopPageCache implements PageCache{
	public boolean Set(int id) {
		return false;
	}
}

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

class SLRUPageCache implements PageCache{
	private int size;
	private Map<Integer, Page> pageMap;
	private Deque<Integer> l1, l2;

	public SLRUPageCache(Map<Integer, Page> map, int size){
		this.size=size;
		this.pageMap=map;
		this.l1=new ArrayDeque<Integer>();
		this.l2=new ArrayDeque<Integer>();
	}

	public boolean Set(int id){
		if (!pageMap.containsKey(id)){
			return false;
		}
		// If the page exists in L1, move it to L2
		// If L2 at size, move end to back of L1
		if (l1.contains(id)){
			l1.remove(id);
			l2.addLast(id);
			if (l2.size() > size){
				l1.addFirst(l2.removeLast());
			}
			return true;
		}
		// If page exists in L2, move to L2 front
		if (l2.contains(id)){
			l2.remove(id);
			l2.addFirst(id);
			return true;
		}
		// Put at front of L1 cache
		l1.addFirst(id);
		// If L1 size is at maximum, evict last page
		if (l1.size() > size){
			Integer evictID=l1.removeLast();
			Page evict=pageMap.get(evictID);
			evict.unloadRawData();
		}
		return false;
	}
}

/**
 * Adaptive Replacement Cache
 * Adapted from http://code.activestate.com/recipes/576532/
 */
class ARCPageCache implements PageCache{
	private int c, p;
	private Map<Integer, Page> pageMap;
	private Deque<Integer> t1, t2, b1, b2;

	public ARCPageCache(Map<Integer, Page> map, int size){
		this.c=size;
		this.p=0;
		this.pageMap=map;
		t1=new ArrayDeque<Integer>();
		t2=new ArrayDeque<Integer>();
		b1=new ArrayDeque<Integer>();
		b2=new ArrayDeque<Integer>();
	}

	public boolean Set(int id){
		if (t1.contains(id)){
			t1.remove(id);
			t2.addFirst(id);
			return true;
		}
		if (t2.contains(id)){
			t2.remove(id);
			t2.addFirst(id);
			return true;
		}
		if (b1.contains(id)){
			p = Math.min(c, p+Math.max(b2.size()/b1.size(), 1));
			replace(id);
			b1.remove(id);
			t2.addFirst(id);
			return false;
		}
		if (b2.contains(id)){
			p = Math.max(0, p-Math.max(b1.size()/b2.size(),1));
			replace(id);
			b2.remove(id);
			t2.addFirst(id);
			return false;
		}
		if (t1.size()+b1.size()==c){
			if (t1.size()<c){
				b1.removeLast();
				replace(id);
			} else {
				t1.removeLast();
				pageMap.get(id).unloadRawData();
			}
		} else {
			int total=t1.size()+b1.size()+t2.size()+b2.size();
			if (total >= c){
				if (total == 2*c){
					b2.removeLast();
				}
				replace(id);
			}
		}
		t1.addFirst(id);
		return false;
	}

	private void replace(int id){
		Integer old;
		if (this.t1.size() > 0 && ((b2.contains(id) && t1.size() == this.p) || (t1.size() > this.p))){
			old=t1.removeLast();
			b1.addFirst(old);
		} else {
			old=t2.removeLast();
			b2.addFirst(old);
		}
		pageMap.get(old).unloadRawData();
	}
}