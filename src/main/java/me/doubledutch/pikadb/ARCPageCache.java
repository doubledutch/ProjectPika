package me.doubledutch.pikadb;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Map;

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