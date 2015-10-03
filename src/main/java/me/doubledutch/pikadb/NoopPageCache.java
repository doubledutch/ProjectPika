package me.doubledutch.pikadb;

class NoopPageCache implements PageCache{
	public boolean Set(int id) {
		return false;
	}
}