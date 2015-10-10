package me.doubledutch.pikadb.page;


class NoopPageCache implements PageCache{
	public boolean Set(int id) {
		return false;
	}
}