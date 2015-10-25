package me.doubledutch.pikadb.page;

public class PageDiff{
	private int offset;
	private byte[] data;

	public PageDiff(int offset,byte[] data){
		this.offset=offset;
		this.data=data;
	}

	public int getOffset(){
		return offset;
	}

	public byte[] getData(){
		return data;
	}
}