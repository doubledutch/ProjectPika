package me.doubledutch.pikadb;

import me.doubledutch.pikadb.page.PageDiff;

public class WriteAheadLog{
	public WriteAheadLog(String filename){

	}

	public void addMetaData(int pageId,int nextPageId,int currentFill){

	}

	public void addPageDiff(int pageId, PageDiff diff){

	}

	public void beginTransaction(){

	}

	public void commitTransaction(){

	}

	public void closeTransaction(){
		
	}

	public void abortTransaction(){

	}
}