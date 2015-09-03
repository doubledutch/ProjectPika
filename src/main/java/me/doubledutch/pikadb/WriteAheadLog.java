package me.doubledutch.pikadb;

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