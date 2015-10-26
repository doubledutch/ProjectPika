package me.doubledutch.pikadb;

import java.io.*;
import org.json.*;

public class PikaDB{
	private int CURRENT_VERSION=1;
	private PageFile pageFile;
	private Table systemSettings;
	private Table systemTableList;

	public PikaDB(String filename) throws IOException{
		openDatabase(filename);
	}
	public PikaDB(File file) throws IOException{
		openDatabase(file.getAbsolutePath());
	}

	private void openDatabase(String filename) throws IOException{
		pageFile=new PageFile(filename);
		if(pageFile.getPageCount()==0){
			Page page=pageFile.createPage();
			page.makeUnsortable();
			page=pageFile.createPage();
			page.makeUnsortable();
		}
		systemSettings=new Table("_pikadb.system",pageFile,0);
		systemTableList=new Table("_pikadb.tables",pageFile,1);
	}

	private void createTable(String name,int... constraints){
		Page startingPage=pageFile.createPage();
		
	}

	public Table getTable(String name) throws IOException{
		return null;
	}

	public Table declareTable(String name,int... constraints) throws IOException{
		return null;
	}
}