package me.doubledutch.pikadb;

import java.util.*;
import java.io.*;
import org.json.*;

public class PikaDB{
	private int CURRENT_VERSION=1;
	private PageFile pageFile;
	private Table systemSettings;
	private Table systemTableList;
	private Map<String,Table> tableMap;
	private int nextTableOID=0;

	public PikaDB(String filename) throws IOException,JSONException{
		openDatabase(filename);
	}
	public PikaDB(File file) throws IOException,JSONException{
		openDatabase(file.getAbsolutePath());
	}

	private void openDatabase(String filename) throws IOException,JSONException{
		pageFile=new PageFile(filename);
		if(pageFile.getPageCount()==0){
			Page page=pageFile.createPage();
			page.makeUnsortable();
			page=pageFile.createPage();
			page.makeUnsortable();
		}	
		systemSettings=new Table("_pikadb.system",pageFile,0,true);
		systemTableList=new Table("_pikadb.tables",pageFile,1,true);
		tableMap=new HashMap<String,Table>();
		ResultSet result=systemTableList.scan();
		List<JSONObject> list=result.getObjectList();
		nextTableOID=list.size();
		for(JSONObject obj:list){
			Table table=new Table(obj.getString("name"),pageFile,obj.getInt("root"),obj.getBoolean("preserve_order"));
			tableMap.put(obj.getString("name"),table);
		}
	}

	private Table createTable(String name,int... constraints) throws JSONException,IOException{
		Page startingPage=pageFile.createPage();
		JSONObject obj=new JSONObject();
		obj.put("name",name);
		obj.put("root",startingPage.getId());
		boolean preserve_order=false;
		for(int i:constraints){
			if(i==Constraints.PRESERVE_ORDER){
				preserve_order=true;
			}
		}
		obj.put("preserve_order",preserve_order);
		systemTableList.add(nextTableOID,obj);
		nextTableOID+=1;
		Table table=new Table(name,pageFile,startingPage.getId(),preserve_order);
		tableMap.put(name,table);
		return table;
	}

	public Table getTable(String name) throws IOException,JSONException{
		if(tableMap.containsKey(name)){
			return tableMap.get(name);
		}
		return createTable(name);
	}

	public Table declareTable(String name,int... constraints) throws IOException,JSONException{
		if(tableMap.containsKey(name)){
			Table table=tableMap.get(name);
			table.enforceConstraints(constraints);
			// TODO: Now add them to meta data
			return table;
		}
		return createTable(name,constraints);
	}

	public void save() throws IOException{
		pageFile.saveChanges();
	}
	public void close() throws IOException{
		pageFile.close();
	}
}