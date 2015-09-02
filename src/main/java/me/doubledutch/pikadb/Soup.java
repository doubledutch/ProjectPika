package me.doubledutch.pikadb;

import java.io.*;
import java.util.*;
import org.json.*;

public class Soup{
	private String name;
	private PageFile pageFile;
	private int rootPageId;
	private Column metaData;
	private Map<String,Column> columnMap;

	public Soup(String name,PageFile pageFile,int rootPageId) throws IOException{
		this.name=name;
		this.pageFile=pageFile;
		this.rootPageId=rootPageId;
		metaData=new Column(pageFile,rootPageId);
		loadColumns();
	}

	private void loadColumns() throws IOException{
		Map<String,Column> tmp=new HashMap<String,Column>();
		List<Variant> list=metaData.scan();
		int index=0;
		while(list.size()>index){
			Variant.String name=(Variant.String)list.get(index++);
			Variant.Integer pageId=(Variant.Integer)list.get(index++);
			Column col=new Column(pageFile,pageId.getValue());
			tmp.put(name.getValue(),col);
		}
		columnMap=tmp;
	}

	private Column getColumn(String name){
		return columnMap.get(name);
	}

	private Column createColumn(String name) throws IOException{
		Page page=pageFile.createPage();
		metaData.append(new Variant.String(-1,name));
		metaData.append(new Variant.Integer(-1,page.getId()));
		pageFile.saveChanges();
		Column col=new Column(pageFile,page.getId());
		columnMap.put(name,col);
		return col;
	}

	public void add(int oid,JSONObject obj) throws IOException,JSONException{
		for(String key:JSONObject.getNames(obj)){
			if(!columnMap.containsKey(key)){
				createColumn(key);
			}
			Column col=getColumn(key);
			Object value=obj.get(key);
			Variant variant=Variant.createVariant(oid,value);
			col.append(variant);
		}
	}

	public List<JSONObject> scan() throws IOException,JSONException{
		return scan(columnMap.keySet());
	}

	public List<JSONObject> scan(Collection<String> columns) throws IOException,JSONException{
		Map<Integer,JSONObject> objMap=new HashMap<Integer,JSONObject>();
		List<JSONObject> result=new ArrayList<JSONObject>();
		for(String columnName:columns){
			Column col=columnMap.get(columnName);
			List<Variant> list=col.scan();
			for(Variant v:list){
				int oid=v.getOID();
				if(!objMap.containsKey(oid)){
					JSONObject obj=new JSONObject();
					objMap.put(oid,obj);
					result.add(obj);
				}
				JSONObject obj=objMap.get(oid);
				obj.put(columnName,v.getObjectValue());
			}
		}
		return result;
	}

	public List<JSONObject> scan(Map<Integer,JSONObject> objMap,Collection<String> columns) throws IOException,JSONException{
		// Map<Integer,JSONObject> objMap=new HashMap<Integer,JSONObject>();
		List<JSONObject> result=new ArrayList<JSONObject>();
		for(JSONObject obj:objMap.values()){
			result.add(obj);
		}
		for(String columnName:columns){
			Column col=columnMap.get(columnName);
			List<Variant> list=col.scan();
			for(Variant v:list){
				int oid=v.getOID();
				if(objMap.containsKey(oid)){
					JSONObject obj=objMap.get(oid);
					obj.put(columnName,v.getObjectValue());
				}
			}
		}
		return result;
	}
}