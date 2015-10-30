package me.doubledutch.pikadb.query;

import java.io.*;
import org.json.*;
import me.doubledutch.pikadb.*;

public class Query{
	private String[] columns;
	private Predicate predicate=null;
	private Table table=null;

	public Query(Table table,String... columns){
		this.table=table;
		this.columns=columns;
	}

	public ResultSet execute() throws IOException,JSONException{
		if(predicate==null){
			if(columns==null || (columns.length==1 && columns[0].equals("*"))){
				return table.scan();
			}else{
				return table.scan(columns);
			}
		}
		return null;
	}
}