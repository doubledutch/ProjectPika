package me.doubledutch.pikadb.query;

import java.io.*;
import org.json.*;
import me.doubledutch.pikadb.*;

public class Query{
	private String[] columns;
	private Predicate predicate=null;
	private Table table=null;
	private ResultSet result=null;

	public Query(Table table,String... columns){
		this.table=table;
		this.columns=columns;
	}

	public Predicate where(String column){
		return new Predicate(this,column);
	}

	public ResultSet execute() throws IOException,JSONException{
		if(predicate==null){
			if(columns==null || (columns.length==1 && columns[0].equals("*"))){
				return table.scan();
			}else{
				return table.scan(columns);
			}
		}
		result=new ResultSet();
		result.startTimer();
		// First build object set using predicates
		ObjectSet set=executePredicate(predicate);
		// Now fill in missing values from columns using object set

		result.endTimer();
		return result;
	}

	private ObjectSet executePredicate(Predicate predicate){
		ObjectSet set=new ObjectSet(false);
		if(predicate.getType()==Predicate.OR){

		}else if(predicate.getType()==Predicate.AND){

		}else if(predicate.getType()==Predicate.NOT){

		}else if(predicate.getType()==Predicate.WHERE){

		}else{

		}
		return set;
	}

	private ObjectSet executePredicate(String columnName,Predicate predicate){
		ObjectSet set=new ObjectSet(false);
		return set;
	}
}