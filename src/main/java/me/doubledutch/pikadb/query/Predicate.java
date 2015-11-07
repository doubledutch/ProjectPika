package me.doubledutch.pikadb.query;

import me.doubledutch.pikadb.Variant;

import java.io.*;
import org.json.*;
import me.doubledutch.pikadb.ResultSet;

public class Predicate{
	public static final int EQUALS=0;
	public static final int LESSTHAN=1;
	public static final int GREATERTHAN=2;
	public static final int LIKE=3;
	public static final int ISNULL=4;
	public static final int OR=5;
	public static final int AND=6;
	public static final int NOT=7;
	public static final int WHERE=8;

	private Query query;
	private String column;
	private Variant value=null;
	private String pattern=null;
	private int type;
	private Predicate leftChild=null;
	private Predicate rightChild=null;

	protected Predicate(Query query,String column){
		this.column=column;
		this.type=WHERE;
		this.query=query;
	}

	protected Predicate(Query query,int type,Variant value){
		this.type=type;
		this.query=query;
		this.value=value;
	}

	protected Predicate(Query query,int type,String value){
		this.type=type;
		this.query=query;
		this.pattern=value;
	}

	public ResultSet execute() throws IOException,JSONException{
		return query.execute();
	}

	public int getType(){
		return type;
	}

	protected String getColumn(){
		return column;
	}

	protected String[] getColumnList(){
		String[] a=leftChild==null?new String[0]:leftChild.getColumnList();
		String[] b=rightChild==null?new String[0]:rightChild.getColumnList();
		int num=a.length+b.length;
		if(type==WHERE){
			num+=1;
		}
		if(num==0)return new String[0];
		String[] result=new String[num];
		int index=0;
		if(type==WHERE){
			result[index++]=column;
		}
		for(String key:a){
			result[index++]=key;
		}
		for(String key:b){
			result[index++]=key;
		}
		return result;
	}

	protected Predicate getLeftChild(){
		return leftChild;
	}

	protected Predicate getRightChild(){
		return rightChild;
	}

	public Predicate equalTo(int v){
		Predicate p=new Predicate(query,EQUALS,new Variant.Integer(-1,v));
		leftChild=p;
		return p;
		// TODO: check that this can only be created on a where predicate
	}

	public Predicate lessThan(int v){
		Predicate p=new Predicate(query,LESSTHAN,new Variant.Integer(-1,v));
		leftChild=p;
		return p;
		// TODO: check that this can only be created on a where predicate
	}

	public Predicate greaterThan(int v){
		Predicate p=new Predicate(query,GREATERTHAN,new Variant.Integer(-1,v));
		leftChild=p;
		return p;
		// TODO: check that this can only be created on a where predicate
	}
	/*
	public Predicate lessThan(Variant v){
		Predicate p=new Predicate(query,LESSTHAN,v);
		leftChild=p;
		return p;
		// TODO: check that this can only be created on a where predicate
	}

	public Predicate greaterThan(Variant v){
		Predicate p=new Predicate(query,GREATERTHAN,v);
		leftChild=p;
		return p;
		// TODO: check that this can only be created on a where predicate
	}

	public Predicate like(String str){
		Predicate p=new Predicate(query,LIKE,str);
		leftChild=p;
		return p;
		// TODO: check that this can only be created on a where predicate
	}
*/
/*
	public static Predicate isNull(){
		return new Predicate(ISNULL,null,null,null);
	}

	public Predicate or(Predicate p1){
		return new Predicate(OR,null,this,p1);
	}

	public Predicate and(Predicate p1){
		return new Predicate(AND,null,this,p1);
	}

	public Predicate not(){
		return new Predicate(NOT,null,this,null);
	}
*/
	public boolean testVariant(Variant v){
		// System.out.println("testVariant "+v.getType()+" vs "+value.getType());
		switch(type){
			case EQUALS:return v.compareTo(value)==0;
			case LESSTHAN:return v.compareTo(value)==1;
			case GREATERTHAN:return v.compareTo(value)==-1;
			case OR:return leftChild.testVariant(v) || rightChild.testVariant(v);
		}
		return false;
	}
}