package me.doubledutch.pikadb;

import java.io.*;
import java.util.*;
import org.json.*;

public class Test{
	public static void main(String args[]){
		String filename="./test.data";
		int RECORDS=50000;
		try{
			
			System.out.println("Running test "+RECORDS);


			long pre=System.currentTimeMillis();
			PageFile f=new PageFile(filename);
			Page p1=f.createPage();
			Soup soup=new Soup("users",f,p1.getId());

			for(int i=0;i<RECORDS;i++){
				JSONObject obj=new JSONObject();
				obj.put("id",i);
				obj.put("username","kasper.jeppesen"+i);
				obj.put("firstName","Kasper");
				obj.put("lastName","Jeppesen");
				soup.add(i,obj);
			}
			f.saveChanges();
			f.close();

			long post=System.currentTimeMillis();

			System.out.println(" Write in "+(post-pre)+"ms "+(int)(RECORDS/((post-pre)/1000.0))+" obj/s");

			File ftest=new File(filename);
			System.out.println("length:"+ftest.length());

			pre=System.currentTimeMillis();
			f=new PageFile(filename);
			soup=new Soup("users",f,0);

			System.out.println("Scanning full objects");

			List<JSONObject> list=soup.scan();
			System.out.println("Records returned by scan "+list.size());
			for(int i=0;i<RECORDS;i++){
				JSONObject obj=list.get(i);
			}
			f.close();
			post=System.currentTimeMillis();
			System.out.println(" Read in "+(post-pre)+"ms "+(int)(RECORDS/((post-pre)/1000.0))+" obj/s");
			
			pre=System.currentTimeMillis();
			f=new PageFile(filename);
			soup=new Soup("users",f,0);
			System.out.println("Scanning partial objects");
			List<String> columns=new ArrayList<String>();
			columns.add("id");
			columns.add("username");
			list=soup.scan(columns);
			for(int i=0;i<RECORDS;i++){
				JSONObject obj=list.get(i);
			}
			f.close();
			post=System.currentTimeMillis();
			System.out.println(" Read in "+(post-pre)+"ms "+(int)(RECORDS/((post-pre)/1000.0))+" obj/s");
		
		}catch(Exception e){
			e.printStackTrace();
		}
		try{
			File ftest=new File(filename);
			if(!ftest.delete()){
				System.out.println("Couldn`t delete file");
			}
		}catch(Exception e){}
	}
}