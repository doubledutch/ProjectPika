package me.doubledutch.pikadb;

import java.io.*;
import java.util.*;
import org.json.*;

public class Test{
	public static void main(String args[]){
		String filename="./test.data";
		int RECORDS=100000;
		try{	
			System.out.println(" + Writing "+RECORDS+" objects");

			long pre=System.currentTimeMillis();
			PageFile f=new PageFile(filename);
			Page p1=f.createPage();
			Soup soup=new Soup("users",f,0);

			for(int i=0;i<RECORDS;i++){
				JSONObject obj=new JSONObject();
				obj.put("id",i);
				obj.put("username","kasper.jeppesen"+i);
				obj.put("firstName","Kasper");
				obj.put("lastName","Jeppesen");
				obj.put("image","jerk-"+i+".png");
				// obj.put("number",3.1415f);
				soup.add(i,obj);
			}
			f.saveChanges();
			f.close();
			f=null;
			
			long post=System.currentTimeMillis();
			System.out.println("   - Write in "+(post-pre)+"ms "+(int)(RECORDS/((post-pre)/1000.0))+" obj/s");
			System.gc();


			File ftest=new File(filename);
			System.out.println("   - Database size : "+(ftest.length()/1024)+"kb");

			// Do a quick warmup scan
			f=new PageFile(filename);
			soup=new Soup("users",f,0);
			List<JSONObject> list=soup.scan();
			list=null;
			f.close();
			f=null;
			System.gc();

			System.out.println(" + Reading full objects");
			pre=System.currentTimeMillis();
			f=new PageFile(filename);
			soup=new Soup("users",f,0);
			list=soup.scan();
			for(int i=0;i<RECORDS;i++){
				JSONObject obj=list.get(i);
			}
			f.close();
			f=null;
			post=System.currentTimeMillis();
			System.out.println("   - Read in "+(post-pre)+"ms "+(int)(RECORDS/((post-pre)/1000.0))+" obj/s");
			System.gc();

			pre=System.currentTimeMillis();
			f=new PageFile(filename);
			soup=new Soup("users",f,0);		
			
			System.out.println(" + Reading partial objects");
			List<String> columns=new ArrayList<String>();
			columns.add("id");
			columns.add("username");
			 list=soup.scan(columns);
			for(int i=0;i<RECORDS;i++){
				JSONObject obj=list.get(i);
			}
			f.close();
			f=null;
			post=System.currentTimeMillis();
			System.out.println("   - Read in "+(post-pre)+"ms "+(int)(RECORDS/((post-pre)/1000.0))+" obj/s");
		
			System.gc();
			/*
			System.out.println(" + Reading 50% of all objects");
			pre=System.currentTimeMillis();
			f=new PageFile(filename);
			soup=new Soup("users",f,p1.getId());
			Map<Integer,JSONObject> objMap=new HashMap<Integer,JSONObject>();
			for(int i=0;i<RECORDS;i++){
				if(i%2==0){
					JSONObject obj=new JSONObject();
					objMap.put(i,obj);
				}
			}
			list=soup.scan(objMap);
			for(int i=0;i<list.size();i++){
				JSONObject obj=list.get(i);
				// System.out.println(obj.toString());
			}
			f.close();
			f=null;
			post=System.currentTimeMillis();
			System.out.println("   - Read in "+(post-pre)+"ms "+(int)((RECORDS/2)/((post-pre)/1000.0))+" obj/s");

			System.gc();
			System.out.println(" + Reading 50% of all objects partially");
			pre=System.currentTimeMillis();
			f=new PageFile(filename);
			soup=new Soup("users",f,p1.getId());
			columns=new ArrayList<String>();
			columns.add("id");
			columns.add("username");
			objMap=new HashMap<Integer,JSONObject>();
			for(int i=0;i<RECORDS;i++){
				if(i%2==0){
					JSONObject obj=new JSONObject();
					objMap.put(i,obj);
				}
			}
			list=soup.scan(objMap,columns);
			for(int i=0;i<list.size();i++){
				JSONObject obj=list.get(i);
				// System.out.println(obj.toString());
			}
			f.close();
			f=null;
			post=System.currentTimeMillis();
			System.out.println("   - Read in "+(post-pre)+"ms "+(int)((RECORDS/2)/((post-pre)/1000.0))+" obj/s");
		
			System.gc();
			System.out.println(" + Reading 25% of all objects partially");
			pre=System.currentTimeMillis();
			f=new PageFile(filename);
			soup=new Soup("users",f,p1.getId());
			columns=new ArrayList<String>();
			columns.add("id");
			columns.add("username");
			objMap=new HashMap<Integer,JSONObject>();
			for(int i=0;i<RECORDS;i++){
				if(i%4==0){
					JSONObject obj=new JSONObject();
					objMap.put(i,obj);
				}
			}
			list=soup.scan(objMap,columns);
			for(int i=0;i<list.size();i++){
				JSONObject obj=list.get(i);
				// System.out.println(obj.toString());
			}
			f.close();
			f=null;
			post=System.currentTimeMillis();
			System.out.println("   - Read in "+(post-pre)+"ms "+(int)((RECORDS/2)/((post-pre)/1000.0))+" obj/s");
			*/

			System.gc();
			System.out.println(" + Reading a single object");
			pre=System.currentTimeMillis();
			f=new PageFile(filename);
			soup=new Soup("users",f,p1.getId());
			soup.scan(RECORDS/2,columns);
			f.close();
			post=System.currentTimeMillis();
			System.out.println("   - Read in "+(post-pre)+"ms");

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