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
			PikaDB db=new PikaDB(filename);
			Table users=db.declareTable("users");
			for(int i=0;i<RECORDS;i++){
				JSONObject obj=new JSONObject();
				obj.put("id",i);
				obj.put("username","kasper.jeppesen"+i);
				obj.put("firstName","Kasper");
				obj.put("lastName","Jeppesen");
				obj.put("image","jerk-"+i+".png");
				// obj.put("number",3.1415f);
				users.add(i,obj);
			}
			db.save();
			db.close();
			db=null;
			// f.close();
			// f=null;
			
			long post=System.currentTimeMillis();
			System.out.println("   - Write in "+(post-pre)+"ms "+(int)(RECORDS/((post-pre)/1000.0))+" obj/s");
			System.gc();


			File ftest=new File(filename);
			System.out.println("   - Database size : "+(ftest.length()/1024)+"kb");

			List<JSONObject> list=null;

			// Do a quick warmup scan
			db=new PikaDB(filename);
			users=db.declareTable("users");
			list=users.scan();
			db.close();
			list=null;
			db=null;
			System.gc();

			System.out.println(" + Reading full objects - 100%");
			pre=System.currentTimeMillis();
			db=new PikaDB(filename);
			users=db.declareTable("users");
			list=users.scan();
			for(int i=0;i<RECORDS;i++){
				JSONObject obj=list.get(i);
				// System.out.println(obj.toString());
				if(obj.getInt("id")!=i){
					System.out.println("   - Data error!! at "+i);
					System.out.println(obj.toString());
					System.exit(0);
				}
			}
			db.close();
			db=null;
			post=System.currentTimeMillis();
			System.out.println("   - Read in "+(post-pre)+"ms "+(int)(RECORDS/((post-pre)/1000.0))+" obj/s");
			System.gc();

			System.out.println(" + Reading full objects - 50%");
			pre=System.currentTimeMillis();
			db=new PikaDB(filename);
			users=db.declareTable("users");
			ObjectSet set=new ObjectSet(false);
			for(int i=0;i<RECORDS;i++){
				if(i%2==0){
					set.addOID(i);
					
				}
			}
			list=users.scan(set);
			for(int i=0;i<list.size();i++){
				JSONObject obj=list.get(i);
				// System.out.println(obj.toString());
			}
			db.close();
			db=null;
			post=System.currentTimeMillis();
			System.out.println("   - Read in "+(post-pre)+"ms "+(int)((RECORDS/2)/((post-pre)/1000.0))+" obj/s");
			

			System.out.println(" + Reading full objects - 25%");
			pre=System.currentTimeMillis();
			db=new PikaDB(filename);
			users=db.declareTable("users");
			set=new ObjectSet(false);
			for(int i=0;i<RECORDS;i++){
				if(i%4==0){
					set.addOID(i);
					
				}
			}
			list=users.scan(set);
			for(int i=0;i<list.size();i++){
				JSONObject obj=list.get(i);
				// System.out.println(obj.toString());
			}
			db.close();
			db=null;
			post=System.currentTimeMillis();
			System.out.println("   - Read in "+(post-pre)+"ms "+(int)((RECORDS/4)/((post-pre)/1000.0))+" obj/s");
			
			System.out.println(" + Reading full objects - 5%");
			pre=System.currentTimeMillis();
			db=new PikaDB(filename);
			users=db.declareTable("users");
			set=new ObjectSet(false);
			for(int i=0;i<RECORDS;i++){
				if(i%20==0){
					set.addOID(i);
					
				}
			}
			list=users.scan(set);
			for(int i=0;i<list.size();i++){
				JSONObject obj=list.get(i);
				// System.out.println(obj.toString());
			}
			db.close();
			db=null;
			post=System.currentTimeMillis();
			System.out.println("   - Read in "+(post-pre)+"ms "+(int)((RECORDS/20)/((post-pre)/1000.0))+" obj/s");
			
			System.out.println(" + Reading full objects - 1%");
			pre=System.currentTimeMillis();
			db=new PikaDB(filename);
			users=db.declareTable("users");
			set=new ObjectSet(false);
			for(int i=0;i<RECORDS;i++){
				if(i%100==0){
					set.addOID(i);
					
				}
			}
			list=users.scan(set);
			for(int i=0;i<list.size();i++){
				JSONObject obj=list.get(i);
				// System.out.println(obj.toString());
			}
			db.close();
			db=null;
			post=System.currentTimeMillis();
			System.out.println("   - Read in "+(post-pre)+"ms "+(int)((RECORDS/100)/((post-pre)/1000.0))+" obj/s");
			
			
			/*


			

			pre=System.currentTimeMillis();
			f=new PageFile(filename);
			soup=new Table("users",f,0,false);		
			
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


			System.out.println(" + Reading a single object - early");
			pre=System.currentTimeMillis();
			f=new PageFile(filename);
			soup=new Table("users",f,0,false);

			JSONObject obj=soup.scan(3);
			// System.out.println(obj.toString());

			post=System.currentTimeMillis();
			System.out.println("   - Read in "+(post-pre)+"ms");

			System.out.println(" + Reading a single object - mid");
			pre=System.currentTimeMillis();

			obj=soup.scan(RECORDS/2);
			// System.out.println(obj.toString());

			post=System.currentTimeMillis();
			System.out.println("   - Read in "+(post-pre)+"ms");

			System.out.println(" + Reading a single object - late");
			pre=System.currentTimeMillis();

			obj=soup.scan(RECORDS-1);
			System.out.println(obj.toString());

			post=System.currentTimeMillis();
			System.out.println("   - Read in "+(post-pre)+"ms");

			pre=System.currentTimeMillis();
			System.out.println(" + Updating a single object");
			JSONObject objUpdate=new JSONObject();
			objUpdate.put("firstName","Jason");
			objUpdate.put("background","#FFB807");
			objUpdate.put("SoMuchBoolean",false);

			soup.update(RECORDS/2,objUpdate);
			f.saveChanges(false);
			post=System.currentTimeMillis();
			System.out.println("   - Updated in "+(post-pre)+"ms");

			obj=soup.scan(RECORDS/2);
			System.out.println(obj.toString());

			pre=System.currentTimeMillis();
			System.out.println(" + Deleting a single object");
			soup.delete(RECORDS/2);
			f.saveChanges(false);
			post=System.currentTimeMillis();
			System.out.println("   - Deleted in "+(post-pre)+"ms");
			obj=soup.scan(RECORDS/2);
			// System.out.println(obj);

			f.close();
			*/
			/*
			System.out.println("1 = "+MurmurHash3.hashInt(1337,1));
			System.out.println("2 = "+MurmurHash3.hashInt(1337,2));
			System.out.println("3 = "+MurmurHash3.hashInt(1337,3));
			System.out.println("1000 = "+MurmurHash3.hashInt(1337,1000));
			*/

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