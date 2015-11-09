package me.doubledutch.pikadb;

import java.io.*;
import java.util.*;
import org.json.*;

import java.sql.*;

public class Test{
	public static void main(String args[]){
		String filename="./test.data";
		int RECORDS=100000;
		try{	
			
			String[] firstNames=new String[]{"James","John","Robert","Michael","William","David","Richard","Charles","Joseph","Thomas",
										"Mary","Patricia","Linda","Barbara","Elizabeth","Jennifer","Maria","Susan","Margaret","Dorothy",
										"Christopher","Daniel","Paul","Mark","Donald","George","Kenneth","Steven","Brian","Anthony",
										"Lisa","Nancy","Karen","Betty","Helen","Sandra","Donna","Carol","Ruth","Sharon","Michelle"};
			String[] lastNames=new String[]{"Smith","Johnson","Williams","Jones","Brown","Davis","Miller","Wilson","Moore",
										"Taylor","Anderson","Thomas","Jackson","White","Harris","Martin","Thompson","Garcia",
										"Martinez","Robinson","Clark","Rodriguez","Lewis","Lee","Walker","Hall","Allen","Young",
										"Hernandez","King","Wright","Lopez","Hill","Scott","Green","Adams","Baker","Gonzales",
										"Nelson","Carter","Mitchell","Perez","Roberts","Turner","Phillips","Campbell","Parker",
										"Evans","Edwards","Collins","Stewart","Sanchez","Morriz","Rogers","Reed","Cook","Morgan",
										"Bell","Murphy","Bailey","Rivera","Cooper","Richardson","Cox","Howard","Ward","Torres",
										"Peterson","Gray","Ramirez","James","Watson","Brooks","Kelly","Sanders","Price","Bennett",
										"Wood","Barnes","Ross","Henderson","Coleman","Jenkins","Perry","Powell","Long","Patterson",
										"Hughes","Flores","Washington","Butler","Simmons","Foster","Bryant","Alexander","Diaz",
										"Myers","Ford","Rice","West","Jordan","Owens","Fisher","Harrison","Gibson","Cruz"};
			
			/*

			// create table raw_users (record_id int not null,id varchar(38) not null, firstname varchar(128), lastname varchar(128),username varchar(256),image varchar(256));
			// create table fast_users (record_id int not null,id varchar(38) not null, firstname varchar(128), lastname varchar(128),username varchar(256),image varchar(256));
			// create index ndx_users on fast_users(record_id);


			Class.forName("org.postgresql.Driver");
			Connection con= DriverManager.getConnection("jdbc:postgresql://127.0.0.1:5432/benchdb","bench","test123");
			Statement st=con.createStatement();
			st.executeUpdate("DELETE FROM raw_users");
			st.executeUpdate("DELETE FROM fast_users");
			st.executeUpdate("VACUUM FULL");
			PreparedStatement stInsert=con.prepareStatement("INSERT INTO raw_users (record_id,id,firstName,lastName,username,image) VALUES(?,?,?,?,?,?)");
			System.out.println("PostgreSQL no index");
			System.out.println(" + Writing "+RECORDS+" objects");
			long t1=System.currentTimeMillis();
			for(int i=0;i<RECORDS;i++){
				stInsert.setInt(1,i);
				stInsert.setString(2,java.util.UUID.randomUUID().toString());
				String firstName=firstNames[(int)(Math.random()*firstNames.length)];
				stInsert.setString(3,firstName);
				String lastName=lastNames[(int)(Math.random()*lastNames.length)];
				stInsert.setString(4,lastName);
				stInsert.setString(5,firstName+"."+lastName+"."+i);
				stInsert.setString(6,"http://some-great-service/img/profile-"+i+".png");
				stInsert.executeUpdate();
			}
			long t2=System.currentTimeMillis();
			System.out.println("   - Write in "+(t2-t1)+"ms "+(int)(RECORDS/((t2-t1)/1000.0))+" obj/s");
			System.gc();
			System.out.println(" + Reading full objects - 100%");
			t1=System.currentTimeMillis();
			PreparedStatement stSelect=con.prepareStatement("SELECT record_id,id,firstName,lastName,username,image FROM raw_users");
			java.sql.ResultSet rs=stSelect.executeQuery();
			while(rs.next()){
				JSONObject obj=new JSONObject();
				obj.put("record_id",rs.getInt(1));
				obj.put("id",rs.getString(2));
				obj.put("firstName",rs.getString(3));
				obj.put("lastName",rs.getString(4));
				obj.put("username",rs.getString(5));
				obj.put("image",rs.getString(6));
			}
			t2=System.currentTimeMillis();
			System.out.println("   - Read in "+(t2-t1)+"ms "+(int)(RECORDS/((t2-t1)/1000.0))+" obj/s");
			System.gc();

			System.out.println(" + Reading two columns - 100%");
			t1=System.currentTimeMillis();
			stSelect=con.prepareStatement("SELECT record_id,username FROM raw_users");
			rs=stSelect.executeQuery();
			while(rs.next()){
				JSONObject obj=new JSONObject();
				obj.put("record_id",rs.getInt(1));
				obj.put("username",rs.getString(2));
			}
			t2=System.currentTimeMillis();
			System.out.println("   - Read in "+(t2-t1)+"ms "+(int)(RECORDS/((t2-t1)/1000.0))+" obj/s");
			System.gc();


			System.out.println(" + Predicate based queries");
			t1=System.currentTimeMillis();
			stSelect=con.prepareStatement("SELECT record_id,id,firstName,lastName,username,image FROM raw_users WHERE record_id=2");
			rs=stSelect.executeQuery();
			while(rs.next()){
				JSONObject obj=new JSONObject();
				obj.put("record_id",rs.getInt(1));
				obj.put("id",rs.getString(2));
				obj.put("firstName",rs.getString(3));
				obj.put("lastName",rs.getString(4));
				obj.put("username",rs.getString(5));
				obj.put("image",rs.getString(6));
			}
			t2=System.currentTimeMillis();
			System.out.println("   - Read early object in "+(t2-t1)+"ms");
			t1=System.currentTimeMillis();
			stSelect=con.prepareStatement("SELECT record_id,id,firstName,lastName,username,image FROM raw_users WHERE record_id="+(RECORDS/2));
			rs=stSelect.executeQuery();
			while(rs.next()){
				JSONObject obj=new JSONObject();
				obj.put("record_id",rs.getInt(1));
				obj.put("id",rs.getString(2));
				obj.put("firstName",rs.getString(3));
				obj.put("lastName",rs.getString(4));
				obj.put("username",rs.getString(5));
				obj.put("image",rs.getString(6));
			}
			t2=System.currentTimeMillis();
			System.out.println("   - Read mid object in "+(t2-t1)+"ms");
			t1=System.currentTimeMillis();
			stSelect=con.prepareStatement("SELECT record_id,id,firstName,lastName,username,image FROM raw_users WHERE record_id="+(RECORDS-2));
			rs=stSelect.executeQuery();
			while(rs.next()){
				JSONObject obj=new JSONObject();
				obj.put("record_id",rs.getInt(1));
				obj.put("id",rs.getString(2));
				obj.put("firstName",rs.getString(3));
				obj.put("lastName",rs.getString(4));
				obj.put("username",rs.getString(5));
				obj.put("image",rs.getString(6));
			}
			t2=System.currentTimeMillis();
			System.out.println("   - Read late object in "+(t2-t1)+"ms");

			t1=System.currentTimeMillis();
			stSelect=con.prepareStatement("SELECT record_id,id,firstName,lastName,username,image FROM raw_users WHERE record_id<100");
			rs=stSelect.executeQuery();
			while(rs.next()){
				JSONObject obj=new JSONObject();
				obj.put("record_id",rs.getInt(1));
				obj.put("id",rs.getString(2));
				obj.put("firstName",rs.getString(3));
				obj.put("lastName",rs.getString(4));
				obj.put("username",rs.getString(5));
				obj.put("image",rs.getString(6));
			}
			t2=System.currentTimeMillis();
			System.out.println("   - Read 100 early objects in "+(t2-t1)+"ms "+(int)(100/((t2-t1)/1000.0))+" obj/s");
			t1=System.currentTimeMillis();
			stSelect=con.prepareStatement("SELECT record_id,id,firstName,lastName,username,image FROM raw_users WHERE record_id>"+(RECORDS-100));
			rs=stSelect.executeQuery();
			while(rs.next()){
				JSONObject obj=new JSONObject();
				obj.put("record_id",rs.getInt(1));
				obj.put("id",rs.getString(2));
				obj.put("firstName",rs.getString(3));
				obj.put("lastName",rs.getString(4));
				obj.put("username",rs.getString(5));
				obj.put("image",rs.getString(6));
			}
			t2=System.currentTimeMillis();
			System.out.println("   - Read 100 late objects in "+(t2-t1)+"ms "+(int)(100/((t2-t1)/1000.0))+" obj/s");
			System.gc();

			System.out.println("PostgreSQL with index");
			stInsert=con.prepareStatement("INSERT INTO fast_users (record_id,id,firstName,lastName,username,image) VALUES(?,?,?,?,?,?)");
			System.out.println(" + Writing "+RECORDS+" objects");
			t1=System.currentTimeMillis();
			for(int i=0;i<RECORDS;i++){
				stInsert.setInt(1,i);
				stInsert.setString(2,java.util.UUID.randomUUID().toString());
				String firstName=firstNames[(int)(Math.random()*firstNames.length)];
				stInsert.setString(3,firstName);
				String lastName=lastNames[(int)(Math.random()*lastNames.length)];
				stInsert.setString(4,lastName);
				stInsert.setString(5,firstName+"."+lastName+"."+i);
				stInsert.setString(6,"http://some-great-service/img/profile-"+i+".png");
				stInsert.executeUpdate();
			}
			t2=System.currentTimeMillis();
			System.out.println("   - Write in "+(t2-t1)+"ms "+(int)(RECORDS/((t2-t1)/1000.0))+" obj/s");
			System.gc();
			System.out.println(" + Reading full objects - 100%");
			t1=System.currentTimeMillis();
			stSelect=con.prepareStatement("SELECT record_id,id,firstName,lastName,username,image FROM fast_users");
			rs=stSelect.executeQuery();
			while(rs.next()){
				JSONObject obj=new JSONObject();
				obj.put("record_id",rs.getInt(1));
				obj.put("id",rs.getString(2));
				obj.put("firstName",rs.getString(3));
				obj.put("lastName",rs.getString(4));
				obj.put("username",rs.getString(5));
				obj.put("image",rs.getString(6));
			}
			t2=System.currentTimeMillis();
			System.out.println("   - Read in "+(t2-t1)+"ms "+(int)(RECORDS/((t2-t1)/1000.0))+" obj/s");
			System.gc();

			System.out.println(" + Reading two columns - 100%");
			t1=System.currentTimeMillis();
			stSelect=con.prepareStatement("SELECT record_id,username FROM fast_users");
			rs=stSelect.executeQuery();
			while(rs.next()){
				JSONObject obj=new JSONObject();
				obj.put("record_id",rs.getInt(1));
				obj.put("username",rs.getString(2));
			}
			t2=System.currentTimeMillis();
			System.out.println("   - Read in "+(t2-t1)+"ms "+(int)(RECORDS/((t2-t1)/1000.0))+" obj/s");
			System.gc();

			System.out.println(" + Predicate based queries");
			t1=System.currentTimeMillis();
			stSelect=con.prepareStatement("SELECT record_id,id,firstName,lastName,username,image FROM fast_users WHERE record_id=2");
			rs=stSelect.executeQuery();
			while(rs.next()){
				JSONObject obj=new JSONObject();
				obj.put("record_id",rs.getInt(1));
				obj.put("id",rs.getString(2));
				obj.put("firstName",rs.getString(3));
				obj.put("lastName",rs.getString(4));
				obj.put("username",rs.getString(5));
				obj.put("image",rs.getString(6));
			}
			t2=System.currentTimeMillis();
			System.out.println("   - Read early object in "+(t2-t1)+"ms");
			t1=System.currentTimeMillis();
			stSelect=con.prepareStatement("SELECT record_id,id,firstName,lastName,username,image FROM fast_users WHERE record_id="+(RECORDS/2));
			rs=stSelect.executeQuery();
			while(rs.next()){
				JSONObject obj=new JSONObject();
				obj.put("record_id",rs.getInt(1));
				obj.put("id",rs.getString(2));
				obj.put("firstName",rs.getString(3));
				obj.put("lastName",rs.getString(4));
				obj.put("username",rs.getString(5));
				obj.put("image",rs.getString(6));
			}
			t2=System.currentTimeMillis();
			System.out.println("   - Read mid object in "+(t2-t1)+"ms");
			t1=System.currentTimeMillis();
			stSelect=con.prepareStatement("SELECT record_id,id,firstName,lastName,username,image FROM fast_users WHERE record_id="+(RECORDS-2));
			rs=stSelect.executeQuery();
			while(rs.next()){
				JSONObject obj=new JSONObject();
				obj.put("record_id",rs.getInt(1));
				obj.put("id",rs.getString(2));
				obj.put("firstName",rs.getString(3));
				obj.put("lastName",rs.getString(4));
				obj.put("username",rs.getString(5));
				obj.put("image",rs.getString(6));
			}
			t2=System.currentTimeMillis();
			System.out.println("   - Read late object in "+(t2-t1)+"ms");

			t1=System.currentTimeMillis();
			stSelect=con.prepareStatement("SELECT record_id,id,firstName,lastName,username,image FROM fast_users WHERE record_id<100");
			rs=stSelect.executeQuery();
			while(rs.next()){
				JSONObject obj=new JSONObject();
				obj.put("record_id",rs.getInt(1));
				obj.put("id",rs.getString(2));
				obj.put("firstName",rs.getString(3));
				obj.put("lastName",rs.getString(4));
				obj.put("username",rs.getString(5));
				obj.put("image",rs.getString(6));
			}
			t2=System.currentTimeMillis();
			System.out.println("   - Read 100 early objects in "+(t2-t1)+"ms "+(int)(100/((t2-t1)/1000.0))+" obj/s");
			t1=System.currentTimeMillis();
			stSelect=con.prepareStatement("SELECT record_id,id,firstName,lastName,username,image FROM fast_users WHERE record_id>"+(RECORDS-100));
			rs=stSelect.executeQuery();
			while(rs.next()){
				JSONObject obj=new JSONObject();
				obj.put("record_id",rs.getInt(1));
				obj.put("id",rs.getString(2));
				obj.put("firstName",rs.getString(3));
				obj.put("lastName",rs.getString(4));
				obj.put("username",rs.getString(5));
				obj.put("image",rs.getString(6));
			}
			t2=System.currentTimeMillis();
			System.out.println("   - Read 100 late objects in "+(t2-t1)+"ms "+(int)(100/((t2-t1)/1000.0))+" obj/s");
			System.gc();
			*/
			System.out.println("PikaDB");
			System.out.println(" + Writing "+RECORDS+" objects");
			PikaDB db=new PikaDB(filename);
			Table users=db.declareTable("users");
			long pre=System.currentTimeMillis();
			for(int i=0;i<RECORDS;i++){
				JSONObject obj=new JSONObject();
				obj.put("record_id",i);
				obj.put("id",java.util.UUID.randomUUID().toString());
				obj.put("firstName",firstNames[(int)(Math.random()*firstNames.length)]);
				obj.put("lastName",lastNames[(int)(Math.random()*lastNames.length)]);
				obj.put("username",obj.getString("firstName")+"."+obj.getString("lastName")+"."+i);
				obj.put("image","http://some-great-service/img/profile-"+i+".png");
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

			ResultSet list=null;

			// Do a quick warmup scan
			db=new PikaDB(filename);
			users=db.declareTable("users");
			list=users.select().execute();
			db.close();
			list=null;
			db=null;
			System.gc();

			System.out.println(" + Reading full objects - 100%");
			
			db=new PikaDB(filename);
			users=db.declareTable("users");
			pre=System.currentTimeMillis();
			list=users.select("*").execute();
			list.getObjectList();
			db.close();
			db=null;
			post=System.currentTimeMillis();
			System.out.println("   - Read in "+(post-pre)+"ms "+(int)(RECORDS/((post-pre)/1000.0))+" obj/s");
			System.gc();

			System.out.println(" + Reading two columns - 100%");
			
			db=new PikaDB(filename);
			users=db.declareTable("users");
			pre=System.currentTimeMillis();
			list=users.select("record_id","username").execute();
			list.getObjectList();
			db.close();
			db=null;
			post=System.currentTimeMillis();
			System.out.println("   - Read in "+(post-pre)+"ms "+(int)(RECORDS/((post-pre)/1000.0))+" obj/s");
			System.gc();

			System.out.println(" + Predicate based queries");
			db=new PikaDB(filename);
			users=db.declareTable("users");
			list=users.scan();
			pre=System.currentTimeMillis();
			list=users.select("id","record_id","username").where("record_id").equalTo(1).execute();
			list.getObjectList();
			post=System.currentTimeMillis();
			System.out.println("   - Read early object in "+(post-pre)+"ms");

			pre=System.currentTimeMillis();
			list=users.select("id","record_id","username").where("record_id").equalTo(RECORDS/2).execute();
			list.getObjectList();
			post=System.currentTimeMillis();
			System.out.println("   - Read mid object in "+(post-pre)+"ms");

			pre=System.currentTimeMillis();
			list=users.select("id","record_id","username").where("record_id").equalTo(RECORDS-2).execute();
			
			list.getObjectList();
			post=System.currentTimeMillis();
			System.out.println("   - Read late object in "+(post-pre)+"ms");
			// System.out.println(list.getExecutionPlan().toString(4));
			// System.out.println(list.getObjectList().get(0).toString());
			pre=System.currentTimeMillis();
			
			list=users.select("id","record_id","username").where("record_id").lessThan(100).execute();
			

			list.getObjectList();
			post=System.currentTimeMillis();
			System.out.println("   - Read 100 early objects in "+(post-pre)+"ms "+(int)((100)/((post-pre)/1000.0))+" obj/s");
			
			pre=System.currentTimeMillis();
			list=users.select("id","record_id","username").where("record_id").greaterThan(RECORDS-100).execute();
			list.getObjectList();
			post=System.currentTimeMillis();
			System.out.println("   - Read 100 late objects in "+(post-pre)+"ms "+(int)((100)/((post-pre)/1000.0))+" obj/s");
			System.out.println(list.getExecutionPlan().toString(4));
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