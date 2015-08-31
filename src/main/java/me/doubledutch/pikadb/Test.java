package me.doubledutch.pikadb;

import java.io.*;
import java.util.*;

public class Test{
	public static void main(String args[]){
		String filename="./test.data";
		int RECORDS=100000;
		try{
			
			System.out.println("Running test "+RECORDS);


			long pre=System.currentTimeMillis();
			PageFile f=new PageFile(filename);
			Page p1=f.createPage();
			Column col=new Column(f,p1.getId());

			for(int i=0;i<RECORDS;i++){
				Variant v=new Variant.Integer(42,i);
				col.append(v);
				// p1.appendData(v.toByteArray());
			}
			f.saveChanges();
			f.close();

			long post=System.currentTimeMillis();

			System.out.println(" Write in "+(post-pre)+"ms "+(int)(RECORDS/((post-pre)/1000.0))+" var/s");

			File ftest=new File(filename);
			System.out.println("length:"+ftest.length());

			pre=System.currentTimeMillis();
			f=new PageFile(filename);
			col=new Column(f,0);
			System.out.println("Ready to scan");
			// p1=f.getPage(0);
			// DataInput in=p1.getDataInput();
			List<Variant> list=col.scan();
			System.out.println("Records returned by scan "+list.size());
			for(int i=0;i<RECORDS;i++){
				/*Variant.Integer v=(Variant.Integer)Variant.readVariant(in);
				*/
				Variant.Integer v=(Variant.Integer)list.get(i);
				if(v.getValue()!=i){
					System.out.println("Bad data");
				}
			}
			f.close();
			post=System.currentTimeMillis();
			System.out.println(" Read in "+(post-pre)+"ms "+(int)(RECORDS/((post-pre)/1000.0))+" var/s");
			 
			
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