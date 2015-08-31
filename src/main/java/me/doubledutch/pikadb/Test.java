package me.doubledutch.pikadb;

import java.io.*;

public class Test{
	public static void main(String args[]){
		String filename="./test.data";
		try{
			
			System.out.println("Running test");

			PageFile f=new PageFile(filename);
			Page p1=f.createPage();

			for(int i=0;i<10;i++){
				Variant v=new Variant.Integer(42,i);
				p1.appendData(v.toByteArray());
			}
			f.saveChanges();
			f.close();

			File ftest=new File(filename);
			System.out.println("length:"+ftest.length());

			f=new PageFile(filename);
			p1=f.getPage(0);
			DataInput in=p1.getDataInput();
			for(int i=0;i<10;i++){
				Variant.Integer v=(Variant.Integer)Variant.readVariant(in);
				if(v.getValue()!=i){
					System.out.println("Bad data");
				}
			}
			f.close();

			 
			
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