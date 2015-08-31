package me.doubledutch.pikadb;

import java.io.*;

public class Test{
	public static void main(String args[]){
		try{
			PageFile f=new PageFile("./test.data");
			Page p1=f.createPage();

			File ftest=new File("./test.data");
			if(ftest.exists()){
				ftest.delete();
			}
		}catch(Exception e){
			e.printStackTrace();
		}
	}
}