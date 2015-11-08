package me.doubledutch.pikadb;

import java.io.*;

public class LargeHash{
	private final static int SIZE=4; // Number of 32bit ints
	private int[] hash;

	public LargeHash(int[] data){
		this.hash=data;
	}

	public static int getSize(){ // actual size in bytes
		return SIZE*4;
	}

	public static LargeHash read(DataInput in) throws IOException{
		int[] data=new int[SIZE];
		// for()
		return null;
	}

	public void write(DataOutput out) throws IOException{

	}
}