package me.doubledutch.pikadb;

import java.io.*;
import org.junit.Test;
import static org.junit.Assert.assertTrue;

public class VariantTest{
	@Test
	public void testInteger(){
		Variant v=new Variant.Integer(42,1337);
		assertTrue(v.getType()==Variant.INTEGER);
		assertTrue((int)(v.getObjectValue())==1337);
		assertTrue(v.getOID()==42);
		assertTrue(((Variant.Integer)v).getValue()==1337);
	}

	@Test
	public void testFloat(){
		Variant v=new Variant.Float(42,3.1415f);
		assertTrue(v.getType()==Variant.FLOAT);
		assertTrue((float)(v.getObjectValue())==3.1415f);
		assertTrue(v.getOID()==42);
		assertTrue(((Variant.Float)v).getValue()==3.1415f);
	}

	@Test
	public void testShort() {
		short value=128;
		Variant v=new Variant.Short(42,value);
		assertTrue(v.getType()==Variant.SHORT);
		assertTrue((short)(v.getObjectValue())==value);
		assertTrue(v.getOID()==42);
		assertTrue(((Variant.Short)v).getValue()==value);
	}

	@Test
	public void testByte() {
		byte value=1;
		Variant v=new Variant.Byte(42,value);
		assertTrue(v.getType()==Variant.BYTE);
		assertTrue((byte)(v.getObjectValue())==value);
		assertTrue(v.getOID()==42);
		assertTrue(((Variant.Byte)v).getValue()==value);
	}

	@Test
	public void testVariantIO() throws IOException{
		/*ByteArrayOutputStream bout=new ByteArrayOutputStream();
		DataOutputStream dout=new DataOutputStream(bout);
		Variant v1=new Variant.Integer(42,1337);
		v1.writeVariant(dout);
		Variant v3=new Variant.Float(42,3.1415f);
		v3.writeVariant(dout);
		dout.flush();

		byte[] data=bout.toByteArray();

		ByteArrayInputStream bin=new ByteArrayInputStream(data);
		DataInputStream din=new DataInputStream(bin);
		Variant v2=Variant.readVariant(din);

		assertTrue(v2.getType()==Variant.INTEGER);
		assertTrue((int)(v2.getObjectValue())==1337);
		assertTrue(v2.getOID()==42);
		assertTrue(((Variant.Integer)v2).getValue()==1337);
		
		v2=Variant.readVariant(din);
		assertTrue(v2.getType()==Variant.FLOAT);
		assertTrue((float)(v2.getObjectValue())==3.1415f);
		assertTrue(v2.getOID()==42);
		assertTrue(((Variant.Float)v2).getValue()==3.1415f);
		*/
	}
}