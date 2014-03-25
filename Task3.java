package simpledb;

import simpledb.file.*;
import simpledb.server.SimpleDB;
import java.sql.*;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import simpledb.remote.SimpleDriver;
import java.util.*;

public class Task3 {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		SimpleDB.initFileMgr("simpleDB");
		FileMgr fm = SimpleDB.fileMgr();
		
		Block blk = new Block("course.tbl", 0);
		Page p1 = new Page();
		p1.read(blk);
		
		p1.setShort(80, (short)345);
		p1.write(blk);
		
		short s1 = p1.getShort(80);
		System.out.println(s1);
		
		p1.setBoolean(90, false);
		p1.write(blk);
		
		boolean b1 = p1.getBoolean(90);
		System.out.println(b1);
		
		byte[] byteinput = new byte[3];
		
		byteinput[0] = (byte) (67);
		byteinput[1] = (byte) (87);
		byteinput[2] = (byte) (27);
		
		p1.setBytes(100, byteinput);
		p1.write(blk);
		
		byte[] byteoutput = p1.getBytes(100);
		
		for(int i=0; i<byteoutput.length; i++)
		{
			System.out.println(byteoutput[i]);
		}
		
		java.util.Date d1 = new java.util.Date();
		p1.setDate(110, d1);
		p1.write(blk);
		
		java.util.Date d2 = p1.getDate(110);
		System.out.println("date : " + d2);
		
	}

}
