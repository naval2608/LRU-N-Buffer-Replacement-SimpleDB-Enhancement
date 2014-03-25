package simpledb;

import simpledb.buffer.*;
import simpledb.file.*;
import simpledb.file.*;
import simpledb.server.SimpleDB;
import java.sql.*;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import simpledb.remote.SimpleDriver;
import java.util.*;
import java.lang.*;


public class Task2 {

	public static void main(String[] args) throws InterruptedException {
		// TODO Auto-generated method stub
		
		SimpleDB.initFileLogAndBufferMgr("simpleDB");
		
		Block A = new Block("student.tbl", 0);
		Block B = new Block("fldcat.tbl", 0);
		Block C = new Block("tblcat.tbl", 0);
		Block D = new Block("course.tbl", 0);
		Block E = new Block("dept.tbl", 0);
		Block F = new Block("enroll.tbl", 0);
		
		BufferMgr bm = new BufferMgr(4,2);
		Buffer x;
		
		long t = System.currentTimeMillis();
		Thread.sleep(2000);
		x = bm.pin(A);
		bm.unpin(x);
		
		Thread.sleep(3000);
		x = bm.pin(B);
		bm.unpin(x);
		
		Thread.sleep(3000);
		x = bm.pin(A);
		bm.unpin(x);
		
		Thread.sleep(8000);
		x = bm.pin(B);
		bm.unpin(x);
		
		Thread.sleep(4000);
		x = bm.pin(C);
		bm.unpin(x);
		

		Thread.sleep(4000); //till 24
		x = bm.pin(D);
		bm.unpin(x);
		
		Thread.sleep(6000); //till 30
		x = bm.pin(E);
		bm.unpin(x);
		
		Thread.sleep(10000); //till 40
		x = bm.pin(F);
		bm.unpin(x);
		
		Thread.sleep(5000); //till 45
		x = bm.pin(A);
		bm.unpin(x);
		
		Thread.sleep(9000); //till 54
		x = bm.pin(C);
		bm.unpin(x);
		
		Thread.sleep(16000); //till 70
		x = bm.pin(D);
		bm.unpin(x);
		
		Thread.sleep(10000); //till 80
		x = bm.pin(E);
		bm.unpin(x);
		
		Thread.sleep(20000); //till 100
		x = bm.pin(F);
		bm.unpin(x);
		
		bm.display_blocks();
			
	}

}
