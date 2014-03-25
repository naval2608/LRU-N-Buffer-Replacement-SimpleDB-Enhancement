package simpledb.buffer;

import simpledb.file.Block;

public class LastOfBlock {
	Block blockname;
	long last_timestamp;

	public LastOfBlock(Block bname) {
		// TODO Auto-generated constructor stub
		blockname = bname;
		last_timestamp = 0;
	}

}
