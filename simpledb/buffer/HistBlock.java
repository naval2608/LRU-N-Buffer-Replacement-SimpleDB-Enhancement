package simpledb.buffer;

import simpledb.file.Block;

public class HistBlock {
	Block Blockname;
	long[] block_timestamp;

	
	public HistBlock(){}
	
	public HistBlock(Block bname,int k)
	{
		Blockname = bname;
		block_timestamp = new long[k];
		for(int i=0;i<k;i++)
		{
			block_timestamp[i] = 0;
		}
	}
}
