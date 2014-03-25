package simpledb.buffer;

import simpledb.file.*;
import simpledb.server.SimpleDB;

import java.sql.SQLException;
import java.util.*;

import javax.swing.text.Highlighter.HighlightPainter;

/**
 * Manages the pinning and unpinning of buffers to blocks.
 * @author Edward Sciore
 *
 */
class BasicBufferMgr {
   private Buffer[] bufferpool;
   private int numAvailable;
   private Map<String,Buffer> hash_buff = null;
   private HistBlock[] histblock;
   private int k;
   private int no_of_buffs;
   private LastOfBlock[] last_block;
   private static final long Ref_period = 10000; // 2 milli seconds
   
   /**
    * Creates a buffer manager having the specified number 
    * of buffer slots.
    * This constructor depends on both the {@link FileMgr} and
    * {@link simpledb.log.LogMgr LogMgr} objects 
    * that it gets from the class
    * {@link simpledb.server.SimpleDB}.
    * Those objects are created during system initialization.
    * Thus this constructor cannot be called until 
    * {@link simpledb.server.SimpleDB#initFileAndLogMgr(String)} or
    * is called first.
    * @param numbuffs the number of buffer slots to allocate
    */
   BasicBufferMgr(int numbuffs, int k_param) {
      bufferpool = new Buffer[numbuffs];
      histblock = new HistBlock[1000];
      last_block = new LastOfBlock[1000];
      numAvailable = numbuffs;
      for (int i=0; i<numbuffs; i++)
      {
         bufferpool[i] = new Buffer();
         //String buff_name = bufferpool[i].toString();
         //histbuff[i] = new HistBlock(bufferpool[i],k);
         //last_buff[i] = new LastOfBuff(bufferpool[i]);
      }
      hash_buff = new HashMap<String, Buffer>();
      no_of_buffs = numbuffs;
      k = k_param;
   }
   
   /**
    * Flushes the dirty buffers modified by the specified transaction.
    * @param txnum the transaction's id number
    */
   synchronized void flushAll(int txnum) {
      for (Buffer buff : bufferpool)
         if (buff.isModifiedBy(txnum))
         buff.flush();
   }
   
   /**
    * Pins a buffer to the specified block. 
    * If there is already a buffer assigned to that block
    * then that buffer is used;  
    * otherwise, an unpinned buffer from the pool is chosen.
    * Returns a null value if there are no available buffers.
    * @param blk a reference to a disk block
    * @return the pinned buffer
    */
   synchronized Buffer pin(Block blk,long cur_time) {
      Buffer buff = findExistingBuffer(blk);
      if (buff == null) {
         buff = chooseUnpinnedBuffer(blk,cur_time);
         if (buff == null)
            return null;
         buff.assignToBlock(blk);
         //added code
         hash_buff.put(blk.toString(), buff);
         //System.out.println("***choosing unpinned:" + blk + ":" + buff );
      }
      if (!buff.isPinned())
         numAvailable--;
      buff.pin();
      return buff;
   }
   
   synchronized void display_blocks()
   {
	   for(Buffer buff:bufferpool)
	   {
		   System.out.print(buff.block().toString() + "\t");
	   }
   }
   
   /**
    * Allocates a new block in the specified file, and
    * pins a buffer to it. 
    * Returns null (without allocating the block) if 
    * there are no available buffers.
    * @param filename the name of the file
    * @param fmtr a pageformatter object, used to format the new block
    * @return the pinned buffer
    */
   synchronized Buffer pinNew(String filename, PageFormatter fmtr,long cur_time) {
	  //System.out.println("Starting");
	  //long cur_time = System.currentTimeMillis();
	  //System.out.println("pin new TIME:" + cur_time);
	  Buffer buff = chooseUnpinnedBuffer(cur_time);
	  
	  String victim_blk = null;
      if(buff.block() != null)
      {
    	  victim_blk = buff.block().toString();
      }
	  
	  if (buff == null)
         return null;
      buff.assignToNew(filename, fmtr);
      //added code
      hash_buff.put(buff.block().toString(),buff);    
       
      /***adding code to iitialize history for new block***/
      
      Block blk = buff.block();
      int old_blk = -1;
      //long cur_time = System.currentTimeMillis();
      
    //allocate the new block
	  for(LastOfBlock val : last_block)
	  {
		  if(val != null)
		  {
			  if(val.blockname.equals(blk))
    		  {
    			 old_blk = 1;
    			 break;
    		  }
		  }
		  else
			  break;
		  
	  }
	  
	  if(old_blk == -1)
	  {
		  int count = 0;
		  for(LastOfBlock val : last_block)
		  {
			  if(val != null)
			  {
				  count++;
			  }
			  else
				  break;
			 
		  }
		  histblock[count] = new HistBlock(blk,k);
	      last_block[count] = new LastOfBlock(blk);
	  }
	  
		  //update the hist buff for the incoming block and set the hist_p_1 to current timestamp
		  for(HistBlock v_hist_blk:histblock)
		  {
			  if(v_hist_blk.Blockname.equals(blk))
			  {
				  for(int i=0; i<= (k-2); i++)
	    		  {
	    			  v_hist_blk.block_timestamp[i] = v_hist_blk.block_timestamp[i+1];
	    		  }
				  v_hist_blk.block_timestamp[k-1] = cur_time;
				  break;
			  }	    		 
		  }
		  
		  for(LastOfBlock v_last_blk:last_block)
		  {
			  if(v_last_blk.blockname.equals(blk))
			  {
				  v_last_blk.last_timestamp = cur_time;
				  break;
			  }
		  }
		  if(victim_blk != null)
		  {
			  System.out.println(cur_time + " " + buff.toString() + " " + victim_blk + " "+ blk.toString());
		  }
		  else
		  {
			  System.out.println(cur_time + " " + buff.toString() + " " + "\t\t\t\t" + " "+ blk.toString());
		  }
      /***************************/
      
      
      numAvailable--;
      buff.pin();
      return buff;
   }
   
   /**
    * Unpins the specified buffer.
    * @param buff the buffer to be unpinned
    */
   synchronized void unpin(Buffer buff) {
      buff.unpin();
      //System.out.println("***unpinning buffer:" +  buff);
      if (!buff.isPinned())
      {
    	 hash_buff.values().remove(buff);
         numAvailable++;
      }
   }
   
   /**
    * Returns the number of available (i.e. unpinned) buffers.
    * @return the number of available buffers
    */
   int available() {
      return numAvailable;
   }
   
   private Buffer findExistingBuffer(Block blk) {
	   Buffer buff =  hash_buff.get(blk.toString());
	   if( buff != null)
	   {
		   //System.out.println("***Got a buffer:" + blk + ":" + buff);
		   return buff;
	   }
	   /*
	    for (Buffer buff : bufferpool) {
         Block b = buff.block();
         if (b != null && b.equals(blk))
         {
        	 System.out.println("Got a buffer:" + blk + ":" + buff);
        	 return buff;
         }
            
      }*/
	  
      return null;
   }
   
   private Buffer chooseUnpinnedBuffer(long cur_time) {
	      /*for (Buffer buff : bufferpool)
	         if (!buff.isPinned())
	         return buff;
	      return null;*/
	   		
		  int empty_buff = -1;
    	  Buffer victim = null;
    	  int old_blk = -1;
    	  
    	  //choosing if any empty buffer is present or not
    	  for (Buffer buff : bufferpool)
          {
    		 if(empty_buff == 1)
    		 {
    			 break;
    		 }
             if (!buff.isPinned())
             {
            	 Block b = buff.block();
                 if (b == null)
                 {
                	empty_buff = 1;
               		victim = buff;
                  	break;	  
          		 }
               			 
              }
          }
    	  
    	  if(empty_buff == 1)
    	  {
    		  //System.out.println("New block,bufferpool is empty, using victim buff:" + victim);
    		  return victim;
    	  }
    	  else if(empty_buff == -1)
    	  {
	    	  //System.out.println("Buffer does not contain the new block, and all the buffers are full. Hence choosing a victim to replace!!");
	    	  long min = cur_time;
	    	  victim = null;
	    	  long last_q = 0;
	    	  long hist_q_ref_counter = 0;
	    	  int ref_counter = 1;
	    	  Block victim_blk = null;
	    	  
	    	  
	    	  HistBlock[] list_of_victim_blks = new HistBlock[no_of_buffs];
	    	  //System.out.println(histblock);
	    	  //System.out.println(last_block);
	    	  //for all pages q in the buffer do
	    	  //		if t – LAST(q) > Correlated Reference Period /*eligible for replacement*/ and HIST(q,K) < min /* maximum Backward K–distance so
	    	  //choosing the victim to replace
	    	  
	    	  
	    	  //get all the blocks which are in buffer..
	    	  
	    	  int j=0;
	    	  for(Buffer buff:bufferpool)
	    	  {
	    		  
	    		  Block cur_blk = null;
	    		  cur_blk = buff.block();
	    		  for(HistBlock val : histblock)
	    		  {
	    			  if(val.Blockname.equals(cur_blk))
	    			  {
	    				  //list_of_victim_blks[i] = new HistBlock(cur_blk,k);
	    				  list_of_victim_blks[j] = val;
	    				  break;
	    			  }
	    			  
	    		  }
	    		  j++;
	    	 }
	    	  /*
	    	  for(int a=0;a < list_of_victim_blks.length;a++)
	    	  {
	    		  System.out.println("No param:" + list_of_victim_blks[a].Blockname.toString());
	    		  for(int b=0;b < list_of_victim_blks[a].block_timestamp.length;b++)
	    			  System.out.println(list_of_victim_blks[a].block_timestamp[b]);
	    	  }
	    	  */
	    	  for(int y = ref_counter; y <= k; y++)
	    	  {
	    		  //System.out.println("ref_counter:" + y);
	    		  if(victim_blk != null)
	    		  {
	    			  //System.out.println("got victim block:" + victim_blk.toString());
	    			  break;
	    		  }
		    	  for(HistBlock var_victim_blk:list_of_victim_blks)
		    	  {
		    		  //find all block with reference equals to ref_counter
		    		  //check if they are valid blocks or not.
		    		  int no_of_expected_zeros = k - y;
		    		  int got_no_of_zeros = 0;
		    		  for(int z = 0 ; z < k-y ; z++)
		    		  {
		    			  if(var_victim_blk.block_timestamp[z] == 0)
		    			  {
		    				  got_no_of_zeros++;
		    				  //System.out.println("number of zeros:" + got_no_of_zeros);
		    			  }
		    		  }
		    		  if(got_no_of_zeros == no_of_expected_zeros)
		    		  {
		    			  //System.out.println("inside this");
		    			  //get the last_p for this victim block from lastofblock
		    			  for(LastOfBlock var_get_last_timestamp : last_block)
		    			  {
		    				  if(var_get_last_timestamp.blockname.equals(var_victim_blk.Blockname))
		    				  {
		    					  last_q = var_get_last_timestamp.last_timestamp;
		    					  break;
		    				  }
		    			  }//got the timestamp from the last_block for this victim
		    			  //System.out.println("last time:" + last_q);
		    			  hist_q_ref_counter = var_victim_blk.block_timestamp[k-y];
		    			  //System.out.println("hist last time:" + hist_q_ref_counter);
		    			  if((cur_time - last_q > Ref_period) && (hist_q_ref_counter < min))
	        			  {
		    				  //System.out.println("inside 2");
	        				  victim_blk = var_victim_blk.Blockname;
	        				  min = hist_q_ref_counter;
	        				  //break;
	        			  }
		    			  
		    		  }
		    	  }
		    	  //System.out.println("Traversal complete for all buffs");
	    	  }
	    	  
	    	 	  	  
	    	  for(Buffer buff:bufferpool)
	    	  {
	    		  //System.out.println("victims:" + buff.toString() + ":" + buff.block().toString());
	    		  try{
		    		  if(buff.block().equals(victim_blk))
		    		  {
		    			  victim = buff;
		    			  break;
		    		  }
	    		  }
	    		  catch(Exception e)
	    		  {
	    			  System.out.println("There is no victim to replace!! Program is exiting");
	    			  //throw new SQLException(e);
	    		  }
	    	  }
	    	  //System.out.println("bufferpool is occupied, no param, using victim buff:" + victim);
	    	  return victim;
    	  }//end of else empty_buff is -1
		  
		  
	   return null;
	   }
   
      
   private Buffer chooseUnpinnedBuffer(Block blk,long cur_time) {
	  Buffer choose_buff = null;
	  //long cur_time = System.currentTimeMillis();
	  //System.out.println("pin TIME:" + cur_time);
	  
      for (Buffer buff : bufferpool)
      {
         if (!buff.isPinned())
         {
        	 Block b = buff.block();
             if (b != null && b.equals(blk))
             {
            	 //System.out.println("Using same buffer block:" + blk + ",buffer:" +  buff);
            	 choose_buff = buff;
            	 break;
             }
        	 
         }
      }
      //if p is already in the buffer then
      // update history information of p
      if (choose_buff != null)
      {
    	  long last_p = 0;
    	  for(LastOfBlock val:last_block)
    	  {
    		  if(val.blockname.equals(blk))
    		  {
    			  last_p = val.last_timestamp;
    			  break;
    		  }
    	  }
    	  
    	  if (cur_time - last_p > Ref_period)
    	  {
    		  //System.out.println("buffer is getting accessed in a new reference period");
    		  /* a new, uncorrelated reference */
    		  //correlation_period_of_referenced_page := LAST(p) – HIST(p,1)
    		  //get HIST(p,1)
    		  long hist_p_1;
    		  long[] curr_blk_timestamp = new long[k] ;
    		  for(HistBlock val1 : histblock)
    		  {
    			  if(val1.Blockname.equals(blk))
        		  {
        			  curr_blk_timestamp = val1.block_timestamp;
        			  break;
        		  }
    		  }
    		  //got timestamp array for this buffer from histbuff
    		  //now get hist(p,1)
    		  hist_p_1 = curr_blk_timestamp[k-1];
    		  //got hist(p,1)
    		  
    		  //a new, uncorrelated reference
    		  //correlation_period_of_referenced_page := LAST(p) – HIST(p,1)
    		  
    		  long cor_per_ref_page = last_p - hist_p_1;
    		  
    		  /*for i := 2 to K do
  					HIST(p,i) := HIST(p,i–1) + correlation_period_of_referenced_page
  				od*/
    		  for(HistBlock val1 : histblock)
    		  {
    			  if(val1.Blockname.equals(blk))
        		  {
    				  for(int i=0; i<= (k-2); i++)
    	    		  {
    	    			  val1.block_timestamp[i] = val1.block_timestamp[i+1] +  cor_per_ref_page;
    	    		  }
    				  //HIST (p,1) := t
    				  val1.block_timestamp[k-1] = cur_time;
    				  break;
        		  }
    		  }
    		  //update the last time stamp for this buffer.
    		  for(LastOfBlock val:last_block)
        	  {
        		  if(val.blockname.equals(blk))
        		  {
        			  //LAST(p) := t
        			  val.last_timestamp = cur_time;
        			  break;
        		  }
        	  }
    		  
    	  }
    	  else 
    	  {
    		  /* a correlated reference */
    		  //System.out.println("buffer is getting accessed in the same reference period");
    		  for(LastOfBlock val:last_block)
        	  {
        		  if(val.blockname.equals(blk))
        		  {
        			  val.last_timestamp = cur_time;
        			  break;
        		  }
        	  }
    	  }
    	  return choose_buff;
      }
      //end of if page is already in buffer
      //else select replacement victim
      else if(choose_buff == null)
      {
    	  //checking if an existing buffer is empty for which we can add the block
    	  int empty_buff = -1;
    	  Buffer victim = null;
    	  int old_blk = -1;
    	  
    	  //choosing if any empty buffer is present or not
    	  for (Buffer buff : bufferpool)
          {
    		 if(empty_buff == 1)
    		 {
    			 break;
    		 }
             if (!buff.isPinned())
             {
            	 Block b = buff.block();
                 if (b == null)
                 {
                	empty_buff = 1;
               		victim = buff;
                  	break;	  
          		 }
               			 
              }
          }
    	  
    	  //allocating the new block to the victim which is empty.
    	  if(empty_buff == 1)
    	  {
    		  //System.out.println("Buffer does not contain the new block, and found a new buffer which is empty. Hence choosing this as the victim!!");
    		  //allocate the new block
    		  
    		  for(LastOfBlock val : last_block)
        	  {
    			  if(val != null)
    			  {
    				  if(val.blockname.equals(blk))
            		  {
            			 old_blk = 1;
            			 break;
            		  }
    			  }
    			  else
    				  break;
        		  
        	  }
        	  
        	  if(old_blk == -1)
        	  {
        		  int count = 0;
        		  for(LastOfBlock val : last_block)
        		  {
        			  if(val != null)
        			  {
        				  /*if(val.blockname == null)
            			  {
            				  break;
            			  }*/
        				  count++;
        			  }     
        			  else
        				  break;
        			  
        		  }
        		  histblock[count] = new HistBlock(blk,k);
        	      last_block[count] = new LastOfBlock(blk);
        	   }
    		  
	    	  for(HistBlock v_hist_blk:histblock)
	    	  {
	    		  if(v_hist_blk.Blockname.equals(blk))
	    		  {
	    			  for(int i=0; i<= (k-2); i++)
    	    		  {
    	    			  v_hist_blk.block_timestamp[i] = v_hist_blk.block_timestamp[i+1];
    	    		  }
	    			  v_hist_blk.block_timestamp[k-1] = cur_time;
	    			  break;
	    		  }	    		 
	    	  }
	    	  
	    	  for(LastOfBlock v_last_blk:last_block)
	    	  {
	    		  if(v_last_blk.blockname.equals(blk))
	    		  {
	    			  v_last_blk.last_timestamp = cur_time;
	    			  break;
	    		  }
	    	  }
	    	  //System.out.println("bufferpool is still empty, using victim buff:" + victim + ",block:" + blk);
	    	  System.out.println(cur_time + " " + victim.toString() + " " + "\t\t\t\t" + " " + blk.toString());
	    	  return victim;
    	  }
    	  else if(empty_buff == -1)
    	  {
	    	  //System.out.println("Buffer does not contain the new block, and all the buffers are full. Hence choosing a victim to replace!!");
	    	  long min = cur_time;
	    	  victim = null;
	    	  long last_q = 0;
	    	  long hist_q_ref_counter = 0;
	    	  int ref_counter = 1;
	    	  Block victim_blk = null;
	    	  
	    	  HistBlock[] list_of_victim_blks = new HistBlock[no_of_buffs];
	    	  
	    	  //for all pages q in the buffer do
	    	  //		if t – LAST(q) > Correlated Reference Period /*eligible for replacement*/ and HIST(q,K) < min /* maximum Backward K–distance so
	    	  //choosing the victim to replace
	    	  
	    	  
	    	  //get all the blocks which are in buffer..
	    	  
	    	  int j=0;
	    	  for(Buffer buff:bufferpool)
	    	  {
	    		  
	    		  Block cur_blk = null;
	    		  cur_blk = buff.block();
	    		  for(HistBlock val : histblock)
	    		  {
	    			  if(val.Blockname.equals(cur_blk))
	    			  {
	    				  //list_of_victim_blks[i] = new HistBlock(cur_blk,k);
	    				  list_of_victim_blks[j] = val;
	    				  break;
	    			  }
	    			  
	    		  }
	    		  j++;
	    	 }
	    	  /*
	    	  for(int a=0;a < list_of_victim_blks.length;a++)
	    	  {
	    		  System.out.println("with param:" + list_of_victim_blks[a].Blockname.toString());
	    		  for(int b=0;b < list_of_victim_blks[a].block_timestamp.length;b++)
	    			  System.out.println(list_of_victim_blks[a].block_timestamp[b]);
	    	  }
	    	  */
	    	  for(int y = ref_counter; y <= k; y++)
	    	  {
	    		  //System.out.println("with param ref_counter" + y);
	    		  if(victim_blk != null)
	    		  {
	    			  //System.out.println("victim block chosen:" + victim_blk);
	    			  break;
	    		  }
		    	  for(HistBlock var_victim_blk:list_of_victim_blks)
		    	  {
		    		  //find all block with reference equals to ref_counter
		    		  //check if they are valid blocks or not.
		    		  int no_of_expected_zeros = k - y;
		    		  int got_no_of_zeros = 0;
		    		  for(int z = 0 ; z < k-y ; z++)
		    		  {
		    			  if(var_victim_blk.block_timestamp[z] == 0)
		    			  {
		    				  got_no_of_zeros++;
		    				  //System.out.println("with param, got no. of zeros:" + got_no_of_zeros);
		    			  }
		    		  }
		    		  if(got_no_of_zeros == no_of_expected_zeros)
		    		  {
		    			  //System.out.println("with param, inside this");
		    			  //get the last_p for this victim block from lastofblock
		    			  for(LastOfBlock var_get_last_timestamp : last_block)
		    			  {
		    				  if(var_get_last_timestamp.blockname.equals(var_victim_blk.Blockname))
		    				  {
		    					  last_q = var_get_last_timestamp.last_timestamp;
		    					  break;
		    				  }
		    			  }//got the timestamp from the last_block for this victim
		    			  
		    			  hist_q_ref_counter = var_victim_blk.block_timestamp[k-y];
		    			  
		    			  //System.out.println("cur time" + cur_time + "\n" + "last q:" + last_q + "\n hist_q_ref_counter" + hist_q_ref_counter);
		    			  
		    			  if((cur_time - last_q > Ref_period) && (hist_q_ref_counter < min))
	        			  {
		    				  //System.out.println("with param:inside 2");
	        				  victim_blk = var_victim_blk.Blockname;
	        				  min = hist_q_ref_counter;
	        				  //break;
	        			  }
		    			  
		    		  }
		    	  }
	    	  }
	    	  
	    	  
	    		    	  
	    	  //allocate the new block
	    	  for(LastOfBlock val : last_block)
	    	  {
	    		  if(val != null)
	    		  {
	    			  if(val.blockname.equals(blk))
		    		  {
		    			 old_blk = 1;
		    			 break;
		    		  }
	    		  }
	    		  else
	    			  break;
	    		  
	    	  }
	    	  
	    	  if(old_blk == -1)
	    	  {
	    		  int count = 0;
	    		  for(LastOfBlock val : last_block)
	    		  {
	    			  if(val != null)
	    			  {
	    				  /*if(val.blockname == null)
		    			  {
		    				  break;
		    			  }*/
		    			  count++;
	    			  }
	    			  else
	    				  break;
	    			 
	    		  }
	    		  histblock[count] = new HistBlock(blk,k);
	    	      last_block[count] = new LastOfBlock(blk);
	    	   }
	    	  
	    	  //update the hist buff for the incoming block and set the hist_p_1 to current timestamp
	    	  for(HistBlock v_hist_blk:histblock)
	    	  {
	    		  if(v_hist_blk.Blockname.equals(blk))
	    		  {
	    			  for(int i=0; i<= (k-2); i++)
    	    		  {
    	    			  v_hist_blk.block_timestamp[i] = v_hist_blk.block_timestamp[i+1];
    	    		  }
	    			  v_hist_blk.block_timestamp[k-1] = cur_time;
	    			  break;
	    		  }	    		 
	    	  }
	    	  
	    	  for(LastOfBlock v_last_blk:last_block)
	    	  {
	    		  if(v_last_blk.blockname.equals(blk))
	    		  {
	    			  v_last_blk.last_timestamp = cur_time;
	    			  break;
	    		  }
	    	  }
	    	  
	    	  for(Buffer buff:bufferpool)
	    	  {
	    		  //System.out.println("victim block:" + buff.block().toString() + ":" + buff.toString());
	    		  if(buff.block().equals(victim_blk))
	    		  {
	    			  victim = buff;
	    			  break;
	    		  }
	    	  }
	    	  System.out.println(cur_time + " " + victim.toString() + " " + victim_blk.toString() + " " + blk.toString());
	    	  return victim;
    	  }//end of else empty_buff is -1
      }//end of else choose_buff is null
     return null;
   }
}
