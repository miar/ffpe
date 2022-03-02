import java.util.Random;
import java.util.Map;
import java.util.Collections;
import java.util.HashMap;
import java.util.Hashtable;
import com.ffp.*;
import com.romix.scala.collection.concurrent.TrieMap;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.ArrayList;
import java.io.Reader;
import java.io.Writer;
import java.io.FileWriter;
import java.io.BufferedWriter;
import java.io.IOException;
import java.util.concurrent.ThreadLocalRandom;

public class MultiThreadInsertLookupRemoveSpeedup {

    private final static double [] DATASET_INSERT_RATIOS =       {
	1.00, // All - Ins
	0.00, // All - Look
	0.00, // All - Look
	0.00, // All - Rem
	0.80, // Mix - Ins = 0.80  Look (to be found) = 0.05 Look (not to be found) = 0.05  Rem = 0.10
	0.60, // Mix - Ins = 0.60  Look (to be found) = 0.15 Look (not to be found) = 0.15  Rem = 0.10
	0.40, // Mix - Ins = 0.40  Look (to be found) = 0.25 Look (not to be found) = 0.25  Rem = 0.10
	0.20, // Mix - Ins = 0.20  Look (to be found) = 0.35 Look (not to be found) = 0.35  Rem = 0.10
	
	0.50, // Mix - Ins = 0.50  Look (to be found) = 0.25 Look (not to be found) = 0.25  Rem = 0.00
	0.00, // Mix - Ins = 0.00  Look (to be found) = 0.25 Look (not to be found) = 0.25  Rem = 0.50
	0.50, // Mix - Ins = 0.50  Look (to be found) = 0.00 Look (not to be found) = 0.00  Rem = 0.50
	0.25  // Mix - Ins = 0.25  Look (to be found) = 0.25 Look (not to be found) = 0.25  Rem = 0.25
    };
    
    private final static double [] DATASET_LOOKUP_FOUND_RATIOS = {
	0.00, // All - Ins 
	1.00, // All - Look
	0.50, // All - Look
	0.00, // All - Rem
	0.05, // Mix - Ins = 0.80  Look (to be found) = 0.05 Look (not to be found) = 0.05  Rem = 0.10
	0.15, // Mix - Ins = 0.60  Look (to be found) = 0.15 Look (not to be found) = 0.15  Rem = 0.10
	0.25, // Mix - Ins = 0.40  Look (to be found) = 0.25 Look (not to be found) = 0.25  Rem = 0.10
	0.35, // Mix - Ins = 0.20  Look (to be found) = 0.35 Look (not to be found) = 0.35  Rem = 0.10
	
	0.25, // Mix - Ins = 0.50  Look (to be found) = 0.25 Look (not to be found) = 0.25  Rem = 0.00
	0.25, // Mix - Ins = 0.00  Look (to be found) = 0.25 Look (not to be found) = 0.25  Rem = 0.50
	0.00, // Mix - Ins = 0.50  Look (to be found) = 0.00 Look (not to be found) = 0.00  Rem = 0.50
	0.25  // Mix - Ins = 0.25  Look (to be found) = 0.25 Look (not to be found) = 0.25  Rem = 0.25
    };
        
    private final static double [] DATASET_LOOKUP_NFOUND_RATIOS = {
	0.00, // All - Ins 
	0.00, // All - Look
	0.50, // All - Look
	0.00, // All - Rem
	0.05, // Mix - Ins = 0.80  Look (to be found) = 0.05 Look (not to be found) = 0.05  Rem = 0.10
	0.15, // Mix - Ins = 0.60  Look (to be found) = 0.15 Look (not to be found) = 0.15  Rem = 0.10
	0.25, // Mix - Ins = 0.40  Look (to be found) = 0.25 Look (not to be found) = 0.25  Rem = 0.10
	0.35, // Mix - Ins = 0.20  Look (to be found) = 0.35 Look (not to be found) = 0.35  Rem = 0.10
	
	0.25, // Mix - Ins = 0.50  Look (to be found) = 0.25 Look (not to be found) = 0.25  Rem = 0.00
	0.25, // Mix - Ins = 0.00  Look (to be found) = 0.25 Look (not to be found) = 0.25  Rem = 0.50
	0.00, // Mix - Ins = 0.50  Look (to be found) = 0.00 Look (not to be found) = 0.00  Rem = 0.50
	0.25  // Mix - Ins = 0.25  Look (to be found) = 0.25 Look (not to be found) = 0.25  Rem = 0.25
    };


    private static final int ARCH = 64; // 64 bit machine    
    private final static int [] THREADS = {1,4,8,16,24,32};
    //private final static int [] THREADS = {1};
    private final static int RUNS = 1; //10
    private final static int WARMUP_RUNS = 1; //5;    
    private  static int DATASET_SIZE = 10; //16777216; // 8^8
    private  static int DATASET_SIZE_BENCH = 3; //3000000;
    private static long DATASET[] = new long[DATASET_SIZE];
    private final static int TOTAL_RUNS = RUNS + WARMUP_RUNS;     
    Thread threads[] = new Thread[32];	
        
    // insert 8^8 + time + search 8^8 + time
    private void test_1(Map maps[]) throws InterruptedException {
	System.out.println("## insert 8^8 + time + search 8^8 + time ##");
	for (Map map : maps) {
	    if (map instanceof ConcurrentHashMap) 
		System.out.println("################ CHM  ##############");
	    else if (map instanceof ConcurrentSkipListMap) 
		System.out.println("################ CSL  ##############");
	    else if (map instanceof TrieMap) 
		System.out.println("################ CT  ##############");
	    else if (map instanceof FFPS) 
		System.out.println("################ FFPS ##############");
	    else
		System.out.println("################ FFPE ##############");

	    for (final int T : THREADS) {
		long averageTime = 0;
		final int thread_dataset_offset = DATASET_SIZE / T;
		for (int r = 1; r <= TOTAL_RUNS; r++) {
		    
		    if (map instanceof ConcurrentHashMap)
			map = new ConcurrentHashMap<Long, Long>();
		    else if (map instanceof ConcurrentSkipListMap) 
			map = new ConcurrentSkipListMap<Long, Long>();
		    else if (map instanceof TrieMap) 
			map = new TrieMap<Long, Long>();
		    else if (map instanceof FFPS) 
			map = new FFPS<Long, Long>();
		    else 
			map = new FFPE<Long, Long>();
 
		    final Map absmap = map;
		    
		    int put_threads = 1;
		    
		    // put numbers
		    for (int ti = 0 ; ti < put_threads; ti++) {
			int local_thread_dataset_offset = DATASET_SIZE / put_threads;
			int thread_begin_i = ti * local_thread_dataset_offset;
			int thread_end_i = thread_begin_i + local_thread_dataset_offset;
			threads[ti] = new Thread(new Runnable() {
				@Override
				public void run() {
				    for (long i = thread_begin_i; i < thread_end_i; i++) {
					absmap.put(i, i);
				    }
				}
			    });
		    }
		    
		    for (int ti = 0; ti < put_threads; ti++)
			threads[ti].start();
		    
		    for (int ti = 0; ti < put_threads; ti++)
			threads[ti].join();
		    
		    // get numbers		
		    for (int ti = 0 ; ti < T; ti++) {
			int thread_begin_i = ti * thread_dataset_offset;
			int thread_end_i = thread_begin_i + thread_dataset_offset;
			
			threads[ti] = new Thread(new Runnable() {
				@Override
				public void run() {
				    for (int i = thread_begin_i; i < thread_end_i; i++) {
					int op = (int) getOperation(DATASET[i]);
					Long v = getNumber(DATASET[i]);
					
					switch(op) {
					case 0:
					    // op remove
					    absmap.remove(v);
					    break;
					case 1:
					    // op insert
					    absmap.put(v, v);
					    break;
					default: 
					    // code lookup found / not found
					    absmap.get(v);
					}
				    }
				}
			    });
		    }
		    long time = System.nanoTime();
		    
		    for (int ti = 0; ti < T; ti++)
			threads[ti].start();
		    
		    for (int ti = 0; ti < T; ti++)
			threads[ti].join();
		    
		    long timeUsed = (System.nanoTime() - time) / 1000000L;
		    
		    if (r > WARMUP_RUNS) {
			averageTime += timeUsed;
		    }
		}
		// show time
		System.out.println("Threads = " + T + 
				   " Time = " + averageTime / RUNS + " MSeconds");
	    }	    
	}	    
	return;
    }
	
    // insert 8^8 + remove 8^8 + time + search 8^8 + time      
    private void test_2(Map maps[]) throws InterruptedException {    	
	System.out.println("## insert 8^8 + remove 8^8 + time + search 8^8 + time ##");	
	for (Map map : maps) {
	    if (map instanceof ConcurrentHashMap) 
		System.out.println("################ CHM  ##############");
	    else if (map instanceof ConcurrentSkipListMap) 
		System.out.println("################ CSL  ##############");
	    else if (map instanceof TrieMap) 
		System.out.println("################ CT  ##############");
	    else if (map instanceof FFPS) 
		System.out.println("################ FFPS ##############");
	    else
		System.out.println("################ FFPE ##############");

	    for (final int T : THREADS) {
		long averageTime = 0;
		final int thread_dataset_offset = DATASET_SIZE / T;
		for (int r = 1; r <= TOTAL_RUNS; r++) {		    
		    if (map instanceof ConcurrentHashMap)
			map = new ConcurrentHashMap<Long, Long>();
		    else if (map instanceof ConcurrentSkipListMap) 
			map = new ConcurrentSkipListMap<Long, Long>();
		    else if (map instanceof TrieMap) 
			map = new TrieMap<Long, Long>();
		    else if (map instanceof FFPS) 
			map = new FFPS<Long, Long>();
		    else 
			map = new FFPE<Long, Long>();
		    
		    final Map absmap = map;
		    
		    int put_threads = 1;
		    int remove_threads = 1;
		    // put numbers
		    for (int ti = 0 ; ti < put_threads; ti++) {
			int local_thread_dataset_offset = DATASET_SIZE / put_threads;
			int thread_begin_i = ti * local_thread_dataset_offset;
			int thread_end_i = thread_begin_i + local_thread_dataset_offset;
			threads[ti] = new Thread(new Runnable() {
				@Override
				public void run() {
				    for (long i = thread_begin_i; i < thread_end_i; i++) {
					absmap.put(i, i);
				    }
				}
			    });
		    }
		    
		    for (int ti = 0; ti < put_threads; ti++)
			threads[ti].start();
		    
		    for (int ti = 0; ti < put_threads; ti++)
			threads[ti].join();
		    
		    // remove numbers		
		    for (int ti = 0 ; ti < remove_threads; ti++) {
			int local_thread_dataset_offset = DATASET_SIZE / remove_threads;
			int thread_begin_i = ti * local_thread_dataset_offset;
			int thread_end_i = thread_begin_i + local_thread_dataset_offset;
			threads[ti] = new Thread(new Runnable() {
				@Override
				public void run() {
				    for (long i = thread_begin_i; i < thread_end_i; i++) {
					absmap.remove(i);
				    }
				}
			    });
		    }
		    
		    for (int ti = 0; ti < remove_threads; ti++)
			threads[ti].start();
		    
		    for (int ti = 0; ti < remove_threads; ti++)
			threads[ti].join();
		    
		    // prepare the threads for the execution 
		    for (int t = 0; t < T; t++) {
			int thread_begin_i = t * thread_dataset_offset;
			int thread_end_i = thread_begin_i + thread_dataset_offset;

			threads[t] = new Thread(new Runnable() {
				@Override
				public void run() {
				    for (int i = thread_begin_i; i < thread_end_i; i++) {
					int op = (int) getOperation(DATASET[i]);
					Long v = getNumber(DATASET[i]);			
					
					switch(op) {
					case 0:
					    // op remove
					    absmap.remove(v);
					    break;
					case 1:
					    // op insert
					    absmap.put(v, v);
					    break;
					default: 
					    // code lookup found / not found
					    absmap.get(v);
					}
				    }				
				}
			    });
		    }
		    // run threads
		    long time = System.nanoTime();
		    
		    for (int t = 0; t < T; t++)
			threads[t].start();
		    
		    for (int t = 0; t < T; t++)
			threads[t].join();
		    
		    long timeUsed = (System.nanoTime() - time) / 1000000L;
		    if (r > WARMUP_RUNS) {
			averageTime += timeUsed;
		    }
		}
		// show time
		System.out.println("Threads = " + T + 
				   " Time = " +  averageTime / RUNS + " MSeconds");
	    }	
	}
	return;
    }
	
	
    // insert 8^8 + time + remove 8^8 + time      
    private void test_3(Map maps[]) throws InterruptedException {
	System.out.println("## insert 8^8 + time + remove 8^8 + time ##");
	for (Map map : maps) {
	    if (map instanceof ConcurrentHashMap) 
		System.out.println("################ CHM  ##############");
	    else if (map instanceof ConcurrentSkipListMap) 
		System.out.println("################ CSL  ##############");
	    else if (map instanceof TrieMap) 
		System.out.println("################ CT  ##############");
	    else if (map instanceof FFPS) 
		System.out.println("################ FFPS ##############");
	    else
		System.out.println("################ FFPE ##############");


	    for (final int T : THREADS) {
		long averageTime = 0;
		final int thread_dataset_offset = DATASET_SIZE / T;
		for (int r = 1; r <= TOTAL_RUNS; r++) {

		    if (map instanceof ConcurrentHashMap)
			map = new ConcurrentHashMap<Long, Long>();
		    else if (map instanceof ConcurrentSkipListMap) 
			map = new ConcurrentSkipListMap<Long, Long>();
		    else if (map instanceof TrieMap) 
			map = new TrieMap<Long, Long>();
		    else if (map instanceof FFPS) 
			map = new FFPS<Long, Long>();
		    else 
			map = new FFPE<Long, Long>();		    
		    
		    final Map absmap = map;
		    
		    int put_threads = 1;
		    int remove_threads = 1;
		    
		    // put numbers
		    for (int ti = 0 ; ti < put_threads; ti++) {
			int local_thread_dataset_offset = DATASET_SIZE / put_threads;
			int thread_begin_i = ti * local_thread_dataset_offset;
			int thread_end_i = thread_begin_i + local_thread_dataset_offset;
			threads[ti] = new Thread(new Runnable() {
				@Override
				public void run() {
				    for (long i = thread_begin_i; i < thread_end_i; i++) {
					absmap.put(i, i);
				    }
				}
			    });
		    }
		    
		    for (int ti = 0; ti < put_threads; ti++)
			threads[ti].start();
		    
		    for (int ti = 0; ti < put_threads; ti++)
			threads[ti].join();
		    
		    // remove numbers		
		    for (int ti = 0 ; ti < T; ti++) {
			int thread_begin_i = ti * thread_dataset_offset;
			int thread_end_i = thread_begin_i + thread_dataset_offset;
			threads[ti] = new Thread(new Runnable() {
				@Override
				public void run() {
				    for (int i = thread_begin_i; i < thread_end_i; i++) {
					int op = (int) getOperation(DATASET[i]);
					Long v = getNumber(DATASET[i]);
					
					switch(op) {
					case 0:
					    // op remove
					    absmap.remove(v);
					    break;
					case 1:
					    // op insert
					    absmap.put(v, v);
					    break;
					default: 
					    // code lookup found / not found
					    absmap.get(v);
					}
					
				    }
				}
			    });
		    }
		    
		    long time = System.nanoTime();
		    
		    for (int ti = 0; ti < T; ti++)
			threads[ti].start();
		    
		    for (int ti = 0; ti < T; ti++)
			threads[ti].join();
		    
		    long timeUsed = (System.nanoTime() - time) / 1000000L;
		    
		    if (r > WARMUP_RUNS) {
			averageTime += timeUsed;
		    }
		}
		// show time
		System.out.println("Threads = " + T + 
				   " Time = " + averageTime / RUNS + " MSeconds");	
	    }
	}
	return;
    }
	
    // insert 8^8 + remove (8^8 - 8^7) + time + search 8^8 + time
    private void test_4(Map maps[]) throws InterruptedException {    
	System.out.println("## insert 8^8 + remove (8^8 - 8^7) + time + search 8^8 + time ##");
	for (Map map : maps) {
	    if (map instanceof ConcurrentHashMap) 
		System.out.println("################ CHM  ##############");
	    else if (map instanceof ConcurrentSkipListMap) 
		System.out.println("################ CSL  ##############");
	    else if (map instanceof TrieMap) 
		System.out.println("################ CT  ##############");
	    else if (map instanceof FFPS) 
		System.out.println("################ FFPS ##############");
	    else
		System.out.println("################ FFPE ##############");

	    for (final int T : THREADS) {
		long averageTime = 0;
		long averageTimeRemove = 0;
		long averageMemory = 0;
		final int thread_dataset_offset = DATASET_SIZE / T;
		for (int r = 1; r <= TOTAL_RUNS; r++) {
		    if (map instanceof ConcurrentHashMap)
			map = new ConcurrentHashMap<Long, Long>();
		    else if (map instanceof ConcurrentSkipListMap) 
			map = new ConcurrentSkipListMap<Long, Long>();
		    else if (map instanceof TrieMap) 
			map = new TrieMap<Long, Long>();
		    else if (map instanceof FFPS) 
			map = new FFPS<Long, Long>();
		    else 
			map = new FFPE<Long, Long>();		    
		    
		    final Map absmap = map;
		    
		    int put_threads = 1;
		    
		    // put numbers
		    for (int ti = 0 ; ti < put_threads; ti++) {
			int local_thread_dataset_offset = DATASET_SIZE / put_threads;
			int thread_begin_i = ti * local_thread_dataset_offset;
			int thread_end_i = thread_begin_i + local_thread_dataset_offset;
			threads[ti] = new Thread(new Runnable() {
				@Override
				public void run() {
				    for (long i = thread_begin_i; i < thread_end_i; i++) {
					absmap.put(i, i);
				    }
				}
			    });
		    }
		    
		    for (int ti = 0; ti < put_threads; ti++)
			threads[ti].start();
		    
		    for (int ti = 0; ti < put_threads; ti++)
			threads[ti].join();
		    
		    // remove numbers - sequential for now
		    n = 0;
		    remove_keys_by_depth(absmap, 8, 8, DATASET_SIZE);
		    
		    // prepare the threads for the execution 
		    
		    for (int t = 0; t < T; t++) {
			int thread_begin_i = t * thread_dataset_offset;
			int thread_end_i = thread_begin_i + thread_dataset_offset;
			
			threads[t] = new Thread(new Runnable() {
				@Override
				public void run() {
				    for (int i = thread_begin_i; i < thread_end_i; i++) {
					int op = (int) getOperation(DATASET[i]);
					Long v = getNumber(DATASET[i]);
					
					switch(op) {
					case 0:
					    // op remove
					    absmap.remove(v);
					    break;
					case 1:
					    // op insert
					    absmap.put(v, v);
					    break;
					default: 
					    // code lookup found / not found
					    absmap.get(v);
					}
				    }				
				}
			    });
		    }
		    
		    // run threads
		    long time = System.nanoTime();
		    
		    for (int t = 0; t < T; t++)
			threads[t].start();
		    
		    for (int t = 0; t < T; t++)
			threads[t].join();
		    
		    long timeUsed = (System.nanoTime() - time) / 1000000L;
		    if (r > WARMUP_RUNS) {
			averageTime += timeUsed;
		    }
		}
		// show time
		System.out.println("Threads = " + T + 
				   " Time = " +  averageTime / RUNS + " MSeconds");
	    }
	}
	return;
    }		
	
    // insert 8^8 + remove (8^8 - 8^7) + time + insert 8^8 + time
    private void test_5(Map maps[]) throws InterruptedException {
	System.out.println("## insert 8^8 + remove (8^8 - 8^7) + time + insert 8^8 + time ##");
	for (Map map : maps) {
	    if (map instanceof ConcurrentHashMap) 
		System.out.println("################ CHM  ##############");
	    else if (map instanceof ConcurrentSkipListMap) 
		System.out.println("################ CSL  ##############");
	    else if (map instanceof TrieMap) 
		System.out.println("################ CT  ##############");
	    else if (map instanceof FFPS) 
		System.out.println("################ FFPS ##############");
	    else
		System.out.println("################ FFPE ##############");

	    for (final int T : THREADS) {
		long averageTime = 0;
		long averageTimeRemove = 0;
		long averageMemory = 0;
		final int thread_dataset_offset = DATASET_SIZE / T;
		for (int r = 1; r <= TOTAL_RUNS; r++) {
		    if (map instanceof ConcurrentHashMap)
			map = new ConcurrentHashMap<Long, Long>();
		    else if (map instanceof ConcurrentSkipListMap) 
			map = new ConcurrentSkipListMap<Long, Long>();
		    else if (map instanceof TrieMap) 
			map = new TrieMap<Long, Long>();
		    else if (map instanceof FFPS) 
			map = new FFPS<Long, Long>();
		    else 
			map = new FFPE<Long, Long>();

		    
		    final Map absmap = map;
		    
		    int put_threads = 1;
		    
		    // put numbers
		    for (int ti = 0 ; ti < put_threads; ti++) {
			int local_thread_dataset_offset = DATASET_SIZE / put_threads;
			int thread_begin_i = ti * local_thread_dataset_offset;
			int thread_end_i = thread_begin_i + local_thread_dataset_offset;
			threads[ti] = new Thread(new Runnable() {
				@Override
				public void run() {
				    for (long i = thread_begin_i; i < thread_end_i; i++) {
					absmap.put(i, i);
				    }
				}
			    });
		    }
		    
		    for (int ti = 0; ti < put_threads; ti++)
			threads[ti].start();
		    
		    for (int ti = 0; ti < put_threads; ti++)
			threads[ti].join();
		    
		    // remove numbers - sequential for now
		    n = 0;
		    remove_keys_by_depth(absmap, 8, 8, DATASET_SIZE);
		    
		    // prepare the threads for the execution 
		    
		    for (int t = 0; t < T; t++) {
			int thread_begin_i = t * thread_dataset_offset;
			int thread_end_i = thread_begin_i + thread_dataset_offset;
			
			threads[t] = new Thread(new Runnable() {
				@Override
				public void run() {
				    for (int i = thread_begin_i; i < thread_end_i; i++) {
					int op = (int) getOperation(DATASET[i]);
					Long v = getNumber(DATASET[i]);
					
					switch(op) {
					case 0:
					    // op remove
					    absmap.remove(v);
					    break;
					case 1:
					    // op insert
					    absmap.put(v, v);
					    break;
					default: 
					    // code lookup found / not found
					    absmap.get(v);
					}
					
				    }				
				}
			    });
		    }
		    
		    // run threads
		    long time = System.nanoTime();
		    
		    for (int t = 0; t < T; t++)
			threads[t].start();
		    
		    for (int t = 0; t < T; t++)
			threads[t].join();
		    
		    long timeUsed = (System.nanoTime() - time) / 1000000L;
		    if (r > WARMUP_RUNS) {
			averageTime += timeUsed;
		    }
		}
		// show time
		System.out.println("Threads = " + T + 
				   " Time = " +  averageTime / RUNS + " MSeconds");
	    }
	}
	return;
    }		
	
    // insert 8^8 + remove 8^8 + time + insert 8^8 + time      
    private void test_6(Map maps[]) throws InterruptedException {    			
	System.out.println("## insert 8^8 + remove 8^8 + time + insert 8^8 + time ##");
	for (Map map : maps) {
	    if (map instanceof ConcurrentHashMap) 
		System.out.println("################ CHM  ##############");
	    else if (map instanceof ConcurrentSkipListMap) 
		System.out.println("################ CSL  ##############");
	    else if (map instanceof TrieMap) 
		System.out.println("################ CT  ##############");
	    else if (map instanceof FFPS) 
		System.out.println("################ FFPS ##############");
	    else
		System.out.println("################ FFPE ##############");

	    for (final int T : THREADS) {
		long averageTime = 0;
		final int thread_dataset_offset = DATASET_SIZE / T;
		for (int r = 1; r <= TOTAL_RUNS; r++) {
		    
		    if (map instanceof ConcurrentHashMap)
			map = new ConcurrentHashMap<Long, Long>();
		    else if (map instanceof ConcurrentSkipListMap) 
			map = new ConcurrentSkipListMap<Long, Long>();
		    else if (map instanceof TrieMap) 
			map = new TrieMap<Long, Long>();
		    else if (map instanceof FFPS) 
			map = new FFPS<Long, Long>();
		    else 
			map = new FFPE<Long, Long>();
		    
		    final Map absmap = map;
		    
		    int put_threads = 1;
		    int remove_threads = 1;
		    // put numbers
		    for (int ti = 0 ; ti < put_threads; ti++) {
			int local_thread_dataset_offset = DATASET_SIZE / put_threads;
			int thread_begin_i = ti * local_thread_dataset_offset;
			int thread_end_i = thread_begin_i + local_thread_dataset_offset;
			threads[ti] = new Thread(new Runnable() {
				@Override
				public void run() {
				    for (long i = thread_begin_i; i < thread_end_i; i++) {
					absmap.put(i, i);
				    }
				}
			    });
		    }
		    
		    for (int ti = 0; ti < put_threads; ti++)
			threads[ti].start();
		    
		    for (int ti = 0; ti < put_threads; ti++)
			threads[ti].join();
		    
		    // remove numbers		
		    for (int ti = 0 ; ti < remove_threads; ti++) {
			int local_thread_dataset_offset = DATASET_SIZE / remove_threads;
			int thread_begin_i = ti * local_thread_dataset_offset;
			int thread_end_i = thread_begin_i + local_thread_dataset_offset;
			threads[ti] = new Thread(new Runnable() {
				@Override
				public void run() {
				    for (long i = thread_begin_i; i < thread_end_i; i++) {
					absmap.remove(i);
				    }
				}
			    });
		    }
		    
		    for (int ti = 0; ti < remove_threads; ti++)
			threads[ti].start();
		    
		    for (int ti = 0; ti < remove_threads; ti++)
			threads[ti].join();
		    
		    // prepare the threads for the execution 
		    
		    for (int t = 0; t < T; t++) {
			int thread_begin_i = t * thread_dataset_offset;
			int thread_end_i = thread_begin_i + thread_dataset_offset;
			
			threads[t] = new Thread(new Runnable() {
				@Override
				public void run() {
				    for (int i = thread_begin_i; i < thread_end_i; i++) {
					int op = (int) getOperation(DATASET[i]);
					Long v = getNumber(DATASET[i]);
					
					switch(op) {
					case 0:
					    // op remove
					    absmap.remove(v);
					    break;
					case 1:
					    // op insert
					    absmap.put(v, v);
					    break;
					default: 
					    // code lookup found / not found
					    absmap.get(v);
					}
				    }				
				}
			    });
		    }
		    
		    // run threads
		    long time = System.nanoTime();
		    
		    for (int t = 0; t < T; t++)
			threads[t].start();
		    
		    for (int t = 0; t < T; t++)
			threads[t].join();
		    
		    long timeUsed = (System.nanoTime() - time) / 1000000L;
		    if (r > WARMUP_RUNS) {
			averageTime += timeUsed;
		    }
		}
		// show time
		System.out.println("Threads = " + T + 
				   " Time = " +  averageTime / RUNS + " MSeconds");
	    }	
	}
	return;
    }


    // (Used in Computing) insert 8^8 + remove (8^8 - 8^7) + insert removes/gets 8^8 + time + random 8^8 + time
    private void test_random(Map maps[]) throws InterruptedException {    
	System.out.println("## insert 8^8 + remove (8^8 - 8^7) + insert removes/gets 8^8 + time + random 8^8 + time  ##");
	for (Map map : maps) {
	    if (map instanceof ConcurrentHashMap) 
		System.out.println("################ CHM  ##############");
	    else if (map instanceof ConcurrentSkipListMap) 
		System.out.println("################ CSL  ##############");
	    else if (map instanceof TrieMap) 
		System.out.println("################ CT  ##############");
	    else if (map instanceof FFPS) 
		System.out.println("################ FFPS ##############");
	    else
		System.out.println("################ FFPE ##############");
	    
	    
	    for (final int T : THREADS) {
		long averageTime = 0;
		long averageTimeRemove = 0;
		long averageMemory = 0;
		final int thread_dataset_offset = DATASET_SIZE / T;
		for (int r = 1; r <= TOTAL_RUNS; r++) {
		    
		    if (map instanceof ConcurrentHashMap)
			map = new ConcurrentHashMap<Long, Long>();
		    else if (map instanceof ConcurrentSkipListMap) 
			map = new ConcurrentSkipListMap<Long, Long>();
		    else if (map instanceof TrieMap) 
			map = new TrieMap<Long, Long>();
		    else if (map instanceof FFPS) 
			map = new FFPS<Long, Long>();
		    else 
			map = new FFPE<Long, Long>();

		    final Map absmap = map;    
		    int put_threads = 1;
		    
		    // put sequential numbers
		    for (int ti = 0 ; ti < put_threads; ti++) {
			int local_thread_dataset_offset = DATASET_SIZE / put_threads;
			int thread_begin_i = ti * local_thread_dataset_offset;
			int thread_end_i = thread_begin_i + local_thread_dataset_offset;
			threads[ti] = new Thread(new Runnable() {
				@Override
				public void run() {
				    for (long i = thread_begin_i; i < thread_end_i; i++) {
					absmap.put(i, i);
				    }
				}
			    });
		    }
		    
		    for (int ti = 0; ti < put_threads; ti++)
			threads[ti].start();
		    
		    for (int ti = 0; ti < put_threads; ti++)
			threads[ti].join();
		    
		    // remove numbers - sequential for now
		    n = 0;
		    remove_keys_by_depth(absmap, 8, 8, DATASET_SIZE);

		    // put random numbers	    
		    
		    for (int ti = 0 ; ti < put_threads; ti++) {
			int local_thread_dataset_offset = DATASET_SIZE / put_threads;
			int thread_begin_i = ti * local_thread_dataset_offset;
			int thread_end_i = thread_begin_i + local_thread_dataset_offset;
			threads[ti] = new Thread(new Runnable() {
				@Override
				public void run() {
				    for (int i = thread_begin_i; i < thread_end_i; i++) {
					int op = getOperation(DATASET[i]);
					Long v = getNumber(DATASET[i]);
					if (op == 0 || op == 2)
					    absmap.put(v, v);
				    }
				}
			    });
		    }
		    
		    for (int ti = 0; ti < put_threads; ti++)
			threads[ti].start();
		    
		    for (int ti = 0; ti < put_threads; ti++)
			threads[ti].join();
			    
		    // prepare the threads for the execution 
		    
		    for (int t = 0; t < T; t++) {
			int thread_begin_i = t * thread_dataset_offset;
			int thread_end_i = thread_begin_i + thread_dataset_offset;
			
			threads[t] = new Thread(new Runnable() {
				@Override
				public void run() {
				    for (int i = thread_begin_i; i < thread_end_i; i++) {
					int op = (int) getOperation(DATASET[i]);
					Long v = getNumber(DATASET[i]);
					
					switch(op) {
					case 0:
					    // op remove
					    absmap.remove(v);
					    break;
					case 1:
					    // op insert
					    absmap.put(v, v);
					    break;
					default: 
					    // code lookup found / not found
					    absmap.get(v);
					}
				    }				
				}
			    });
		    }
		    
		    // run threads
		    long time = System.nanoTime();
		    
		    for (int t = 0; t < T; t++)
			threads[t].start();
		    
		    for (int t = 0; t < T; t++)
			threads[t].join();
		    
		    long timeUsed = (System.nanoTime() - time) / 1000000L;
		    if (r > WARMUP_RUNS) {
			averageTime += timeUsed;
		    }
		}
		// show time
		System.out.println("Threads = " + T + 
				   " Time = " +  averageTime / RUNS + " MSeconds");
	    }
	}
	return;
    }		

    // insert 8^8 + remove (8^8 - 8^7) + insert removes/gets 8^8 + time + random DATASET_SIZE_BENCH + time
    private void test_random1(Map maps[]) throws InterruptedException { 
	System.out.println("## insert 8^8 + remove (8^8 - 8^7) + insert removes/gets 8^8 + time + random DATASET_SIZE_BENCH + time ##");
 	for (Map map : maps) {
	    if (map instanceof ConcurrentHashMap) 
		System.out.println("################ CHM  ##############");
	    else if (map instanceof ConcurrentSkipListMap) 
		System.out.println("################ CSL  ##############");
	    else if (map instanceof TrieMap) 
		System.out.println("################ CT  ##############");
	    else if (map instanceof FFPS) 
		System.out.println("################ FFPS ##############");
	    else
		System.out.println("################ FFPE ##############");
	    
	    
	    for (final int T : THREADS) {
		long averageTime = 0;
		long averageTimeRemove = 0;
		long averageMemory = 0;

		for (int r = 1; r <= TOTAL_RUNS; r++) {
		    
		    if (map instanceof ConcurrentHashMap)
			map = new ConcurrentHashMap<Long, Long>();
		    else if (map instanceof ConcurrentSkipListMap) 
			map = new ConcurrentSkipListMap<Long, Long>();
		    else if (map instanceof TrieMap) 
			map = new TrieMap<Long, Long>();
		    else if (map instanceof FFPS) 
			map = new FFPS<Long, Long>();
		    else 
			map = new FFPE<Long, Long>();

		    final Map absmap = map;    
		    int put_threads = 1;
		    
		    // put sequential numbers
		    for (int ti = 0 ; ti < put_threads; ti++) {
			int local_thread_dataset_offset = DATASET_SIZE / put_threads;
			int thread_begin_i = ti * local_thread_dataset_offset;
			int thread_end_i = thread_begin_i + local_thread_dataset_offset;
			threads[ti] = new Thread(new Runnable() {
				@Override
				public void run() {
				    for (long i = thread_begin_i; i < thread_end_i; i++) {
					absmap.put(i, i);
				    }
				}
			    });
		    }
		    
		    for (int ti = 0; ti < put_threads; ti++)
			threads[ti].start();
		    
		    for (int ti = 0; ti < put_threads; ti++)
			threads[ti].join();
		    
		    // remove numbers - sequential for now
		    n = 0;
		    remove_keys_by_depth(absmap, 8, 8, DATASET_SIZE);

		    DATASET_SIZE = DATASET_SIZE_BENCH;
		    // put random numbers	    

		    for (int ti = 0 ; ti < put_threads; ti++) {
			int local_thread_dataset_offset = DATASET_SIZE / put_threads;
			int thread_begin_i = ti * local_thread_dataset_offset;
			int thread_end_i = thread_begin_i + local_thread_dataset_offset;
			threads[ti] = new Thread(new Runnable() {
				@Override
				public void run() {
				    for (int i = thread_begin_i; i < thread_end_i; i++) {
					int op = getOperation(DATASET[i]);
					Long v = getNumber(DATASET[i]);
					if (op == 0 || op == 2)
					    absmap.put(v, v);
				    }
				}
			    });
		    }
		    
		    for (int ti = 0; ti < put_threads; ti++)
			threads[ti].start();
		    
		    for (int ti = 0; ti < put_threads; ti++)
			threads[ti].join();
			    
		    // prepare the threads for the execution 
		    final int thread_dataset_offset = DATASET_SIZE / T;
		    
		    for (int t = 0; t < T; t++) {
			int thread_begin_i = t * thread_dataset_offset;
			int thread_end_i = thread_begin_i + thread_dataset_offset;
			
			threads[t] = new Thread(new Runnable() {
				@Override
				public void run() {
				    for (int i = thread_begin_i; i < thread_end_i; i++) {
					int op = (int) getOperation(DATASET[i]);
					Long v = getNumber(DATASET[i]);
					
					switch(op) {
					case 0:
					    // op remove
					    absmap.remove(v);
					    break;
					case 1:
					    // op insert
					    absmap.put(v, v);
					    break;
					default: 
					    // code lookup found / not found
					    absmap.get(v);
					}
				    }				
				}
			    });
		    }
		    
		    // run threads
		    long time = System.nanoTime();
		    
		    for (int t = 0; t < T; t++)
			threads[t].start();
		    
		    for (int t = 0; t < T; t++)
			threads[t].join();
		    
		    long timeUsed = (System.nanoTime() - time) / 1000000L;
		    if (r > WARMUP_RUNS) {
			averageTime += timeUsed;
		    }
		}
		// show time
		System.out.println("Threads = " + T + 
				   " Time = " +  averageTime / RUNS + " MSeconds");
	    }
	}
	return;
    }		

    // time + random DATASET_SIZE_BENCH + time
    private void test_random2(Map maps[]) throws InterruptedException {    
	System.out.println("## time + random DATASET_SIZE_BENCH + time ##");
	DATASET_SIZE = DATASET_SIZE_BENCH;
	for (Map map : maps) {
	    if (map instanceof ConcurrentHashMap) 
		System.out.println("################ CHM  ##############");
	    else if (map instanceof ConcurrentSkipListMap) 
		System.out.println("################ CSL  ##############");
	    else if (map instanceof TrieMap) 
		System.out.println("################ CT  ##############");
	    else if (map instanceof FFPS) 
		System.out.println("################ FFPS ##############");
	    else
		System.out.println("################ FFPE ##############");
	    
	    
	    for (final int T : THREADS) {
		long averageTime = 0;
		long averageTimeRemove = 0;
		long averageMemory = 0;
		final int thread_dataset_offset = DATASET_SIZE / T;
		for (int r = 1; r <= TOTAL_RUNS; r++) {
		    
		    if (map instanceof ConcurrentHashMap)
			map = new ConcurrentHashMap<Long, Long>();
		    else if (map instanceof ConcurrentSkipListMap) 
			map = new ConcurrentSkipListMap<Long, Long>();
		    else if (map instanceof TrieMap) 
			map = new TrieMap<Long, Long>();
		    else if (map instanceof FFPS) 
			map = new FFPS<Long, Long>();
		    else 
			map = new FFPE<Long, Long>();

		    final Map absmap = map;    
		    int put_threads = T;


		    // put random numbers	    

		    for (int ti = 0 ; ti < put_threads; ti++) {
			int local_thread_dataset_offset = DATASET_SIZE / put_threads;
			int thread_begin_i = ti * local_thread_dataset_offset;
			int thread_end_i = thread_begin_i + local_thread_dataset_offset;
			threads[ti] = new Thread(new Runnable() {
				@Override
				public void run() {
				    for (int i = thread_begin_i; i < thread_end_i; i++) {
					int op = getOperation(DATASET[i]);
					Long v = getNumber(DATASET[i]);
					if (op == 0 || op == 2)
					    absmap.put(v, v);
				    }
				}
			    });
		    }
		    
		    for (int ti = 0; ti < put_threads; ti++)
			threads[ti].start();
		    
		    for (int ti = 0; ti < put_threads; ti++)
			threads[ti].join();
			    
		    // prepare the threads for the execution 
		    
		    for (int t = 0; t < T; t++) {
			int thread_begin_i = t * thread_dataset_offset;
			int thread_end_i = thread_begin_i + thread_dataset_offset;
			
			threads[t] = new Thread(new Runnable() {
				@Override
				public void run() {
				    for (int i = thread_begin_i; i < thread_end_i; i++) {
					int op = (int) getOperation(DATASET[i]);
					Long v = getNumber(DATASET[i]);
					
					switch(op) {
					case 0:
					    // op remove
					    absmap.remove(v);
					    break;
					case 1:
					    // op insert
					    absmap.put(v, v);
					    break;
					default: 
					    // code lookup found / not found
					    absmap.get(v);
					}
				    }				
				}
			    });
		    }
		    
		    // run threads
		    long time = System.nanoTime();
		    
		    for (int t = 0; t < T; t++)
			threads[t].start();
		    
		    for (int t = 0; t < T; t++)
			threads[t].join();
		    
		    long timeUsed = (System.nanoTime() - time) / 1000000L;
		    if (r > WARMUP_RUNS) {
			averageTime += timeUsed;
		    }
		}
		// show time
		System.out.println("Threads = " + T + 
				   " Time = " +  averageTime / RUNS + " MSeconds");
	    }
	}
	return;
    }		

    
    // remove (8^8 - 8^7) keys
    static long n = 0;
    static void remove_keys_by_depth(Map absmap,
				     int depth,
				     int chunk,
				     int dataset_size) {
	if (depth == 0) {
	    // jump chunk numbers
	    n = n + chunk;
	    if (n >= dataset_size)
		return;
	    // remove numbers in chunk
	    for (int i = 0; i < chunk; i++) {
		absmap.remove(n);
		n++;
	    }
	} else {
	    for (int c = 0; c < chunk; c++)
		remove_keys_by_depth(absmap,
				     depth - 1,
				     chunk,
				     dataset_size);
	    // special case ...
	    if (n >= dataset_size)
		return;
	    
	    for (int i = 0; i < Math.pow(chunk, depth + 1); i++) {
		absmap.remove(n);
		n++;
	    }
	}
	return;
    }
    
    
    public void run() 
    	throws InterruptedException {
	
	ConcurrentHashMap<Long, Long> chm =
	    new ConcurrentHashMap<Long, Long>();
	ConcurrentSkipListMap<Long, Long> cslm =
	    new ConcurrentSkipListMap<Long, Long>();
	Map<Long, Long> ct =
	    new TrieMap<Long, Long>();
	FFPS<Long, Long> olfht = 
	    new FFPS<Long, Long>();
	FFPE<Long, Long> colfht = 
	    new FFPE<Long, Long>();
	

	Map maps[] = new Map[5];
	maps[0] = chm; maps[1] = cslm; maps[2] = ct;
	maps[3] = olfht; maps[4] = colfht; 	
	
	
	System.out.println("################ TEST 1 ##############");
	loadAllSearchDataset();
	test_1(maps);
	
	System.out.println("################ TEST 2 ##############");
	//loadAllSearchDataset(); // due to test 1
	test_2(maps);
	
	System.out.println("################ TEST 3 ##############");
	loadAllRemoveDataset();
	test_3(maps);

	System.out.println("################ TEST 4 ##############");
	loadAllSearchDataset();
	test_4(maps);

	System.out.println("################ TEST 5 ##############");
	loadAllInsertDataset();
	test_5(maps);
	
	System.out.println("################ TEST 6 ##############");
	//loadAllInsertDataset();  // due to test 5
	test_6(maps);
	
	//random datasets ....

	System.out.println("################ TESTS RANDOM ########");
	
	for (int di = 0; di < DATASET_INSERT_RATIOS.length; di++) {
	    generateLoadLongRandomDatasets(di, 
					   DATASET_INSERT_RATIOS[di],
					   DATASET_LOOKUP_FOUND_RATIOS[di],
					   DATASET_LOOKUP_NFOUND_RATIOS[di],
					   (1.00 - DATASET_INSERT_RATIOS[di] -
					    DATASET_LOOKUP_FOUND_RATIOS[di] -
					    DATASET_LOOKUP_NFOUND_RATIOS[di]));
	    
	    System.out.println("##########################################################");
	    System.out.printf("Rs:I= %.2f L(to be found)= %.2f L(not found)= %.2f R= %.2f\n", 
			      DATASET_INSERT_RATIOS[di], DATASET_LOOKUP_FOUND_RATIOS[di],
			      DATASET_LOOKUP_NFOUND_RATIOS[di],
			      (1.00 - DATASET_INSERT_RATIOS[di] 
			       - DATASET_LOOKUP_FOUND_RATIOS[di] 
			       - DATASET_LOOKUP_NFOUND_RATIOS[di]));
	    System.out.println("##########################################################");	
	    test_random(maps);
	}

	System.out.println("################ TESTS RANDOM1 ########");
	
	for (int di = 0; di < DATASET_INSERT_RATIOS.length; di++) {
	    generateLoadLongRandomDatasets(di, 
					   DATASET_INSERT_RATIOS[di],
					   DATASET_LOOKUP_FOUND_RATIOS[di],
					   DATASET_LOOKUP_NFOUND_RATIOS[di],
					   (1.00 - DATASET_INSERT_RATIOS[di] -
					    DATASET_LOOKUP_FOUND_RATIOS[di] -
					    DATASET_LOOKUP_NFOUND_RATIOS[di]));
	    
	    System.out.println("##########################################################");
	    System.out.printf("Rs:I= %.2f L(to be found)= %.2f L(not found)= %.2f R= %.2f\n", 
			      DATASET_INSERT_RATIOS[di], DATASET_LOOKUP_FOUND_RATIOS[di],
			      DATASET_LOOKUP_NFOUND_RATIOS[di],
			      (1.00 - DATASET_INSERT_RATIOS[di] 
			       - DATASET_LOOKUP_FOUND_RATIOS[di] 
			       - DATASET_LOOKUP_NFOUND_RATIOS[di]));
	    System.out.println("##########################################################");	
	    test_random1(maps);
	}
	   
	    
	System.out.println("################ TESTS RANDOM2 ########");
	
	for (int di = 0; di < DATASET_INSERT_RATIOS.length; di++) {
	    generateLoadLongRandomDatasets(di, 
					   DATASET_INSERT_RATIOS[di],
					   DATASET_LOOKUP_FOUND_RATIOS[di],
					   DATASET_LOOKUP_NFOUND_RATIOS[di],
					   (1.00 - DATASET_INSERT_RATIOS[di] -
					    DATASET_LOOKUP_FOUND_RATIOS[di] -
					    DATASET_LOOKUP_NFOUND_RATIOS[di]));
	    
	    System.out.println("##########################################################");
	    System.out.printf("Rs:I= %.2f L(to be found)= %.2f L(not found)= %.2f R= %.2f\n", 
			      DATASET_INSERT_RATIOS[di], DATASET_LOOKUP_FOUND_RATIOS[di],
			      DATASET_LOOKUP_NFOUND_RATIOS[di],
			      (1.00 - DATASET_INSERT_RATIOS[di] 
			       - DATASET_LOOKUP_FOUND_RATIOS[di] 
			       - DATASET_LOOKUP_NFOUND_RATIOS[di]));
	    System.out.println("##########################################################");	
	    test_random2(maps);
	}

    	return;
    }

    // functions to prepare the benchmarks
    // load functions
    
    private void loadAllInsertDataset() {
	for (int i = 0; i < DATASET_SIZE; i++) {
	    Long rl = ThreadLocalRandom.current().nextLong(DATASET_SIZE + 1);
	    DATASET[i] = markNumberToBeInserted ((long) rl);
	}
	return;
    }

    private void loadAllRemoveDataset() {
	for (int i = 0; i < DATASET_SIZE; i++) {
	    Long rl = ThreadLocalRandom.current().nextLong(DATASET_SIZE + 1);
	    DATASET[i] = markNumberToBeRemoved ((long) rl);
	}
	return;
    }

    private void loadAllSearchDataset() {
	for (int i = 0; i < DATASET_SIZE; i++) {
	    Long rl = ThreadLocalRandom.current().nextLong(DATASET_SIZE + 1);
	    DATASET[i] = markNumberToBeSearched (rl);
	}
	return;
    }


    public static final void generateLoadLongRandomDatasets(int di,
							    double ops_insert,
							    double ops_found_lookup,
							    double ops_nfound_lookup,
							    double ops_remove) {	
	for (int i = 0; i < DATASET_SIZE_BENCH; i++) {
	    Long generatedLong = new Random().nextLong();
	    double op = new Random().nextDouble();
	    if (op < ops_remove) {
		generatedLong = markNumberToBeRemoved(generatedLong);
	    } else if (op < (ops_remove + ops_insert)) {
		generatedLong = markNumberToBeInserted(generatedLong);
	    } else if (op < (ops_remove + ops_insert + ops_found_lookup)) {
		generatedLong = markNumberToBeLookFound(generatedLong);
	    } else {
		generatedLong = markNumberToBeLookNotFound(generatedLong);		
	    }
	    DATASET[i] = generatedLong;
	}	    
    }
   
    /* auxiliar functions */

    private static long markNumberToBeRemoved (long v) {
	// L at the end signals that number is long (Java) ...
	return (v & 0x3FFFFFFFFFFFFFFFL); 
    }
    
    private static long markNumberToBeInserted (long v) {
	return ((v & 0x3FFFFFFFFFFFFFFFL) | (0x1L << (ARCH - 2)));
    }
 
    private static long markNumberToBeLookFound (long v) {
	return ((v & 0x3FFFFFFFFFFFFFFFL) | (0x2L << (ARCH - 2)));
    }
   
    private static long markNumberToBeLookNotFound (long v) {
	return ((v & 0x3FFFFFFFFFFFFFFFL) | (0x3L << (ARCH - 2)));
    }   
    
    private static long markNumberToBeSearched (long v) {	
	return ((v & 0x3FFFFFFFFFFFFFFFL) | (0x3L << (ARCH - 2)));
    }   

    private static int getOperation(Long v) {
	return (int) (v >> (ARCH - 2) & 0x3L);
    }
        
    private static Long getNumber(Long v) {
    	return (v & 0x3FFFFFFFFFFFFFFFL); 
    }    
}



