package com.ffp;
import sun.misc.Unsafe;
import java.lang.reflect.Field;
import java.util.*;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Map;
import java.util.Enumeration;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentHashMap;
import java.awt.BorderLayout;
import javax.swing.*;
import javax.swing.ImageIcon;
import javax.swing.JFrame;
import javax.swing.JLabel;
import javax.swing.JScrollPane;
import javax.swing.JTree;
import javax.swing.SwingUtilities;
import javax.swing.event.TreeSelectionEvent;
import javax.swing.event.TreeSelectionListener;
import javax.swing.tree.DefaultMutableTreeNode;
import javax.swing.tree.DefaultTreeCellRenderer;
import javax.swing.tree.TreePath;
import javax.swing.ImageIcon;
import javax.swing.UIManager;
import java.awt.Component;
import java.awt.Dimension;    
import java.util.concurrent.atomic.AtomicInteger;

@SuppressWarnings({"unchecked", "rawtypes", "unused"})

public class FFPE<E, V> extends AbstractMap<E, V> implements ConcurrentMap<E,V> {

    /****************************************************************************
     *                           statistics                                     *
     ****************************************************************************/
    private int total_nodes_valid;
    private int total_nodes_invalid;
    private long total_buckets;
    private long total_empties;
    private long total_min_hash_trie_depth;
    private long total_max_hash_trie_depth;
    private long total_max_nodes;
    private long total_min_nodes;
    private long total_memory_jumps;
    private long total_memory_jumps_in_hash_levels;
    private long total_memory_jumps_in_chain_nodes;
    private long total_empty_hash_levels;
    private long partial_max_value;
    /****************************************************************************
     *                           configuration                                  *
     ****************************************************************************/

    private final static int ARCH_BITS = 63;
    private static int MAX_NODES_PER_BUCKET;
    private static int N_BITS;

    private static final Unsafe unsafe;
    private static final int base;
    private static final int scale;
    private static final long next_addr;
    private static final long HN_addr;
    private static final long prev_hash_addr;

    public LFHT_AtomicReferenceArray HN;

    static {
	    /* initialize the unsafe */
        try {
            Field field = Unsafe.class.getDeclaredField("theUnsafe");
            field.setAccessible(true);
            unsafe = (Unsafe) field.get(null);
        } catch (IllegalAccessException | IllegalArgumentException |
                NoSuchFieldException | SecurityException e) {
            throw new AssertionError(e);
        }

        try {
            next_addr = unsafe.objectFieldOffset
                    (LFHT_AnsNode.class.getDeclaredField("next"));

            prev_hash_addr = unsafe.objectFieldOffset
                    (LFHT_AtomicReferenceArray.class.getDeclaredField("ph"));


            HN_addr = unsafe.objectFieldOffset
                    (FFPE.class.getDeclaredField("HN"));


        } catch (ReflectiveOperationException e) {
            throw new Error(e);
        }



        base = unsafe.arrayBaseOffset(Object[].class);
        scale  = unsafe.arrayIndexScale(Object[].class);
    }

    public FFPE() {
	HN = null;
	MAX_NODES_PER_BUCKET = 2;
	N_BITS = 3;

    }
    
    public FFPE(int n_bits, int max_nodes){
	HN = null;
	MAX_NODES_PER_BUCKET = max_nodes;
	N_BITS = n_bits;
    }
    


    /****************************************************************************
     *                           auxiliary macros                               *
     ****************************************************************************/

    private boolean IS_EQUAL_ENTRY(LFHT_AnsNode <E,V> node,
                                   long h,
                                   E t) {
        return node.equals(h, t);
    }

    private boolean IS_VALID_ENTRY(LFHT_AnsNode <E,V> node) {
        return node.valid();
    }

    private boolean IS_HASH(Object node) {
        return (node instanceof LFHT_AtomicReferenceArray);
    }

    private boolean WAS_MARKED_AS_INVALID_NOW(LFHT_AnsNode <E,V> node) {
        return node.markAsInvalid();
    }

    private static int next_position_bit(long hash1, long hash2) {
        int next_p_bits = ARCH_BITS - Long.numberOfLeadingZeros(hash1 ^ hash2);
        return ((next_p_bits / N_BITS) * N_BITS);
    }
    private static int next_position_bit(long hash) {
        int next_p_bits = ARCH_BITS - Long.numberOfLeadingZeros(hash);
        return ((next_p_bits / N_BITS) * N_BITS);
    }

    /****************************************************************************
     *                           compress operations                            *
     ****************************************************************************/

    private static boolean IS_COMPRESSION_NODE(Object node) {
	return (node instanceof Pair);
    }
    
    private static boolean IS_COMPRESSION_NODE(Object node, boolean operation) { 
	// operation == true  ->   freezing operation
	// operation == false -> unfreezing operation
	return (node instanceof Pair && 
		Pair.class.cast(node).mark == operation);
    }
    
    private boolean freeze_hash_level(LFHT_AtomicReferenceArray curr_hash, 
				      Object freezing_node) {
        for (int i = 0; i < curr_hash.n_entries; i++)
            if (curr_hash.compareAndSet(i, curr_hash, freezing_node) == false) {
		curr_hash.c_bucket = i; // freeze failed. mark failure
		return false;
	    }
        return true;
    }
    
    private void unfreeze_hash_level(LFHT_AtomicReferenceArray curr_hash,
				     Object freezing_node) {	
	for (int i = 0; i < curr_hash.c_bucket; i++)
	    curr_hash.compareAndSet(i, freezing_node, curr_hash);
	return;
    }

    private void try_to_compress_hash_level(LFHT_AtomicReferenceArray curr_hash, long key) {	
        if (curr_hash.triggerCompression() == false)
	    return;
	
	LFHT_AtomicReferenceArray prev_hash = curr_hash.ph; // previous hash
	if (prev_hash == null)
	    // unable to compress the root hash level
	    return;
	Object freezing_node = (Object) new Pair(curr_hash, true);
	int bucket = prev_hash.hashEntry(key);
	if (prev_hash.compareAndSet(bucket, curr_hash, freezing_node) == false)
	    // unable to do the compression
	    return;
	Object bucket_next;
	// continue with the compression. freezing_node already in place
	if (freeze_hash_level(curr_hash, freezing_node)) {		
	    do {
		bucket_next = prev_hash.hash[bucket]; 
		while (IS_HASH(bucket_next)) {
		    prev_hash = (LFHT_AtomicReferenceArray) bucket_next;
		    bucket = prev_hash.hashEntry(key);
		    bucket_next = prev_hash.hash[bucket]; 
		}
		if (prev_hash.compareAndSet(bucket, freezing_node, prev_hash)) {
		    try_to_compress_hash_level(prev_hash, key);
		    return;
		}
	    } while (IS_COMPRESSION_NODE(bucket_next, false) == false);
	} else {
	    // freezing failed
	    Object unfreezing_node = (Object) new Pair(curr_hash, false); 
	    prev_hash.compareAndSet(bucket, freezing_node, unfreezing_node);	    
	}
	unfreeze_hash_level(curr_hash, freezing_node);
	// remove unfreezing node from prev_hash
	do {
	    bucket_next = prev_hash.hash[bucket]; 
	    while (IS_HASH(bucket_next)) {
		prev_hash = (LFHT_AtomicReferenceArray) bucket_next;
		bucket = prev_hash.hashEntry(key);
		bucket_next = prev_hash.hash[bucket]; 
	    }
	    // here the bucket_next must hold an unfreezing node
	} while (prev_hash.compareAndSet(bucket, bucket_next, curr_hash) == false);	    
    }

    private LFHT_AnsNode<E,V> check_insert_with_compression(LFHT_AtomicReferenceArray curr_hash,
						       int bucket,
						       long h,
						       E t,
						       V v) {
	Object bucket_next = curr_hash.hash[bucket];
	if (IS_COMPRESSION_NODE(bucket_next) == false)
	    return check_insert_bucket_array(curr_hash, h, t, v);

	LFHT_AtomicReferenceArray next_hash = (LFHT_AtomicReferenceArray) 
	    Pair.class.cast(bucket_next).reference;

	if (next_hash != curr_hash) {	    
	    // ordered insertion (begin)	    
            int next_p_bits = next_position_bit(h, next_hash.hash_val);
            if (next_p_bits <= next_hash.p_bits) {
                return check_insert_bucket_array(next_hash, h, t, v);
            }

            LFHT_AtomicReferenceArray new_hash = new LFHT_AtomicReferenceArray (h);

            do {
                bucket_next = curr_hash.hash[bucket];

		if (IS_COMPRESSION_NODE(bucket_next) == false)
		    return check_insert_bucket_array(curr_hash, h, t, v);
		
		next_hash = (LFHT_AtomicReferenceArray) 
		    Pair.class.cast(bucket_next).reference;

                next_p_bits = next_position_bit(h, next_hash.hash_val);
                if (next_p_bits <= next_hash.p_bits) {
                    return check_insert_bucket_array(next_hash, h, t, v);
                } else /*next_p_bits > next_hash.p_bits */ {
                    new_hash.p_bits = next_p_bits;
                    new_hash.ph = curr_hash;
                    int bucket_next_hash = new_hash.hashEntry(next_hash.hash_val);
                    new_hash.hash[bucket_next_hash] = bucket_next;
		    
                    if (curr_hash.compareAndSet(bucket, bucket_next, new_hash) == true) {
                        next_hash.updatePreviousHash(new_hash);
                        return check_insert_bucket_array(new_hash, h, t, v);
                    } else
			new_hash.hash[bucket_next_hash] = new_hash;
                }
            } while(true);
            // ordered insertion (end)

	}else /* next_hash == curr_hash */ {

	    Pair freezing_node = (Pair) bucket_next;
	    LFHT_AtomicReferenceArray prev_hash = curr_hash.ph;
	    int bucket_prev = prev_hash.hashEntry(h);
	    bucket_next = prev_hash.hash[bucket_prev];

	    if (bucket_next == freezing_node) {
		Object unfreezing_node = (Object) new Pair(curr_hash, false);
		if (prev_hash.compareAndSet(bucket_prev, freezing_node, unfreezing_node) == true) {
		    LFHT_AnsNode new_node = new
			LFHT_AnsNode (h, t, v, curr_hash);
		    if (curr_hash.compareAndSet(bucket, freezing_node, new_node) == true)
			return new_node;
		}
	    } else {	    
		if (IS_COMPRESSION_NODE(bucket_next, false) && 
		    Pair.class.cast(bucket_next).reference == curr_hash) {
		    LFHT_AnsNode new_node = new
			LFHT_AnsNode (h, t, v, curr_hash);
		    if (curr_hash.compareAndSet(bucket, freezing_node, new_node) == true)
			return new_node;
		}
	    }
	    
	    return check_insert_bucket_array(prev_hash, h, t, v);
	}
    }

    
    private void insert_with_compression(LFHT_AtomicReferenceArray curr_hash,
						      int bucket,
						      LFHT_AnsNode <E,V> chain_node) {
	long h = chain_node.hash;
	
	Object bucket_next = curr_hash.hash[bucket];
	if (IS_COMPRESSION_NODE(bucket_next) == false) {
	    insert_bucket_array(curr_hash, chain_node);
	    return;
	}
	
	LFHT_AtomicReferenceArray next_hash = (LFHT_AtomicReferenceArray) 
	    Pair.class.cast(bucket_next).reference;

	if (next_hash != curr_hash) {	    
	    // ordered insertion (begin)	    
            int next_p_bits = next_position_bit(h, next_hash.hash_val);
            if (next_p_bits <= next_hash.p_bits) {
		insert_bucket_array(next_hash, chain_node);
		return;		
            }

            LFHT_AtomicReferenceArray new_hash = new LFHT_AtomicReferenceArray (h);

            do {
                bucket_next = curr_hash.hash[bucket];

		if (IS_COMPRESSION_NODE(bucket_next) == false) {
		    insert_bucket_array(curr_hash, chain_node);
		    return;		    
		}
		
		next_hash = (LFHT_AtomicReferenceArray) 
		    Pair.class.cast(bucket_next).reference;

                next_p_bits = next_position_bit(h, next_hash.hash_val);
                if (next_p_bits <= next_hash.p_bits) {
		    insert_bucket_array(next_hash, chain_node);
		    return;
                } else /*next_p_bits > next_hash.p_bits */ {
                    new_hash.p_bits = next_p_bits;
                    new_hash.ph = curr_hash;
                    int bucket_next_hash = new_hash.hashEntry(next_hash.hash_val);
                    new_hash.hash[bucket_next_hash] = bucket_next;
		    
                    if (curr_hash.compareAndSet(bucket, bucket_next, new_hash) == true) {
                        next_hash.updatePreviousHash(new_hash);
			insert_bucket_array(new_hash, chain_node);
			return;
                    } else
			new_hash.hash[bucket_next_hash] = new_hash;
                }
            } while(true);
            // ordered insertion (end)

	}else /* next_hash == curr_hash */ {

	    Pair freezing_node = (Pair) bucket_next;
	    LFHT_AtomicReferenceArray prev_hash = curr_hash.ph;
	    int bucket_prev = prev_hash.hashEntry(h);
	    bucket_next = prev_hash.hash[bucket_prev];

	    if (bucket_next == freezing_node) {
		Object unfreezing_node = (Object) new Pair(curr_hash, false);
		if (prev_hash.compareAndSet(bucket_prev, freezing_node, unfreezing_node) == true) {
		    if (curr_hash.compareAndSet(bucket, freezing_node, chain_node) == true)
			return;
		}
	    } else {	    
		if (IS_COMPRESSION_NODE(bucket_next, false) && 
		    Pair.class.cast(bucket_next).reference == curr_hash) {
		    if (curr_hash.compareAndSet(bucket, freezing_node, chain_node) == true)
			return;
		}
	    }
	    insert_bucket_array(prev_hash, chain_node);
	    return;
	}
    }

    

    /****************************************************************************
     *                           check (search) and insert operation            *
     ****************************************************************************/

    private void insert_bucket_chain(LFHT_AtomicReferenceArray curr_hash,
                                     LFHT_AnsNode <E,V> chain_node,
                                     Object insert_point_candidate,
                                     Object insert_point_candidate_next,
                                     LFHT_AnsNode <E,V> adjust_node,
                                     int count_nodes,
                                     int next_p_bits) {


	/* the key idea is to stop the check for keys in the ipc
	   instead of the curr_hash curr_hash is used to understand
	   with hash is the thread using */

	if (chain_node == adjust_node)
	    // adjust node is already in the correct hash level
	    return;

        long h = adjust_node.hash;
        int cn = count_nodes;

        Object ipc = insert_point_candidate;
        Object ipc_next = insert_point_candidate_next;
        Object chain_next;
        if (IS_VALID_ENTRY(chain_node)) {
            int np = next_position_bit(h, chain_node.hash);
            if (np > next_p_bits)
                next_p_bits = np;
            cn++;
            ipc = chain_node;
            ipc_next = chain_node.getNext();
            chain_next = ipc_next;
        } else
            chain_next = chain_node.getNext();

        if (!IS_HASH(chain_next)) {
            insert_bucket_chain(curr_hash, (LFHT_AnsNode <E,V>) chain_next,
                    ipc, ipc_next, adjust_node, cn, next_p_bits);
            return;
        }

        if (chain_next == curr_hash) {
            if(cn == MAX_NODES_PER_BUCKET) {
                LFHT_AtomicReferenceArray new_hash = 
		    new LFHT_AtomicReferenceArray(adjust_node.hash, next_p_bits, curr_hash);
                // assisted expansion - begin
                if (LFHT_AnsNode.class.cast(ipc).
                        compareAndSetNext(ipc_next, new_hash)) {
                    int bucket = curr_hash.hashEntry(h);
                    chain_next = curr_hash.hash[bucket];
		    if (IS_COMPRESSION_NODE(chain_next)) {
			// compression detected
			insert_with_compression(curr_hash, bucket, chain_node);
			return;
		    }
                    if (IS_HASH(chain_next) == false) {
                        // is node
                        adjust_chain_nodes(new_hash, chain_next);
                        curr_hash.updateBucketToNextHash(bucket, new_hash);
                    }
                    insert_bucket_array(new_hash, adjust_node);
                    return;
                } else {
		    if (IS_VALID_ENTRY(chain_node)) {
			// check the next hash level
			new_hash = null;
			chain_next = chain_node.getNext();
			if (IS_HASH(chain_next) == false) {
			    do {
				chain_next = LFHT_AnsNode.class.cast(chain_next).getNext();
			    } while (IS_HASH(chain_next) == false);
			}
			// chain_next is a hash for sure...
			LFHT_AtomicReferenceArray next_hash = (LFHT_AtomicReferenceArray) chain_next;
			if(curr_hash == next_hash) {
			    // in the same level. No expansion occured.
			    insert_bucket_array(curr_hash, adjust_node);
			    return;
			}
			
			// get the next hash level. deeper that curr_hash
			while(next_hash.ph != curr_hash)
			    next_hash = next_hash.ph;
			// get the beginning of the chain
			int bucket = curr_hash.hashEntry(h);
			chain_next = curr_hash.hash[bucket];
			if (IS_COMPRESSION_NODE(chain_next)) {
			    // compression detected
			    insert_with_compression(curr_hash, bucket, chain_node);
			    return;
			}

			if (IS_HASH(chain_next) == false) {
			    // expansion still undergoing (assisting)
			    adjust_chain_nodes(next_hash, chain_next);
			    curr_hash.updateBucketToNextHash(bucket, next_hash);
			}
		    }
		    
                    // i have to start from curr_hash, because my h might
                    // demand a next with a p_bit higher than next_hash
                    insert_bucket_array(curr_hash, adjust_node);
                    return;
                }
                // assisted expansion - end
            }
            if (ipc == curr_hash) {
		/* ipc is a hash */
                int bucket = curr_hash.hashEntry(h);
                chain_next = curr_hash.hash[bucket];
		if (IS_COMPRESSION_NODE(chain_next)) {
		    // compression detected
		    insert_with_compression(curr_hash, bucket, chain_node);
		    return;
		}

                if (chain_next == ipc_next) {
                    // assisted expansion - node to be adjusted - begin
                    if (setChainNodeNextToNextHash(adjust_node,curr_hash) == false)
                        return;
                    // assisted expansion - node to be adjusted - end
                    if (curr_hash.compareAndSet(bucket, ipc_next, adjust_node)) {
                        if (!IS_VALID_ENTRY(adjust_node))
			            /*  adjusted a node that was valid
			                before and is invalid now. It might
			                not have been seen by the thread
			                that was deleting the node, thus I
			                must check is the node is present
			                in the current chain and if it is,
			                then I must remove it myself. I
			                don't care about the return of
			                check_delete_bucket_chain, because
			                the node was already returned by
			                the thread that was deleting the
			                node...
			            */
                            delete_bucket_chain(curr_hash, adjust_node);
                        return;
                    }
                    chain_next = curr_hash.hash[bucket];
		    if (IS_COMPRESSION_NODE(chain_next)) {
			// compression detected
			insert_with_compression(curr_hash, bucket, chain_node);
			return;
		    }
                }
		/* recover always to a hash bucket array */

                if (!IS_HASH(chain_next) || (LFHT_AtomicReferenceArray) chain_next == curr_hash) {
                    // i'm in the curr_hash level
                    insert_bucket_array(curr_hash, adjust_node);
                    return;
                }

            } else {
		/* ipc is a node */
                chain_next = LFHT_AnsNode.class.cast(ipc).getNext();
                if (chain_next == ipc_next) {
                    // assisted expansion - node to be adjusted - begin
                    if (setChainNodeNextToNextHash(adjust_node,curr_hash) == false)
                        return;
                    // assisted expansion - node to be adjusted - end
                    if (LFHT_AnsNode.class.cast(ipc).
                            compareAndSetNext(ipc_next, adjust_node)) {
                        if (!IS_VALID_ENTRY(adjust_node))
                            /* adjusted a node that was valid
			       before and is invalid now. It might
			       not have been seen by the thread
			       that was deleting the node, thus I
			       must check is the node is present
			       in the current chain and if it is,
			       then I must remove it myself. I
			       don't care about the return of
			       check_delete_bucket_chain, because
			       the node was already returned by
			       the thread that was deleting the
			       node...
			    */
                            delete_bucket_chain(curr_hash, adjust_node);
                        return;
                    }
                    chain_next = LFHT_AnsNode.class.cast(ipc).getNext();
                }

		/* recover always to a hash bucket array */
                if (!IS_HASH(chain_next) || chain_next == curr_hash) {
                    insert_bucket_array(curr_hash, adjust_node);
                    return;
                }
            }
        }

        // assisted expansion - assist in the expansion - former recover to previous hash - begin
        /* IS_HASH(chain_next) && chain_next != curr_hash */
        // there is at least one expansion going on. I must help in the expansion....
        LFHT_AtomicReferenceArray next_hash = (LFHT_AtomicReferenceArray) chain_next;
        while(next_hash.ph != curr_hash)
            next_hash = next_hash.ph;

        int bucket = curr_hash.hashEntry(h);
        chain_next = curr_hash.hash[bucket];
	if (IS_COMPRESSION_NODE(chain_next)) {
	    // compression detected
	    insert_with_compression(curr_hash, bucket, chain_node);
	    return;
	}

        if (IS_HASH(chain_next) == false) {
            adjust_chain_nodes(next_hash, chain_next);
            curr_hash.updateBucketToNextHash(bucket, next_hash);
        }
        insert_bucket_array(curr_hash, adjust_node);
        return;
        // assisted expansion - assist in the expansion - former recover to previous hash - end
    }


    private void insert_bucket_array(LFHT_AtomicReferenceArray curr_hash,
                                     LFHT_AnsNode <E,V> chain_node) {

        // assisted expansion - node to be adjusted - begin
        if (setChainNodeNextToNextHash(chain_node,curr_hash) == false)
            return;
        // assisted expansion - node to be adjusted - end

	if (!IS_VALID_ENTRY(chain_node))
            return;

        int bucket;
        long h = chain_node.hash;
        bucket = curr_hash.hashEntry(h);
        if (curr_hash.isEmptyBucket(bucket)) {
            if (curr_hash.compareAndSet(bucket, curr_hash, chain_node)) {
                if (!IS_VALID_ENTRY(chain_node))
		       /* adjusted a node that was valid
			  before and is invalid now. It might
			  not have been seen by the thread
			  that was deleting the node, thus I
			  must check is the node is present
			  in the current chain and if it is,
			  then I must remove it myself. I
			  don't care about the return of
			  check_delete_bucket_chain, because
			  the node was already returned by
			  the thread that was deleting the
			  node...
		       */
                    delete_bucket_chain(curr_hash, chain_node);
                return;
            }
        }
        Object bucket_next = curr_hash.hash[bucket];
	if (IS_COMPRESSION_NODE(bucket_next)) {
	    // compression detected
	    insert_with_compression(curr_hash, bucket, chain_node);
	    return;
	}

        if (IS_HASH(bucket_next)) {
	        /* with deletes a bucket entry might refer more than once to curr_hash */
            // ordered insertion (begin)
            LFHT_AtomicReferenceArray next_hash = (LFHT_AtomicReferenceArray) bucket_next; // once a hash, always hash
            int next_p_bits = next_position_bit(h, next_hash.hash_val);
            if (next_p_bits <= next_hash.p_bits) {
                insert_bucket_array(next_hash, chain_node);
                return;
            }

            LFHT_AtomicReferenceArray new_hash = new LFHT_AtomicReferenceArray (h);

            do {
                next_hash = (LFHT_AtomicReferenceArray) curr_hash.hash[bucket];
		if (IS_COMPRESSION_NODE(next_hash)) {
		    // compression detected
		    insert_with_compression(curr_hash, bucket, chain_node);
		    return;
		}

                next_p_bits = next_position_bit(h, next_hash.hash_val);
                if (next_p_bits <= next_hash.p_bits) {
                    insert_bucket_array(next_hash, chain_node);
                    return;
                } else /*next_p_bits > next_hash.p_bits */ {
                    new_hash.p_bits = next_p_bits;
                    new_hash.ph = curr_hash;
                    int bucket_next_hash = new_hash.hashEntry(next_hash.hash_val);
                    new_hash.hash[bucket_next_hash] = next_hash;

                    if (curr_hash.compareAndSet(bucket, next_hash, new_hash) == true) {
                        next_hash.updatePreviousHash(new_hash);
                        insert_bucket_array(new_hash, chain_node);
                        return;
                    }
                }
            } while(true);
            // ordered insertion (end)

        } else {
            int next_p_bits = next_position_bit(h, LFHT_AnsNode.class.cast(bucket_next).hash);
            insert_bucket_chain(curr_hash, (LFHT_AnsNode<E,V>) bucket_next,
                    curr_hash, (LFHT_AnsNode<E,V>) bucket_next,
                    chain_node, 0, next_p_bits);
        }
    }

    private void adjust_chain_nodes(LFHT_AtomicReferenceArray new_hash,
                                    Object chain_curr) {
        /* traverse the list of nodes to be adjusted */
	if (IS_HASH(chain_curr))
	    return;
        adjust_chain_nodes(new_hash, LFHT_AnsNode.class.cast(chain_curr).getNext());
        insert_bucket_array(new_hash, LFHT_AnsNode.class.cast(chain_curr));
    }

    private LFHT_AnsNode <E,V> check_insert_bucket_chain(LFHT_AtomicReferenceArray curr_hash,
                                                         LFHT_AnsNode<E,V> chain_node,
                                                         Object insert_point_candidate,
                                                         Object insert_point_candidate_next,
                                                         long h,
                                                         E t,
                                                         V v,
                                                         int count_nodes,
                                                         int next_p_bits) {

	/* the key idea is to stop the check for keys in the ipc
	   instead of the curr_hash. curr_hash is used only to
	   understand which hash is the thread using */

        int cn = count_nodes;
        if (IS_EQUAL_ENTRY(chain_node, h, t))
            return chain_node;
        Object ipc = insert_point_candidate;
        Object ipc_next = insert_point_candidate_next;
        Object chain_next;
        if (IS_VALID_ENTRY(chain_node)) {
            int np = next_position_bit(h, chain_node.hash);
            if (np > next_p_bits)
                next_p_bits = np;
            cn++;
            ipc = chain_node;
            ipc_next = chain_node.getNext();
            chain_next = ipc_next;
        } else {
            chain_next = chain_node.getNext();
        }

        if (!IS_HASH(chain_next)) {
            return check_insert_bucket_chain(curr_hash, (LFHT_AnsNode<E,V>) chain_next,
                    ipc, ipc_next, h, t, v, cn, next_p_bits);
        }

        if ((LFHT_AtomicReferenceArray) chain_next == curr_hash) {
            if(cn == MAX_NODES_PER_BUCKET) {
                LFHT_AtomicReferenceArray new_hash = new LFHT_AtomicReferenceArray(h, next_p_bits, curr_hash);
                // assisted expansion - begin
                if (LFHT_AnsNode.class.cast(ipc).compareAndSetNext(ipc_next, new_hash)) {
                    int bucket = curr_hash.hashEntry(h);
                    chain_next = curr_hash.hash[bucket];
		    if (IS_COMPRESSION_NODE(chain_next)) {
			// compression detected
			return check_insert_with_compression(curr_hash, bucket, h, t, v);
		    }

                    if (IS_HASH(chain_next) == false) {
                        // is node
                        adjust_chain_nodes(new_hash, chain_next);
                        curr_hash.updateBucketToNextHash(bucket, new_hash);
                    }
                    // as my hash was placed correctly, i can move directly to 'new_hash'
                    return check_insert_bucket_array(new_hash, h, t, v);
                } else { 
		    if (IS_VALID_ENTRY(chain_node)) {
			// check the next hash level
			new_hash = null;
			chain_next = chain_node.getNext();
			if (IS_HASH(chain_next) == false) {
			    do {
				chain_next = LFHT_AnsNode.class.cast(chain_next).getNext();
			    } while (IS_HASH(chain_next) == false);
			}
			// chain_next is a hash for sure...
			LFHT_AtomicReferenceArray next_hash = (LFHT_AtomicReferenceArray) chain_next;
			
			if(curr_hash == next_hash)
			    // in the same level. No expansion occured.
			    return check_insert_bucket_array(curr_hash, h, t, v);
			
			// get the next hash level. deeper that curr_hash
			while(next_hash.ph != curr_hash)
			    next_hash = next_hash.ph;
			// get the beginning of the chain
			int bucket = curr_hash.hashEntry(h);
			chain_next = curr_hash.hash[bucket];
			if (IS_COMPRESSION_NODE(chain_next)) {
			    // compression detected
			return check_insert_with_compression(curr_hash, bucket, h, t, v);
			}
			if (IS_HASH(chain_next) == false) {
			    // expansion still undergoing (assisting)
			    adjust_chain_nodes(next_hash, chain_next);
			    curr_hash.updateBucketToNextHash(bucket, next_hash);
			}
		    }
		    
                    // i have to start from curr_hash, because my h might
                    // demand a next with a p_bit higher than next_hash
                    return check_insert_bucket_array(curr_hash, h, t, v);
                }
                // assisted expansion - end

            }
            if (ipc == curr_hash) {
		        /* ipc is a hash */
                int bucket = curr_hash.hashEntry(h);
                chain_next = curr_hash.hash[bucket];
		if (IS_COMPRESSION_NODE(chain_next)) {
		    // compression detected
		    return check_insert_with_compression(curr_hash, bucket, h, t, v);
		}
		
                if (chain_next == ipc_next) {
                    LFHT_AnsNode <E,V> new_node =
                            new LFHT_AnsNode <E,V> (h, t, v, curr_hash);

                    if (curr_hash.compareAndSet(bucket, ipc_next, new_node))
                        return new_node;

                    chain_next = curr_hash.hash[bucket];
                }
		if (IS_COMPRESSION_NODE(chain_next)) {
		    // compression detected
		    return check_insert_with_compression(curr_hash, bucket, h, t, v);
		}
		
		
		/* recover always to a hash bucket array */
                if (!IS_HASH(chain_next) || (LFHT_AtomicReferenceArray) chain_next == curr_hash)
		    // i'm in the curr_hash level
                    return check_insert_bucket_array(curr_hash, h, t, v);

            } else {
                /* ipc is a node */
                chain_next = LFHT_AnsNode.class.cast(ipc).getNext();
                if (chain_next == ipc_next) {
                    LFHT_AnsNode <E,V> new_node = new
                            LFHT_AnsNode <E,V> (h, t, v, curr_hash);

                    if (LFHT_AnsNode.class.cast(ipc).
                            compareAndSetNext(ipc_next, new_node))
                        return new_node;
                    chain_next = LFHT_AnsNode.class.cast(ipc).getNext();
                }

		/* recover always to a hash bucket array */
                if (!IS_HASH(chain_next) || (LFHT_AtomicReferenceArray) chain_next == curr_hash)
                    return check_insert_bucket_array(curr_hash, h, t, v);

            }
        }

        // assisted expansion - assist in the expansion - former recover to previous hash - begin
        /* IS_HASH(chain_next) && chain_next != curr_hash */
        // there is at least one expansion going on. I must help in the expansion....
        LFHT_AtomicReferenceArray next_hash = (LFHT_AtomicReferenceArray) chain_next;
        while(next_hash.ph != curr_hash)
            next_hash = next_hash.ph;

        int bucket = curr_hash.hashEntry(h);
        chain_next = curr_hash.hash[bucket];
	if (IS_COMPRESSION_NODE(chain_next)) {
	    // compression detected
	    return check_insert_with_compression(curr_hash, bucket, h, t, v);
	}

        if (IS_HASH(chain_next) == false) {
            adjust_chain_nodes(next_hash, chain_next);
            curr_hash.updateBucketToNextHash(bucket, next_hash);
        }
        return check_insert_bucket_array(curr_hash, h, t, v);
        // assisted expansion - assist in the expansion - former recover to previous hash - end
    }

    private LFHT_AnsNode<E,V> check_insert_bucket_array(LFHT_AtomicReferenceArray curr_hash,
                                                        long h,
                                                        E t,
                                                        V v) {
        int bucket;
        bucket = curr_hash.hashEntry(h);
        if (curr_hash.isEmptyBucket(bucket)) {
            LFHT_AnsNode <E,V> new_node = new
                    LFHT_AnsNode <E,V> (h, t, v, curr_hash);
            if (curr_hash.compareAndSet(bucket, curr_hash, new_node)) {
                //System.err.println("new_node " + t);
                return new_node;
            }
        }
        Object bucket_next = curr_hash.hash[bucket];
	if (IS_COMPRESSION_NODE(bucket_next)) {
	    // compression detected
	    return check_insert_with_compression(curr_hash, bucket, h, t, v);
	}

        if (IS_HASH(bucket_next)) {
	    /* with deletes a bucket entry might refer more than once to curr_hash */
	    // ordered insertion (begin)
            LFHT_AtomicReferenceArray next_hash = (LFHT_AtomicReferenceArray) bucket_next; // once hash, always hash
            int next_p_bits = next_position_bit(h, next_hash.hash_val);
            if (next_p_bits <= next_hash.p_bits) {
                return check_insert_bucket_array(next_hash, h, t, v);
            }

            LFHT_AtomicReferenceArray new_hash = new LFHT_AtomicReferenceArray (h);

            do {
                next_hash = (LFHT_AtomicReferenceArray) curr_hash.hash[bucket];
		if (IS_COMPRESSION_NODE(next_hash)) {
		    // compression detected
		    return check_insert_with_compression(curr_hash, bucket, h, t, v);
		}

                next_p_bits = next_position_bit(h, next_hash.hash_val);
                if (next_p_bits <= next_hash.p_bits) {
                    return check_insert_bucket_array(next_hash, h, t, v);
                } else /*next_p_bits > next_hash.p_bits */ {
                    new_hash.p_bits = next_p_bits;
                    new_hash.ph = curr_hash;
                    int bucket_next_hash = new_hash.hashEntry(next_hash.hash_val);
                    new_hash.hash[bucket_next_hash] = next_hash;

                    if (curr_hash.compareAndSet(bucket, next_hash, new_hash) == true) {
                        next_hash.updatePreviousHash(new_hash);
                        return check_insert_bucket_array(new_hash, h, t, v);
                    }
                }
            } while(true);
            // ordered insertion (end)
        } else {
            int next_p_bits = next_position_bit(h, LFHT_AnsNode.class.cast(bucket_next).hash);
            return check_insert_bucket_chain(curr_hash, (LFHT_AnsNode<E, V>) bucket_next,
                    curr_hash, (LFHT_AnsNode<E, V>) bucket_next,
                    h, t, v, 0, next_p_bits);
        }
    }

    /****************************************************************************
     *                           check (search) operation                       *
     ****************************************************************************/


    private LFHT_AnsNode<E,V> check_bucket_array(LFHT_AtomicReferenceArray curr_hash,
                                                 long h,
                                                 E t) {
        do {
            int bucket;
            bucket = curr_hash.hashEntry(h);
            Object bucket_next = curr_hash.hash[bucket];
	    if (IS_COMPRESSION_NODE(bucket_next)) {
		// compression detected
		if (Pair.class.cast(bucket_next).reference != curr_hash)
		    bucket_next = Pair.class.cast(bucket_next).reference;
		else
		    return null;
	    }

            if (bucket_next == curr_hash)
                // isEmptyBucket
                return null;

            if (IS_HASH(bucket_next)) {
                // ordered search (begin)
                LFHT_AtomicReferenceArray next_hash = (LFHT_AtomicReferenceArray) bucket_next; // once hash, always hash
                int next_p_bits = next_position_bit(h, next_hash.hash_val);
                if (next_p_bits <= next_hash.p_bits) {
                    curr_hash = next_hash;
                } else /*next_p_bits > next_hash.p_bits */
                    return null;
                // ordered insertion (end)
            } else
                return check_bucket_chain(curr_hash, (LFHT_AnsNode<E,V>) bucket_next,
                        curr_hash, (LFHT_AnsNode<E,V>) bucket_next,
                        h, t);
        } while(true);
    }


    private LFHT_AnsNode <E,V> check_bucket_chain(LFHT_AtomicReferenceArray curr_hash,
                                                  LFHT_AnsNode <E,V> chain_node,
                                                  Object insert_point_candidate,
                                                  Object insert_point_candidate_next,
                                                  long h,
                                                  E t) {
        if (IS_EQUAL_ENTRY(chain_node, h, t))
            return chain_node;

        Object ipc = insert_point_candidate;
        Object ipc_next = insert_point_candidate_next;
        Object chain_next;
        if (IS_VALID_ENTRY(chain_node)) {
            ipc = chain_node;
            ipc_next = chain_node.getNext();
            chain_next = ipc_next;
        } else
            chain_next = chain_node.getNext();

        if (!IS_HASH(chain_next))
            return check_bucket_chain(curr_hash, (LFHT_AnsNode<E,V>) chain_next,
                    ipc, ipc_next, h, t);

        if ((LFHT_AtomicReferenceArray) chain_next == curr_hash) {

            if (ipc == curr_hash) {
		        /* ipc is a hash */
                int bucket = curr_hash.hashEntry(h);
                chain_next = curr_hash.hash[bucket];
		if (IS_COMPRESSION_NODE(chain_next)) {
		    // compression detected
		    if (Pair.class.cast(chain_next).reference != curr_hash)
			chain_next = Pair.class.cast(chain_next).reference;
		    else
			return null;		    
		}

                if (chain_next == ipc_next)
                    return null;
		/* recover always to a hash bucket array */
		
                if (IS_HASH(chain_next))
                    if(chain_next != curr_hash)
			/* invariant */
                        return check_bucket_array((LFHT_AtomicReferenceArray)
                                        chain_next, h, t);

                return check_bucket_array(curr_hash, h, t);
            } else {
		/* ipc is a node */
                chain_next = LFHT_AnsNode.class.cast(ipc).getNext();
                if (chain_next == ipc_next)
                    return null;

		/* recover always to a hash bucket array */
                if (!IS_HASH(chain_next) || (LFHT_AtomicReferenceArray) chain_next == curr_hash)
                    return check_bucket_array(curr_hash,
                            h, t);
		/* recover with jump_hash */
            }
        }

    	/* avoid busy waiting */
        LFHT_AtomicReferenceArray jump_hash =
                LFHT_AtomicReferenceArray.class.cast(chain_next).jumpToPreviousHash(curr_hash, h);

        return check_bucket_array(jump_hash, h, t);
    }


    /****************************************************************************
     *                           check (search) delete operation                *
     ****************************************************************************/

    private LFHT_AnsNode<E,V> check_delete_bucket_array(LFHT_AtomicReferenceArray curr_hash,
                                                        long h,
                                                        E t) {

        int bucket;
        bucket = curr_hash.hashEntry(h);
        if (curr_hash.isEmptyBucket(bucket)) {
	    try_to_compress_hash_level(curr_hash, curr_hash.hash_val);
            return null;
	}
        Object bucket_next = curr_hash.hash[bucket];
	if (IS_COMPRESSION_NODE(bucket_next)) {
	    // compression detected
	    if (Pair.class.cast(bucket_next).reference != curr_hash)
		bucket_next = Pair.class.cast(bucket_next).reference;
	    else
		return null;
	}

        if (IS_HASH(bucket_next)) {
            // ordered search (begin)
            LFHT_AtomicReferenceArray next_hash = (LFHT_AtomicReferenceArray) bucket_next; // once hash, always hash
            int next_p_bits = next_position_bit(h, next_hash.hash_val);
            if (next_p_bits <= next_hash.p_bits) {
                return check_delete_bucket_array(next_hash, h, t);
            } else /*next_p_bits > next_hash.p_bits */
                return null;
            // ordered insertion (end)
        } else
            return check_delete_bucket_chain(curr_hash, (LFHT_AnsNode<E,V>) bucket_next,
                    curr_hash, (LFHT_AnsNode<E,V>) bucket_next,
                    h, t);
    }

    private LFHT_AnsNode<E,V> check_delete_bucket_chain(LFHT_AtomicReferenceArray curr_hash,
                                                        LFHT_AnsNode<E,V> chain_node,
                                                        Object insert_point_candidate,
                                                        Object insert_point_candidate_next,
                                                        long h,
                                                        E t) {
        if (IS_EQUAL_ENTRY(chain_node, h, t)) {
    	    /* be aware that at this instant the node was seen as valid */
            if (WAS_MARKED_AS_INVALID_NOW(chain_node)) {
                delete_bucket_chain(curr_hash, chain_node);
                return chain_node;
            }
        }

        Object ipc = insert_point_candidate;
        Object ipc_next = insert_point_candidate_next;
        Object chain_next;
        if (IS_VALID_ENTRY(chain_node)) {
            ipc = chain_node;
            ipc_next = chain_node.getNext();
            chain_next = ipc_next;
        } else
            chain_next = chain_node.getNext();

        if (!IS_HASH(chain_next)) {
            return check_delete_bucket_chain(curr_hash, (LFHT_AnsNode<E,V>) chain_next,
                    ipc, ipc_next, h, t);
        }

        if ((LFHT_AtomicReferenceArray) chain_next == curr_hash) {
            if (ipc == curr_hash) {
		        /* ipc is a hash */
                int bucket = curr_hash.hashEntry(h);
                chain_next = curr_hash.hash[bucket];
		if (IS_COMPRESSION_NODE(chain_next)) {
		    // compression detected
		    return check_delete_bucket_array(curr_hash, h, t);
		}

                if (chain_next == ipc_next)
                    return null;

		/* recover always to a hash bucket array */
                if (IS_HASH(chain_next))
                    if(chain_next != curr_hash)
			/* invariant */
                        return check_delete_bucket_array((LFHT_AtomicReferenceArray)
							 chain_next, h, t);
                return check_delete_bucket_array(curr_hash, h, t);
            } else {
		/* ipc is a node */
                chain_next = LFHT_AnsNode.class.cast(ipc).getNext();
                if (chain_next == ipc_next)
                    return null;

		/* recover always to a hash bucket array */
                if (!IS_HASH(chain_next) || (LFHT_AtomicReferenceArray) chain_next == curr_hash)
                    return check_delete_bucket_array(curr_hash, h, t);

            }
        }
        /* recover with jump_hash */
    	/* avoid busy waiting */
        LFHT_AtomicReferenceArray jump_hash =
                LFHT_AtomicReferenceArray.class.cast(chain_next).jumpToPreviousHash(curr_hash, h);

        return check_delete_bucket_array(jump_hash, h, t);
    }

    private LFHT_AnsNode<E,V> delete_bucket_chain(LFHT_AtomicReferenceArray curr_hash,
                                                  LFHT_AnsNode chain_node) {

        do {
            Object chain_next_valid_candidate;
            Object chain_curr = (Object) chain_node;
    	    /* --> set chain_next_valid - begin <-- */
            do
                chain_curr = LFHT_AnsNode.class.cast(chain_curr).getNext();
            while (!IS_HASH(chain_curr) &&
		   !LFHT_AnsNode.class.cast(chain_curr).valid());

            if (IS_HASH(chain_curr) && ((LFHT_AtomicReferenceArray)chain_curr != curr_hash)) {
    		/* re-positioning the thread in next hash level.  the
    		   pointer in the chain of curr_hash will be corrected
    		   by the adjust_chain_nodes procedure */
                LFHT_AtomicReferenceArray jump_hash =
                        LFHT_AtomicReferenceArray.class.cast(chain_curr).jumpToPreviousHash(curr_hash, chain_node.hash);

                return delete_bucket_chain(jump_hash, chain_node);
            }
    	    /* chain_curr is a valid node or the curr_hash */
            chain_next_valid_candidate = chain_curr;
            if (!IS_HASH(chain_curr))
                do
                    chain_curr = LFHT_AnsNode.class.cast(chain_curr).getNext();
                while (!IS_HASH(chain_curr));

            if (chain_curr != curr_hash) {
    		/* re-positioning the thread in next hash level.  the
    		   pointer in the chain of curr_hash will be corrected
    		   by the adjust_chain_nodes procedure */
                LFHT_AtomicReferenceArray jump_hash =
                        LFHT_AtomicReferenceArray.class.cast(chain_curr).jumpToPreviousHash(curr_hash, chain_node.hash);

                return delete_bucket_chain(jump_hash, chain_node);
            }

            Object chain_prev_valid_candidate = curr_hash;
            int bucket = curr_hash.hashEntry(chain_node.hash);
            chain_curr = curr_hash.hash[bucket];
	    if (IS_COMPRESSION_NODE(chain_curr)) {
		// compression detected
                LFHT_AtomicReferenceArray jump_hash = (LFHT_AtomicReferenceArray)
		    Pair.class.cast(chain_curr).reference;
		if (jump_hash == curr_hash)
		    return null;
		return delete_bucket_chain(jump_hash, chain_node);
	    }

            Object chain_prev_valid_candidate_next = chain_curr;

            while (!IS_HASH(chain_curr) &&
                    LFHT_AnsNode.class.cast(chain_curr) != chain_node) {

                if (LFHT_AnsNode.class.cast(chain_curr).valid()) {
                    chain_prev_valid_candidate = chain_curr;
                    chain_curr = LFHT_AnsNode.class.cast(chain_curr).getNext();
                    chain_prev_valid_candidate_next = chain_curr;
                } else
                    chain_curr = LFHT_AnsNode.class.cast(chain_curr).getNext();
            }

            if (IS_HASH(chain_curr)) {

                if((LFHT_AtomicReferenceArray)chain_curr == curr_hash) {
		    /* unable to find chain_node in the chain */
		    try_to_compress_hash_level(curr_hash, curr_hash.hash_val);
                    return null;
                } else {
                    LFHT_AtomicReferenceArray jump_hash =
                            LFHT_AtomicReferenceArray.class.cast(chain_curr).jumpToPreviousHash(
                                    curr_hash, chain_node.hash);

                    return delete_bucket_chain(jump_hash, chain_node);
                }
            } else /* LFHT_AnsNode.class.cast(chain_curr) == chain_node */ {

                if (chain_prev_valid_candidate == curr_hash) {

                    if (curr_hash.compareAndSet(bucket,
                            chain_prev_valid_candidate_next,
                            chain_next_valid_candidate)) {
			/* update was ok */
                        if (!IS_HASH(chain_next_valid_candidate) &&
                                !IS_VALID_ENTRY((LFHT_AnsNode)chain_next_valid_candidate))
			    /* restart the process */
                            continue;
			if (chain_next_valid_candidate == curr_hash)
			    // chain is empty
			    try_to_compress_hash_level(curr_hash, curr_hash.hash_val);
                        return chain_node;
                    } else /* compareAndSet == false */ {
    			        /* restart the process */
                        continue;
                    }
                } else /* chain_prev_valid_candidate is node */ {
                    if (LFHT_AnsNode.class.cast(chain_prev_valid_candidate).
			compareAndSetNext(chain_prev_valid_candidate_next,
					  chain_next_valid_candidate)) {
			/* update was ok */
                        if (!IS_HASH(chain_next_valid_candidate) &&
			    !IS_VALID_ENTRY((LFHT_AnsNode)chain_next_valid_candidate))
			    /* restart the process */
                            continue;
                        return chain_node;
                    } else /* compareAndSetNext == false */ {
			/* restart the process */
                        continue;
                    }
                }
            }
        } while(true);
    }

    /****************************************************************************
     *                           flush statistics                               *
     *                           (non concurrent)                               *
     ****************************************************************************/

    private void flush_bucket_chain(Object chain_node,
                                    int count_nodes,
                                    int level,
                                    boolean flush_nodes) {
        if (IS_HASH(chain_node)) {
            if (count_nodes > total_max_nodes)
                total_max_nodes = count_nodes;
            if (count_nodes < total_min_nodes)
                total_min_nodes = count_nodes;
            return;
        }
        if (IS_VALID_ENTRY(LFHT_AnsNode.class.cast(chain_node)))
            total_nodes_valid++;
        else
            total_nodes_invalid++;
        if (flush_nodes)
            System.err.println(" " + LFHT_AnsNode.class.cast(chain_node).entry + " ");
        flush_bucket_chain(LFHT_AnsNode.class.cast(chain_node).getNext(),
                count_nodes + 1, level, flush_nodes);
        return;
    }

    private void flush_bucket_array(LFHT_AtomicReferenceArray curr_hash,
                                    int level,
                                    boolean flush_nodes) {
        int bucket_entry = 0;
	int empty_hash_levels = 0;
        do {
            if (flush_nodes)
                System.err.println("\n bkt entry -> " +
				   bucket_entry + " (level = " +
				   level + ", entries = " + curr_hash.n_entries + ", pos = " + curr_hash.p_bits + ")");
            total_buckets++;

            if (!curr_hash.isEmptyBucket(bucket_entry)) {
                Object bucket_next = curr_hash.hash[bucket_entry];
		if (IS_COMPRESSION_NODE(bucket_next)) {
		    // compression detected
                    System.err.println("ERROR - freezing/unfreezing node found");
                    System.exit(0);
		}

                if (IS_HASH(bucket_next))
                    flush_bucket_array((LFHT_AtomicReferenceArray) bucket_next,
                            level + 1, flush_nodes);
                else {
                    flush_bucket_chain(bucket_next, 0, level, flush_nodes);
    		    /* leaf bucket_array */
                    if (level > total_max_hash_trie_depth)
                        total_max_hash_trie_depth = level;
                    if (level < total_min_hash_trie_depth)
                        total_min_hash_trie_depth = level;
                }
                if (flush_nodes)
                    System.err.println("");
            } else {
                total_empties++;
		empty_hash_levels++;
    		/* leaf bucket_array */
                if (level > total_max_hash_trie_depth)
                    total_max_hash_trie_depth = level;
                if (level < total_min_hash_trie_depth)
                    total_min_hash_trie_depth = level;
            }

        } while (++bucket_entry < curr_hash.n_entries);
	if (empty_hash_levels == curr_hash.n_entries)	    
	    total_empty_hash_levels++;

        return;
    }

    public void flush_hash_statistics(boolean flush_nodes) {

        total_nodes_valid = 0;
        total_nodes_invalid = 0;
        total_buckets = 0;
        total_empties = 0;
        total_min_hash_trie_depth = Long.MAX_VALUE;
        total_max_hash_trie_depth = 0;
        total_max_nodes = 0;
        total_min_nodes =  Long.MAX_VALUE;
	total_empty_hash_levels = 0;

        flush_bucket_array(HN, 0, flush_nodes);

        if (total_min_nodes ==  Long.MAX_VALUE)
            total_min_nodes = 0;

        System.out.println("-----------------------------------------------------");
        System.out.println(" Nr of valid nodes         = " + total_nodes_valid);
	System.out.println(" Nr of empty hash levels   = " + total_empty_hash_levels);
        System.out.println(" Nr of buckets             = " + total_buckets);
        System.out.println(" Nr of empty buckets       = " + total_empties);
        System.out.println(" Min hash trie depth       = " + total_min_hash_trie_depth +
                "   (Root depth = 0)");
        System.out.println(" Max hash trie depth       = " + total_max_hash_trie_depth +
                "   (Root depth = 0)");
        System.out.println(" Max nodes (non empty)     = " + total_max_nodes +
                "   (MAX_NODES_PER_BUCKET = " + MAX_NODES_PER_BUCKET + ")");
        System.out.println(" Min nodes (non empty)     = " + total_min_nodes);
        long non_empty_buckets = total_buckets - total_empties;
        if (non_empty_buckets == 0)
            System.out.println(" Avg nodes per bucket (valid + invalid) = " +
                    (float)(total_nodes_valid + total_nodes_invalid) /
                            (total_buckets) + " (non empty only) = 0.0");
        else
            System.out.println(" Avg nodes per bucket (valid + invalid) = " +
                    (float)(total_nodes_valid + total_nodes_invalid) /
                            (total_buckets) + " (non empty only) = " +
                    (float) (total_nodes_valid + total_nodes_invalid) /
                            (float) non_empty_buckets);

        System.out.println("-----------------------------------------------------");
    }

    /****************************************************************************
     *                           flush statistics2                              *
     *          (non concurrent - with single hash compression)                 *
     ****************************************************************************/


    private void flush_bucket_chain2(LFHT_AtomicReferenceArray curr_hash,
                                     Object chain_node,
                                     int level,
                                     boolean flush_nodes,
                                     Map check_lfht,
                                     int white_spaces) {

        ArrayList<LFHT_AnsNode> chain_of_nodes = new ArrayList<LFHT_AnsNode>();
        int count_nodes = 0;
        while(!IS_HASH(chain_node)) {
            total_memory_jumps++;
            total_memory_jumps_in_chain_nodes++;
            if (IS_VALID_ENTRY(LFHT_AnsNode.class.cast(chain_node))) {
                if (check_lfht.get(LFHT_AnsNode.class.cast(chain_node).entry) == null) {
                    check_lfht.put(LFHT_AnsNode.class.cast(chain_node).entry, LFHT_AnsNode.class.cast(chain_node).entry);
                    chain_of_nodes.add(LFHT_AnsNode.class.cast(chain_node));
                    total_nodes_valid++;
                }
            } else
                total_nodes_invalid++;
            chain_node = LFHT_AnsNode.class.cast(chain_node).getNext();
            count_nodes++;
        }

        if ((LFHT_AtomicReferenceArray) chain_node != curr_hash) {
            System.err.println("ERROR - path chain_node != curr_hash ");
            System.exit(0);
        }

        if (count_nodes > MAX_NODES_PER_BUCKET) {
            System.err.println("ERROR - count_nodes (" + count_nodes + ") > MAX_NODES_PER_BUCKET (" + MAX_NODES_PER_BUCKET + ")");
            System.exit(0);
        }

        if (count_nodes > total_max_nodes)
            total_max_nodes = count_nodes;
        if (count_nodes < total_min_nodes)
            total_min_nodes = count_nodes;

        if (flush_nodes) {
            Collections.sort(chain_of_nodes);
            for (LFHT_AnsNode c_node: chain_of_nodes){
                String string_white_spaces = String.format("%" + white_spaces + "s", "");
                System.err.println(string_white_spaces + " " + LFHT_AnsNode.class.cast(c_node).entry + " ");
            }
            long chain_max = chain_of_nodes.indexOf(chain_of_nodes.size() - 1);

            if (chain_max < partial_max_value) {
                System.err.println("ERROR chain_node.entry = " + (long) LFHT_AnsNode.class.cast(chain_node).entry +
                        "   partial_max_value = " + partial_max_value);
                System.exit(0);
            } else
                partial_max_value = chain_max;
        }
    }

    private void flush_bucket_array2(LFHT_AtomicReferenceArray curr_hash,
                                    int level,
                                    boolean flush_nodes, Map<Long, Long>  check_lfht,
                                    int white_spaces) {
        total_memory_jumps++;
        total_memory_jumps_in_hash_levels++;
        int bucket_entry = 0;
        do {
            if (flush_nodes) {
                String string_white_spaces =  String.format("%" + white_spaces + "s", "");

                System.err.println(string_white_spaces +
				   " bkt entry -> " +  bucket_entry + " [level = " +
				   level + ", entries = " + curr_hash.n_entries + ", pos = " + curr_hash.p_bits + "]");
            }
            total_buckets++;

            if (!curr_hash.isEmptyBucket(bucket_entry)) {
                Object bucket_next = curr_hash.hash[bucket_entry];
		if (IS_COMPRESSION_NODE(bucket_next)) {
		    // compression detected
                    System.err.println("ERROR - freezing/unfreezing node found");
                    System.exit(0);
		}

                if (IS_HASH(bucket_next)) {
                    flush_bucket_array2((LFHT_AtomicReferenceArray) bucket_next,
                            level + 1, flush_nodes, check_lfht, white_spaces + 3);
                } else {
                    flush_bucket_chain2(curr_hash, bucket_next, level, flush_nodes, check_lfht, white_spaces + 3);
    		    /* leaf bucket_array */
                    if (level > total_max_hash_trie_depth)
                        total_max_hash_trie_depth = level;
                    if (level < total_min_hash_trie_depth)
                        total_min_hash_trie_depth = level;
                }
            } else {
                total_empties++;
    		/* leaf bucket_array */
                if (level > total_max_hash_trie_depth)
                    total_max_hash_trie_depth = level;
                if (level < total_min_hash_trie_depth)
                    total_min_hash_trie_depth = level;
            }

        } while (++bucket_entry < curr_hash.n_entries);
        return;
    }

    /*
           flush_nodes   = true -> shows all hash levels and chain node keys
           flush_summary = true -> shows the summary of the internals of the hash map
           flush_nodes = false && flush_summary = false -> checks only the internal properties
           of the hash map. properties checked:
             - all paths end in empty buckets or chain nodes. (no situation such as bucket_1->chain->bucket_2 can happen
             - all values are ordered
     */

    public void flush_hash_statistics2(boolean flush_nodes, boolean flush_summary) {

        total_nodes_valid = 0;
        total_nodes_invalid = 0;
        total_buckets = 0;
        total_empties = 0;
        total_min_hash_trie_depth = Long.MAX_VALUE;
        total_memory_jumps = 0;
        total_memory_jumps_in_hash_levels = 0;
        total_memory_jumps_in_chain_nodes = 0;
        partial_max_value = Integer.MIN_VALUE;
        total_max_hash_trie_depth = 0;
        total_max_nodes = 0;
        total_min_nodes =  Long.MAX_VALUE;

        // ready to be used with compaction
        Map<Long, Long> check_lfth = new ConcurrentHashMap<Long, Long>();

        int white_spaces = 1;

        flush_bucket_array2(HN, 0, flush_nodes, check_lfth, white_spaces);

        if (total_min_nodes ==  Long.MAX_VALUE)
            total_min_nodes = 0;

        if (flush_summary) {
            System.err.println("-----------------------------------------------------");
            System.err.println("  Nr of memory jumps    = " + total_memory_jumps);
            System.err.println("  Nr of memory jumps in hash_levels = " + total_memory_jumps_in_hash_levels);
            System.err.println("  Nr of memory jumps in chain_nodes = " + total_memory_jumps_in_chain_nodes);
            System.err.println("  Nr of valid nodes     = " + total_nodes_valid);
            System.err.println("  Nr of invalid nodes   = " + total_nodes_invalid);
            System.err.println("  Nr of buckets         = " + total_buckets);
            System.err.println("  Nr of empty buckets   = " + total_empties);
            System.err.println("  Min hash trie depth   = " + total_min_hash_trie_depth +
                    "   (Root depth = 0)");
            System.err.println("  Max hash trie depth   = " + total_max_hash_trie_depth +
                    "   (Root depth = 0)");
            System.err.println("  Max nodes (non empty) = " + total_max_nodes +
                    "   (MAX_NODES_PER_BUCKET = " + MAX_NODES_PER_BUCKET + ")");
            System.err.println("  Min nodes (non empty) = " + total_min_nodes);
            System.err.println("  Ordered values = ok ");

            long non_empty_buckets = total_buckets - total_empties;
            if (non_empty_buckets == 0)
                System.err.println("  Avg nodes per bucket (valid + invalid) = " +
                        (float) (total_nodes_valid + total_nodes_invalid) /
                                (total_buckets) + " (non empty only) = 0.0");
            else
                System.err.println("  Avg nodes per bucket (valid + invalid) = " +
                        (float) (total_nodes_valid + total_nodes_invalid) /
                                (total_buckets) + " (non empty only) = " +
                        (float) (total_nodes_valid + total_nodes_invalid) /
                                (float) non_empty_buckets);

            System.err.println("-----------------------------------------------------");
        }
    }

    public long hash (E k) {
        long h = (Long) k;
        return h;
    }

    private boolean setChainNodeNextToNextHash(LFHT_AnsNode<E,V> chain_node, LFHT_AtomicReferenceArray next_hash) {
        Pair curr_pair;
        do {
            curr_pair = chain_node.next;
	    
            if (IS_HASH(curr_pair.reference))
                // continue the expansion for now
                return true;
            if (check_bucket_array(next_hash, chain_node.hash, chain_node.entry) != null)
                // node found- > do not continue expanding this node (false)
                return false;


            unsafe.compareAndSwapObject(chain_node, next_addr,
                    curr_pair,
                    (new Pair(next_hash, curr_pair.mark)));
        } while(true);
    }

    /****************************************************************************
     *                            API compatibility                             *
     *            (Java concurrent data structures - CHM and SkipLists)         *
     ****************************************************************************/

    public int size2() {
        total_nodes_valid = 0;
        total_nodes_invalid = 0; /* this is useless. should be always 0... */


        Map<Long, Long> check_lfht = new ConcurrentHashMap<Long, Long>();

        flush_bucket_array2(HN, 0, false, check_lfht, 1);
        if (total_nodes_invalid != 0) {
            System.out.println("ERROR INVALID NODES VISIBLE -> " + total_nodes_invalid);
            System.exit(0);
        }
        return total_nodes_valid;
    }

    public int size() {
        total_nodes_valid = 0;
        total_nodes_invalid = 0; /* this is useless. should be always 0... */

        flush_bucket_array(HN, 0, false);
        if (total_nodes_invalid != 0) {
            System.out.println("ERROR INVALID NODES VISIBLE -> " + total_nodes_invalid);
            System.exit(0);
        }
        return total_nodes_valid;
    }

    public V get(Object t) {

        if (HN == null)
            return null;
        long h = hash((E)t);

        LFHT_AnsNode <E,V> node = check_bucket_array(HN, h, (E)t);
        if (node != null)
            return node.value;
	return null;	
    }

    @Override
    public V put(E t, V v) {
        long h = hash((E)t);
        if (HN == null && unsafe.compareAndSwapObject(this, HN_addr,
                    null, (new LFHT_AtomicReferenceArray(h)))) {
            return check_insert_bucket_array(HN, h, t, v).value;
        }


        int next_p_bits = next_position_bit(h, HN.hash_val);
        if (next_p_bits <= HN.p_bits) {
            return check_insert_bucket_array(HN, h, t, v).value;
        }

        LFHT_AtomicReferenceArray new_hash = new LFHT_AtomicReferenceArray (h);
        LFHT_AtomicReferenceArray next_hash;

        do {
            next_hash = HN;
            next_p_bits = next_position_bit(h, next_hash.hash_val);
            if (next_p_bits <= next_hash.p_bits) {
                return check_insert_bucket_array(next_hash, h, t, v).value;
            } else /*next_p_bits > next_hash.p_bits */ {
                new_hash.p_bits = next_p_bits;
                new_hash.ph = null;
                int bucket = new_hash.hashEntry(next_hash.hash_val);
                new_hash.hash[bucket] = next_hash;
                if (unsafe.compareAndSwapObject(this, HN_addr, next_hash, new_hash) == true) {
                    next_hash.updatePreviousHash(new_hash);
                    return check_insert_bucket_array(HN, h, t, v).value;
                }
            }
        } while(true);
    }


    @Override
    public V remove(final Object t) {
        if (HN == null)
            return null;
        long h = hash((E)t);
	LFHT_AnsNode<E,V> res = check_delete_bucket_array(HN, h, (E)t);
	if (res != null)
	    return res.value;
        return null;
    }


    // replace is not concurrent
    @Override
    public V replace(Object t, Object v) {
	/* it is not atomic for the moment */
        if (HN == null)
            return null;
        long h = hash((E)t);
        LFHT_AnsNode <E,V> node = check_bucket_array(HN, h, (E)t);
        if (node == null)
            return null;
        V prev_value = (V) node.value;
        node.value = (V)v;
        return prev_value;
    }

    public boolean isEmpty() {
        if (this.size() == 0)
            return true;
        return false;
    }

    public void clear() {
        HN = null;
        return;
    }

    /****************************************************************************
     *                         LFHT_AnsNode                                     *
     *                         Pair (holds state and next reference)            *
     ****************************************************************************/

    static class Pair {
        final Object reference;
        final boolean mark;
        Pair(Object reference, boolean mark) {
            this.reference = reference;
            this.mark = mark;
        }
    }

    static class LFHT_AnsNode <E, V> implements Comparator<LFHT_AnsNode>, Comparable<LFHT_AnsNode> {

        // Overriding the compare method to sort the nodes
        public int compare(LFHT_AnsNode d, LFHT_AnsNode d1) {
            return d.hash.compareTo(d1.hash);
        }

        public int compareTo(LFHT_AnsNode d) {
            return this.hash.compareTo(d.hash);
        }

        public final Long hash;
        public final E entry;
        public V value;
        private Pair next;

        public LFHT_AnsNode (long h, E e, V v, Object node_next) {
            this.hash = h;
            this.entry = e;
            this.value = v;
            this.next = new Pair(node_next, true);
        }

        private Object getNext() {
            return next.reference;
        }

        public boolean valid() {
            return next.mark;
        }

        public boolean compareAndSetNext(Object expect, Object update) {
            return compareAndSet(expect, update, true, true);
        }

        private boolean compareAndSet(Object expectedReference,
                                      Object newReference,
                                      boolean expectedMark,
                                      boolean newMark) {
            Pair current_pair = next;
            return
                    expectedReference == current_pair.reference &&
                            expectedMark == current_pair.mark &&
                            ((newReference == current_pair.reference &&
                                    newMark == current_pair.mark) ||
                                    unsafe.compareAndSwapObject(this, next_addr,
                                            current_pair,
                                            (new Pair(newReference, newMark))));
	    }


        public boolean equals(long h, E t) {
            if (h == hash && entry.equals(t) && valid())
                return true;
            return false;
        }

        public boolean markAsInvalid() {
            Object node_next = next.reference;

            while (!compareAndSet(node_next, node_next, true, false)) {
                if (!valid())
                    return false;
                node_next = next.reference;
            }
            return true;
        }
    }

    /****************************************************************************
     *                         LFHT_AtomicReferenceArray                        *
     ****************************************************************************/

    static class LFHT_AtomicReferenceArray {
        final Object [] hash;               // hash bucket of entries
        final long hash_val;                // integer value representing all of the hash values in the level
        final int n_entries;                // number of entries
        final int n_bits;                   // number of bits
        int p_bits;                         // base position of bits
	int c_bucket;                       // bucket index that triggers the next compression
        LFHT_AtomicReferenceArray ph;       // previous hash level


        // constructor for the root node
        public LFHT_AtomicReferenceArray (long hash_r) {
            hash_val = hash_r;
            n_entries = 1 << N_BITS;
            n_bits = N_BITS;
            ph = null;
            p_bits = next_position_bit(hash_r);
	    c_bucket = this.hashEntry(hash_val);
            hash = new Object[n_entries];
            for (int i = 0; i < n_entries; i++)
                hash[i] = this;
        }

        // constructor to expand on the lower-level bits
        // collision occurred in a lower bit -> insert on front...
        public LFHT_AtomicReferenceArray (long h_v, int next_p_bits, LFHT_AtomicReferenceArray p) {
            hash_val = h_v;
            n_entries = 1 << N_BITS;
            n_bits = N_BITS;
            ph = p;
            p_bits = next_p_bits;
	    c_bucket = this.hashEntry(hash_val);
            hash = new Object[n_entries];
            for (int i = 0; i < n_entries; i++)
                hash[i] = this;
        }

        final boolean compareAndSet(int i, Object expect, Object update) {
            long raw_index = base + i * scale;
            return unsafe.compareAndSwapObject(hash, raw_index, expect, update);
        }

        final LFHT_AtomicReferenceArray jumpToPreviousHash(LFHT_AtomicReferenceArray stop_hash, long h) {
            int next_p_bits;
            int bucket = stop_hash.hashEntry(h);
            if (stop_hash.hash[bucket]  instanceof LFHT_AtomicReferenceArray) {
                // stop_hash.hash[bucket] is a hash
                LFHT_AtomicReferenceArray next_hash = (LFHT_AtomicReferenceArray) stop_hash.hash[bucket];
                next_p_bits = next_position_bit(h, next_hash.hash_val);
                if (next_p_bits <= next_hash.p_bits) {
                    return next_hash;
                }
                return stop_hash;
            }

            LFHT_AtomicReferenceArray prev_hash;

            prev_hash = this;

            next_p_bits = next_position_bit(h, prev_hash.hash_val);
            while (next_p_bits > prev_hash.p_bits) {
                prev_hash = prev_hash.ph;
                next_p_bits = next_position_bit(h, prev_hash.hash_val);
            }
            return prev_hash;
        }

	
	private boolean triggerCompression() {	    
	    if ((hash[c_bucket] == this))
		return true;
	    return false;
	}


        private boolean isEmptyBucket(int bucket_pos) {

            if ((hash[bucket_pos] == this)) {
                return true;
            } else
                return false;
        }

        private int hashEntry(long value) {
            return (int)((value >> p_bits) & (n_entries - 1));
        }

        private void updatePreviousHash(LFHT_AtomicReferenceArray new_hash) {
            LFHT_AtomicReferenceArray next_hash_ph;
            do {
                next_hash_ph = ph;
                if (next_hash_ph != null && next_hash_ph.p_bits < new_hash.p_bits)
                    return;
            } while (!unsafe.compareAndSwapObject(this, prev_hash_addr, next_hash_ph, new_hash));
        }
	
        private void updateBucketToNextHash(int bucket, LFHT_AtomicReferenceArray new_hash) {
            Object bucket_next = this.hash[bucket];
	    if (IS_COMPRESSION_NODE(bucket_next)) {
		// compression detected
		return;
	    }

            if ((bucket_next instanceof LFHT_AnsNode))
                if (compareAndSet(bucket, bucket_next, new_hash) == true)
                    return;
            LFHT_AtomicReferenceArray next_hash = (LFHT_AtomicReferenceArray) this.hash[bucket];
	    if (IS_COMPRESSION_NODE(next_hash)) {
		// compression detected
		return;
	    }

            if (next_hash.p_bits < new_hash.p_bits) {
                do {
                    if (compareAndSet(bucket, bucket_next, new_hash) == true)
                        return;
                    next_hash = (LFHT_AtomicReferenceArray) this.hash[bucket];
		    if (IS_COMPRESSION_NODE(next_hash)) {
			// compression detected
			return;
		    }

                } while(next_hash.p_bits < new_hash.p_bits);
            }
            return;
        }
    }


    /****************************************************************************
     *                           show graphic tree                              *
     *                           (non concurrent)                               *
     ****************************************************************************/

    private void gt_flush_bucket_chain(DefaultMutableTreeNode gt_root,
				       Object chain_node,
				       int count_nodes,
				       int level,
				       boolean flush_nodes) {
        if (IS_HASH(chain_node)) {
            if (count_nodes > total_max_nodes)
                total_max_nodes = count_nodes;
            if (count_nodes < total_min_nodes)
                total_min_nodes = count_nodes;
            return;
        }
        if (IS_VALID_ENTRY(LFHT_AnsNode.class.cast(chain_node)))
            total_nodes_valid++;
        else
            total_nodes_invalid++;

	DefaultMutableTreeNode next_root =
	    new DefaultMutableTreeNode(LFHT_AnsNode.class.cast(chain_node).entry, false);
	gt_root.add(next_root);
	if (flush_nodes) {
	    System.err.println(" " + LFHT_AnsNode.class.cast(chain_node).entry + " ");
	}
        gt_flush_bucket_chain(gt_root, LFHT_AnsNode.class.cast(chain_node).getNext(),
			      count_nodes + 1, level, flush_nodes);
        return;
    }

    private void gt_flush_bucket_array(DefaultMutableTreeNode gt_root,
				       LFHT_AtomicReferenceArray curr_hash,
				       int level,
				       boolean flush_nodes) {
        int bucket_entry = 0;
	int empty_hash_levels = 0;
        do {
	    DefaultMutableTreeNode next_root = new DefaultMutableTreeNode(bucket_entry,true);
	    gt_root.add(next_root);
	    if (flush_nodes) {		
		System.err.println("\n bkt entry -> " +
				   bucket_entry + " (level = " +
				   level + ", entries = " +
				   curr_hash.n_entries + ", pos = " +
				   curr_hash.p_bits + ")");
	    }
            total_buckets++;

            if (!curr_hash.isEmptyBucket(bucket_entry)) {
                Object bucket_next = curr_hash.hash[bucket_entry];
		if (IS_COMPRESSION_NODE(bucket_next)) {
		    // compression detected
                    System.err.println("ERROR - freezing/unfreezing node found");
                    System.exit(0);
		}

                if (IS_HASH(bucket_next))
                    gt_flush_bucket_array(next_root,
					  (LFHT_AtomicReferenceArray) bucket_next,
					  level + 1, flush_nodes);
                else {
                    gt_flush_bucket_chain(next_root,
					  bucket_next, 0, level, flush_nodes);
    		    /* leaf bucket_array */
                    if (level > total_max_hash_trie_depth)
                        total_max_hash_trie_depth = level;
                    if (level < total_min_hash_trie_depth)
                        total_min_hash_trie_depth = level;
                }
                if (flush_nodes)
                    System.err.println("");
            } else {
                total_empties++;
		empty_hash_levels++;
    		/* leaf bucket_array */
                if (level > total_max_hash_trie_depth)
                    total_max_hash_trie_depth = level;
                if (level < total_min_hash_trie_depth)
                    total_min_hash_trie_depth = level;
            }

        } while (++bucket_entry < curr_hash.n_entries);
	if (empty_hash_levels == curr_hash.n_entries)	    
	    total_empty_hash_levels++;

        return;
    }

    public void gt_flush_hash_statistics(DefaultMutableTreeNode gt_root,
					 boolean flush_nodes) {

        total_nodes_valid = 0;
        total_nodes_invalid = 0;
        total_buckets = 0;
        total_empties = 0;
        total_min_hash_trie_depth = Long.MAX_VALUE;
        total_max_hash_trie_depth = 0;
        total_max_nodes = 0;
        total_min_nodes =  Long.MAX_VALUE;
	total_empty_hash_levels = 0;

        gt_flush_bucket_array(gt_root, HN, 0, flush_nodes);

        if (total_min_nodes ==  Long.MAX_VALUE)
            total_min_nodes = 0;

        System.out.println("-----------------------------------------------------");
	// System.out.println(" Nr of hashes used         = " + number_hashes()); 
	// System.out.println(" Nr of compressions tried  = " + number_compressions_tries()); 
	// System.out.println(" Nr of compressions done   = " + number_compressions_ok()); 
	// System.out.println(" Nr of compressions failed = " + number_compressions_failed()); 
        System.out.println(" Nr of valid nodes         = " + total_nodes_valid);
        //System.out.println(" Nr of invalid nodes       = " + total_nodes_invalid);
	System.out.println(" Nr of empty hash levels   = " + total_empty_hash_levels);
        System.out.println(" Nr of buckets             = " + total_buckets);
        System.out.println(" Nr of empty buckets       = " + total_empties);
        System.out.println(" Min hash trie depth       = " + total_min_hash_trie_depth +
                "   (Root depth = 0)");
        System.out.println(" Max hash trie depth       = " + total_max_hash_trie_depth +
                "   (Root depth = 0)");
        System.out.println(" Max nodes (non empty)     = " + total_max_nodes +
                "   (MAX_NODES_PER_BUCKET = " + MAX_NODES_PER_BUCKET + ")");
        System.out.println(" Min nodes (non empty)     = " + total_min_nodes);
        long non_empty_buckets = total_buckets - total_empties;
        if (non_empty_buckets == 0)
            System.out.println(" Avg nodes per bucket (valid + invalid) = " +
                    (float)(total_nodes_valid + total_nodes_invalid) /
                            (total_buckets) + " (non empty only) = 0.0");
        else
            System.out.println(" Avg nodes per bucket (valid + invalid) = " +
                    (float)(total_nodes_valid + total_nodes_invalid) /
                            (total_buckets) + " (non empty only) = " +
                    (float) (total_nodes_valid + total_nodes_invalid) /
                            (float) non_empty_buckets);

        System.out.println("-----------------------------------------------------");
    }

    // flush_nodes == true -> flushes nodes on terminal + statistics
    // flush_nodes == false -> flushes only statistics on terminal
    public void gt_show() {
	gt_show(false);	
    }
    
    public void gt_show(boolean flush_nodes) {
	FFPE lfht = this;
	SwingUtilities.invokeLater(new Runnable() {
		@Override
		public void run() {
		    new GraphicTree(lfht, flush_nodes);
		}
	    });
    }
        
    /****************************************************************************
     *                         Graphic tree                                     *
     ****************************************************************************/
    
    static class GraphicTree extends JFrame  {

	private JTree tree;
	private JLabel selectedLabel;
	public GraphicTree(FFPE lfht, boolean flush_nodes) {    
	    DefaultMutableTreeNode root = new DefaultMutableTreeNode("Root", true);
	    lfht.gt_flush_hash_statistics(root, flush_nodes);
	    tree = new JTree(root);
	    
	    DefaultTreeCellRenderer renderer = (DefaultTreeCellRenderer) tree.getCellRenderer();
	    //tree.setRootVisible(false);
	    tree.setRootVisible(true);
	    tree.getSelectionModel().addTreeSelectionListener(new TreeSelectionListener() {
		    @Override
		    public void valueChanged(TreeSelectionEvent e) {
			selectedLabel.setText(e.getPath().toString());
		    }
		});
  
	    tree.setCellRenderer(new MyTreeCellRenderer());
	    
	    this.add(createTopPanel(tree), BorderLayout.NORTH);	    	    
	    this.setLocationRelativeTo(null);

	    this.add(tree);
	    this.add(new JScrollPane(tree));
	    
	    this.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
	    
	    this.setTitle("JTree Example");
	    this.pack();
	    this.setVisible(true);
	    this.setSize(400, 400);
	    selectedLabel = new JLabel();
	    add(selectedLabel, BorderLayout.SOUTH);
	    
	}

	private static JComponent createTopPanel(JTree tree) {
	    JPanel panel = new JPanel();
	    JButton expandBtn = new JButton("Expand All");
	    expandBtn.addActionListener(ae-> {
		    JTreeUtil.setTreeExpandedState(tree, true);
		});
	    panel.add(expandBtn);
	    JButton collapseBtn = new JButton("Collapse All");
	    collapseBtn.addActionListener(ae-> {JTreeUtil.setTreeExpandedState(tree, false);});
	    panel.add(collapseBtn);
	    return panel;
	}

	// render with special icons
	private static class MyTreeCellRenderer extends DefaultTreeCellRenderer {
	    
	    @Override
	    public Component getTreeCellRendererComponent(JTree tree, Object value, boolean sel, boolean expanded, boolean leaf, int row, boolean hasFocus) {
		super.getTreeCellRendererComponent(tree, value, sel, expanded, leaf, row, hasFocus);
		// decide what icons you want by examining the node
		if (value instanceof DefaultMutableTreeNode) {
		    DefaultMutableTreeNode node = (DefaultMutableTreeNode) value;
		    
		    if (node.getAllowsChildren()) {
			// bucket entry
			if (node.isLeaf())
			    // empty bucket entry
			    setIcon(UIManager.getIcon("FileChooser.upFolderIcon"));
		    } else
			// chain nodes
			setIcon(UIManager.getIcon("Tree.leafIcon"));
		}
		return this;
	    }
	}

	// create expand and contract all nodes
	private static class JTreeUtil {
	    public static void setTreeExpandedState(JTree tree, boolean expanded) {
		DefaultMutableTreeNode node = (DefaultMutableTreeNode) tree.getModel().getRoot();
		setNodeExpandedState(tree, node, expanded);
	    }
	    
	    public static void setNodeExpandedState(JTree tree, DefaultMutableTreeNode node, boolean expanded) {
		ArrayList<DefaultMutableTreeNode> list = Collections.list((Enumeration)node.children());
		for (DefaultMutableTreeNode treeNode : list) {
		    setNodeExpandedState(tree, treeNode, expanded);
		}
		if (!expanded && node.isRoot()) {
		    return;
		}
		TreePath path = new TreePath(node.getPath());
		if (expanded) {
		    tree.expandPath(path);
		} else {
		    tree.collapsePath(path);
		}
	    }
	}
	
    }


    /****************************************************************************
     *                            stuff to support:                             *
     *             'extends AbstractMap<E, V> implements ConcurrentMap<E,V>     *
     *                           (non concurrent)                               *
     ****************************************************************************/

    @Override
    public Set<Map.Entry<E, V>> entrySet () {
	System.out.println("'entrySet' is under construction...");
	return null;
    }


  @Override
  public boolean remove (Object k, Object v) {
      System.out.println("remove (Object k, Object v) -> check this semantics");
      if (remove(k) != null)
	  return true;
      return false;
  }

    @Override
    public boolean replace (E k, V oldvalue, V newvalue) {
	System.out.println("replace(E k, V oldvalue, V newvalue) ->check this semantics");
	if (replace(k, newvalue) != null)
	    return true;
	return false;
    }

    @Override
    public V putIfAbsent (Object k, Object v) {
      return null;
   }
}
