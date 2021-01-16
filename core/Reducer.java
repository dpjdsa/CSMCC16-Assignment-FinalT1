package core;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.*;

/**
 * An abstract Reducer class defining multi-threaded reduce functionality
 *
 * Aspects:
 * - Implements runnable interface for execution as a thread
 * - Uses thread-safe alternative data structures (ConcurrentHashMap)
 */
public abstract class Reducer {
    public static final int NUM_REDUCE_THREADS = 5; 
    // Intermediate records for this reducer instance to process
    protected Map records;

    // Default constructor
    public Reducer() {}

    // Setters
    public void setRecords(Map records) {
        this.records = records;
    }

    // Execute the reduce function for each key-value pair in the intermediate results output by the combiner
    public void run() {
        int num_reduce_threads=NUM_REDUCE_THREADS;
        int numRecords=records.size();
        // The portion of the file for each Reduce Thread is a chunk
        int chunk_size = numRecords / num_reduce_threads;
        int records_remaining = numRecords;
        // Define list to hold each chunk of input to be reduced
        List<ConcurrentHashMap> reduce_chunks = new ArrayList<ConcurrentHashMap> (num_reduce_threads);
        // Define iterator to operate on each line of input;
        Iterator iterator = records.entrySet().iterator();
        // Split up the records into chunks for each reduce thread to process.
        int current_reduce_thread = 0;
        while (records_remaining > 0)
        {
            // Catch the last reduce thread case
            if (current_reduce_thread == num_reduce_threads-1)
            {
                chunk_size = records_remaining;
            }

            // Add chunk for this thread to the Reduce array
            ConcurrentHashMap reduce_thread_record = new ConcurrentHashMap (chunk_size);
            reduce_chunks.add (reduce_thread_record);

            for (int recordno = 0; recordno < chunk_size; recordno++)
            {
              Map.Entry<Object, List<Object>> entry = (Map.Entry) iterator.next();
              reduce_thread_record.put(entry.getKey(),entry.getValue());
            }
            ++current_reduce_thread;
            records_remaining -= chunk_size;
        }
        // Now kick off the reduce threads
        Thread [] reduce_threads = new Thread [num_reduce_threads];
        System.out.println("Starting "+num_reduce_threads+" Reduce Threads.");

        for (int i = 0; i < num_reduce_threads; i++)
        {
            RunnableThread reducer = new RunnableThread (reduce_chunks.get(i));
            reduce_threads [i] = new Thread (reducer);
            reduce_threads [i].start ();
        }

        // Now wait for all the threads to complete
        try
            {
            for (int i = 0; i < num_reduce_threads; i++)
                {
                    reduce_threads [i].join ();
                }
            }
        catch (InterruptedException e)
        {
        }
    }

    // Abstract reduce function to the overwritten by objective-specific class
    public abstract void reduce(Object key, List values);

    // Simply replace the intermediate and final result for each key
    // Map <KEY, List<VALUES>> -> Map <KEY, VALUE>
    public void Emit(Object key, Object value) {
        Job.map1.put(key, value);
    }
    // Runnable Thread which iterates through each element of input and calls reduce method for it.
    private class RunnableThread implements Runnable {
        private volatile ConcurrentHashMap<String,Object>  recordList;
        public RunnableThread(ConcurrentHashMap recordIn){
            this.recordList=recordIn;
        }
        public void run()
        {
            for (Map.Entry<String,Object> entry : recordList.entrySet()){
                String key=entry.getKey();
                List value=(List) entry.getValue();
                reduce(key,value);
            }
            System.out.println("Executing run method in Reducer Thread");
        }
    }
}