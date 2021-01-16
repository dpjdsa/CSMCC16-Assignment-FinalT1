package core;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.*;

/**
 * An abstract Combiner class defining multi-threaded combine functionality
 *
 * - Number of Combine Threads defined by NUM_COMBINE_THREADS.
 * - Splits inputs across threads and then executes threads in parallel.
 * - Implements runnable interface for execution as a thread.
 * - Uses thread-safe alternative data structures.
 */
public abstract class Combiner {
    public static final int NUM_COMBINE_THREADS = 12; 
    // Intermediate records for this combiner instance to process
    protected ConcurrentHashMap records;

    // Default constructor
    public Combiner() {}

    // Setters
    public void setRecords(ConcurrentHashMap records) {
        this.records = records;
    }

    // Execute the reduce function for each key-value pair in the intermediate results output by the mapper
    public void run() {
        int num_combine_threads=NUM_COMBINE_THREADS;
        int numRecords=records.size();
        // The portion of the file for each Combine thread is a chunk
        int chunk_size = numRecords / num_combine_threads;
        int records_remaining = numRecords;
        // Define the list to hold each chunk of map to be combined
        List<ConcurrentHashMap> combine_chunks = new ArrayList<ConcurrentHashMap> (num_combine_threads);
        // Define iterator to operate on each line of the input;
        Iterator iterator = records.entrySet().iterator();
        // Split up the records into chunks for each combine thread to process.
        int current_combine_thread = 0;
        while (records_remaining > 0)
        {
            // Catch the last combine thread case
            if (current_combine_thread==num_combine_threads-1)
            {
                chunk_size = records_remaining;
            }
            // Add chunk for this thread to the Combine array
            ConcurrentHashMap combine_thread_record = new ConcurrentHashMap (chunk_size);
            combine_chunks.add (combine_thread_record);

            for (int recordno = 0; recordno < chunk_size; recordno++)
            {
              Map.Entry<Object, List<Object>> entry = (Map.Entry) iterator.next();
              combine_thread_record.put(entry.getKey(),entry.getValue());
            }
            ++current_combine_thread;
            records_remaining -= chunk_size;
        }
        // Now kick off the combine threads
        Thread [] combine_threads = new Thread [num_combine_threads];

        System.out.println("Starting "+num_combine_threads+" combine threads.");
        for (int i = 0; i < num_combine_threads; i++)
        {
            RunnableThread combiner = new RunnableThread (combine_chunks.get(i));
            combine_threads [i] = new Thread (combiner);
            combine_threads [i].start ();
        }

        // Now wait for all the threads to complete
        try
            {
            for (int i = 0; i < num_combine_threads; i++)
                {
                    combine_threads [i].join ();
                }
            }
        catch (InterruptedException e)
        {
        }
    }

    // Abstract reduce function to the overwritten by objective-specific class
    public abstract void combine(Object key, List values);
    
    // Add the value to the existing values linked to the observed key else create 
    //a new map entry with an empty list and add value.
    public void EmitIntermediate3(Object key, Object value) {
        List values;
        if(Job.map1.containsKey(key)) {
            values = (List) Job.map1.get(key);
        } else {
            values = new ArrayList<>();
            Job.map1.put(key, values);       
        }
        // Add the new value to the list
        values.add(value);        
    }
    // Runnable Thread which iterates through each element of input and calls combine method for it.
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
                combine(key,value);
            }
            System.out.println("Executing run method in Combiner Thread");
        }
    }
}
