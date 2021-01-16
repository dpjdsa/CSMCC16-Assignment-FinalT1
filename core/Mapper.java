package core;

import java.io.File;
import java.io.IOException;
import java.util.*;

/**
 * An an abstract Mapper class defining multi-threaded map functionality
 *
 *  - Number of Map threads defined by NUM_MAP_THREADS
 *  - Operates on portions of the input in parallel rather than on entire files.
 *  - Implements the runnable interface for execution as a thread
 *  - Replaces operations on the global map object with thread-safe alternatives
 */

//public abstract class Mapper {
public abstract class Mapper{
    public static final int NUM_MAP_THREADS = 10; 
    // The input file for this mapper to process
    protected File file;
    protected PassengerList pList;

    // Default constructor
    public Mapper() {}

    // Set the input file for the given instance
    public void setPList(PassengerList pListIn) {
        this.pList = pListIn;
    }

    // Execute the map function for each line of the provided file
    public void run() throws IOException {
        int num_map_threads=NUM_MAP_THREADS;
        int numRecords=0;
        // Read the file and get the size of it
        try {
            numRecords = Config.read(pList);
            if (num_map_threads>numRecords)
                num_map_threads=numRecords;
        } catch (Exception e){
            e.printStackTrace();
        }
        // The portion of the input file for each Map thread is a chunk
        int chunk_size = numRecords / num_map_threads;
        int records_remaining = numRecords;
        // Define the List to hold each chunk of the input to be mapped
        List<ArrayList<String>> map_chunks = new ArrayList<ArrayList<String>> (num_map_threads);
        // Define iterator to iterate on each record of the input
        Iterator<String> readIn = Job.record.iterator(); 
        // Split up the records into chunks for each map thread to process.
        int current_map_thread = 0;
        while (records_remaining > 0)
        {
            // Catch the last map thread case
            if (current_map_thread == num_map_threads-1)
            {
                chunk_size = records_remaining;
            }

            // add chunk for this thread to the map array
            ArrayList<String> map_thread_record = new ArrayList<String> (chunk_size);
            map_chunks.add (map_thread_record);
            for (int recordno = 0; recordno < chunk_size; recordno++)
            {
              map_thread_record.add(readIn.next());
            }
            ++current_map_thread;
            records_remaining -= chunk_size;
        }

        // Now kick off the map threads
        Thread [] map_threads = new Thread [num_map_threads];

        System.out.println("Starting "+num_map_threads+" map threads.");
        for (int i = 0; i < num_map_threads; i++)
        {
            RunnableThread mapper = new RunnableThread (map_chunks.get(i));
            map_threads [i] = new Thread (mapper);
            map_threads [i].start ();
        }

        // Now wait for all the threads to complete
        try
            {
            for (int i = 0; i < num_map_threads; i++)
                {
                    map_threads [i].join ();
                }
            }
        catch (InterruptedException e)
        {
        }
    }
    
    // Abstract map function to be overwritten by objective-specific class.
    public abstract void map(String value);

    // Adds values to a list determined by a key
    // Map<KEY, List<VALUES>>
    public void EmitIntermediate(Object key, Object value) {
        // Only add the key value pair if it doesn't already exist in the list.
        List values;
        if(!Job.map.containsKey(key)) {
            values = new ArrayList<>();
            Job.map.put(key, values);
            // Add the new value to the list.
            values.add(value);
        }
    }
    // Runnable Thread which iterates through each element of input and calls map method for it.
    private class RunnableThread implements Runnable {
        private volatile ArrayList  recordList;
        public RunnableThread(ArrayList recordIn){
            this.recordList=recordIn;
        }
        public void run()
        {
            for (int i=0;i<recordList.size();i++){
                map((String) recordList.get(i));
            }
            System.out.println("Executing run method in Mapper Thread");
        }
    }
}