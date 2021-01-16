package core;

import java.io.*;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.*;
import java.util.concurrent.*;

//import src.objective.Passenger;
//import src.objective.PassengerList;
//import objective.Passenger;
//import objective.PassengerList;

/**
 * MapReduce Job Configuration
 * Stores the file specifications provided at run-time and
 * uses reflection to set objective-specific mapper and reducer classes.
 *
 * Areas for improvement:
 * - Output to file or implement a user interface to display results
 */
public class Config {
    // Input files to process
    private File[] files;

    // Classes to implement job-specific map and reduce functions
    private Class mapper, reducer, combiner;

    // Constructor
    //public Config(String[] args, Class mapper, Class reducer, Class combiner) {
    public Config(Class mapper, Class reducer, Class combiner) {
        //init(args);
        this.mapper = mapper;
        this.reducer = reducer;
        this.combiner = combiner;
    }

    // Initialise a job using the provided arguments
    /*private void init(String[] args) {
        if(args == null || args.length == 0) {
            System.out.println("Usage: java MapReduce <files>\n\tProcess a set of files listed by <files> using a trivial MapReduce implementation.");
            System.exit(1);
        }
        this.files = new File[args.length];
        for(int i=0; i<args.length; i++)
            this.files[i] = new File(args[i]);
    }
    */
    /* Generic file reader returning an iterator cycling through each line of the specified file
    protected static Iterator read(File file) throws IOException {
        List record = new ArrayList();
        BufferedReader br = new BufferedReader(new FileReader(file));
        String line;
        while((line = br.readLine()) != null)
            record.add(line);
        br.close();
        System.out.println("Size of file ="+record.size());
        return record.iterator();
    }
    */
    /* Generic file reader returning a list containing each line of input file
    protected static int read(File file) throws IOException {
    //List record = new ArrayList();
    BufferedReader br = new BufferedReader(new FileReader(file));
    String line;
    while((line = br.readLine()) != null)
        Job.record.add(line);
    br.close();
    System.out.println("Size of file ="+Job.record.size());
    return Job.record.size();
    }
    */
    // Reads the Passenger List as if it was a file
    protected static int read(PassengerList pListIn){
        String line;
        for (int i=0;i<pListIn.size();i++){
            Passenger passenger=pListIn.getPassenger(i);
            line=passenger.toCSV();
            Job.record.add(line);
        }
        System.out.println("Size of file ="+Job.record.size());
        return Job.record.size();
    }
    // Return the list of files to process
    protected File[] getFiles() {
        return this.files;
    }

    
    // Using reflection get an instance of the mapper operating on a specified file
    //protected Mapper getMapperInstance(File file) throws Exception {
    protected Mapper getMapperInstance() throws Exception {
        Mapper mapper = (Mapper) this.mapper.getConstructor().newInstance();
        //mapper.setFile(file);
        return mapper;
    }

    // Using reflection get an instance of the reducer operating on a chunk of the intermediate results
    protected Reducer getReducerInstance(ConcurrentHashMap results) throws Exception {
        Reducer reducer = (Reducer) this.reducer.getConstructor().newInstance();
        reducer.setRecords(results);
        return reducer;
    }
    // Using reflection get an instance of the combiner operating on a chunk of the intermediate results
    protected Combiner getCombinerInstance(ConcurrentHashMap results) throws Exception {
        Combiner combiner = (Combiner) this.combiner.getConstructor().newInstance();
        combiner.setRecords(results);
        return combiner;
    }
}