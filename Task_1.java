package objective;

import core.*;

import java.util.List;
import java.util.*;
import java.util.regex.*;
import java.io.*;
import java.util.concurrent.*;
import java.util.HashMap;
/**
 * Assignment:
 * Counts the number of flights from each airport.
 * A multi-threaded solution which creates a mapper for the input file and a combiner for each airport and a reducer for each flight,
 * each processed sequentially. Also error checks and corrects file input. Outputs results to screen but also writes as CSV file
 * to Task_1_Output.csv in current directory for input to charting module.
 *
 * To run:
 * java Task_1 <files>
 *     i.e. java Task_1.java Top30_airports_LatLong.csv AComp_Passenger_data.csv
 * Graphical Output to be added
 * 
 * 
 * 
 * 
 *  
 */
class Task_1
    {
    // Configure and set-up the job using command line arguments specifying input files and job-specific mapper and
    // reducer functions
    private static AirportList aList=new AirportList(30);
    private static PassengerList pList=new PassengerList();
    public static void main(String[] args) throws Exception {
        ReadAndErrorCheck.run(args);
        aList=ReadAndErrorCheck.getAList();
        pList=ReadAndErrorCheck.getPList();
        Config config = new Config(mapper.class, reducer.class, combiner.class);
        Job job = new Job(config,pList);
        job.run();
        DisplayandWriteTotals(aList,Job.getMap());
    }
    
    // Print out total flights from each airport and unused airports as obtained from map
    // and also write out results as a CSV file
    // https://stackabuse.com/reading-and-writing-csvs-in-java/
    private static void DisplayandWriteTotals(AirportList AirportListIn, ConcurrentHashMap mapIn){
        try{
            FileWriter csvWriter = new FileWriter("Task_1_Output.csv");
            int total=0;
            System.out.println("*** Used Airports ***");
            for (String apCode: AirportListIn.getKeys()){
                if (mapIn.containsKey(apCode)){
                    total=(int) mapIn.get(apCode);
                    System.out.format("Used Airport:  %-18s Total Flights: %3d\n",AirportListIn.getName(apCode),total);
                    csvWriter.append(AirportListIn.getName(apCode)+","+Integer.toString(total)+"\n");
                }
            }
            System.out.println("*** Unused Airports ***");
            for (String apCode: AirportListIn.getKeys()){
                if (!mapIn.containsKey(apCode)){
                    System.out.format("Unused Airport:%-18s\n",AirportListIn.getName(apCode));
                    csvWriter.append(AirportListIn.getName(apCode)+","+"0"+"\n");
                }
            }
            csvWriter.flush();
            csvWriter.close();
        }
        catch (IOException e){
            System.out.println("IO Exception");
            e.printStackTrace();
        }
    }
    // FromAirportCode+Flightid count mapper:
    // Output a one for each occurrence of FromAirportCode+Flightid.
    // KEY = FromAirportCode+Flightid
    // VALUE = 1
    public static class mapper extends Mapper {
        public void map(String line) {
            String[] Fields=line.split(",");
            EmitIntermediate((Fields[2]+Fields[1]),1);
        }
    }
    // Airport Code count combiner:
    // Output the total number of occurrences of each unique Aiport Code
    // KEY = FromAirport Code
    // VALUE = count
    public static class combiner extends Combiner {
        public void combine(Object key, List values) {
            EmitIntermediate3(key.toString().substring(0,3), values);
        }
    }
    // Airport Code count reducer:
    // Output the total number of occurrences of each unique Aiport Code
    // KEY = FromAirport Code
    // VALUE = count
    public static class reducer extends Reducer {
        public void reduce(Object key, List values) {
            int count = 0;
            for (Object lst : values){
                for (Object value : (List) lst) count += (int) value;
                Emit(key, count);
            }
        }
    }
}