package core;

import java.util.List;
import java.util.*;
import java.util.regex.*;
import java.io.*;
import java.nio.file.Files;
import java.util.concurrent.*;
import java.util.HashMap;
/**
 * Assignment:
 * Reading, Error Checking and Correcting input file
 * Reads in Airport list and Passenger record file.
 * Assumes airport list is error free but corrects passenger record fields
 * which have incorrect formats based on finding lowest Levensthein
 * score to fields that have already been scanned correctly.
 * 
 *
 */
public class ReadAndErrorCheck
    {
    private File[] files;
    private static PassengerList pList=new PassengerList();
    private static PassengerList bpList=new PassengerList();
    private static HashMap fltCodeList=new HashMap<String,Integer>();
    // Set up to read airport and passenger lists
    private static AirportList aList=new AirportList(30);
    public static AirportList getAList(){
        return aList;
    }
    public static PassengerList getPList(){
        return pList;
    }
    //Main method to read in the Airports and the Passenger List
    public static void run(String[] args) throws Exception {
        if(args == null || args.length != 2) {
            System.out.println("Two Input files need to be supplied: Please supply Airport List and Passenger List Files");
            System.exit(1);
        }
        ReadAirports(args[0]);
        ReadPassengers(args[1]);
    }
    // Read Airports in from CSV file and create list of airports
    public static void ReadAirports(String FileName)
    {
        BufferedReader br = null;
        String line = "";
        try {
                br = new BufferedReader(new FileReader(FileName));
                while((line=br.readLine())!=null){
                    if (line.length()>0){
                        String[] Field = line.split(",");
                        String name=Field[0];
                        String code=Field[1];
                        double lat=Double.parseDouble(Field[2]);
                        double lon=Double.parseDouble(Field[3]);
                        Airport airport = new Airport(name,code,lat,lon);
                        aList.addAirport(airport);
                    }
                }
                br.close();
        } catch (IOException e) {
            System.out.println("IO Exception");
            e.printStackTrace();
        }
        System.out.println(aList);
        System.out.println("*** no of airports is: "+aList.size());
    }
    // Read passenger list from CSV file
    public static void ReadPassengers(String FileName)
    {
        BufferedReader br = null;
        String line = "";
        int linecount=0;       
        HashSet<String> PassIds = new HashSet<String>();
        HashSet<String> FltIds = new HashSet<String>();
        // Java Regex from https://www.javatpoint.com/java-regex accessed on 6/1/21 at 13.30
        try {
                br = new BufferedReader(new FileReader(FileName));
                while((line=br.readLine())!=null){
                    if (line.length()>0){
                        linecount++;
                        System.out.print("\nLine: "+ linecount+" Data: "+line);
                        String[] Field = line.split(",");
                        String PassId=Field[0];
                        String FltId=Field[1];
                        String FrmApt=Field[2];
                        String DstApt=Field[3];                        
                        boolean Okflg=true;
                        // Check for lines containing missing fields
                        if ((PassId.length()==0)&&(FltId.length()==0)&&(FrmApt.length()==0)
                                &&(DstApt.length()==0)){
                            System.out.println("Empty Line");
                        // otherwise check each field matches permitted format, if so add it to set of permitted values
                        // If not reset flag to say this line is not OK.
                        } else {
                            if (!Pattern.matches("[A-Z]{3}[0-9]{4}[A-Z]{2}[0-9]",PassId)){
                                System.out.print("\n** Incorrect Passenger ID: "+PassId);
                                Okflg=false;
                            } else {
                                PassIds.add(PassId);
                            }
                            if (!Pattern.matches("[A-Z]{3}[0-9]{4}[A-Z]",FltId)){
                                System.out.print("\n** Incorrect Flight ID: "+FltId);
                                Okflg=false;
                            } 
                            if ((!Pattern.matches("[A-Z]{3}",FrmApt))||(!(aList.validCode(FrmApt)))){
                                System.out.print("\n** Incorrect Origin Code: "+FrmApt);
                                Okflg=false;
                            } 
                            
                            if ((!Pattern.matches("[A-Z]{3}",DstApt))||(!(aList.validCode(DstApt)))){
                                System.out.print("\n** Incorrect Destination Code: "+DstApt);
                                Okflg=false;
                            }
                            
                            if (!Pattern.matches("[0-9]{10}",Field[4])){
                                System.out.print("\n** Incorrect Departure Time: "+Field[4]);
                                Okflg=false;
                            }
                            
                            if (!Pattern.matches("[0-9]{1,4}",Field[5])){
                                System.out.print("\n** Incorrect Flight Time: "+Field[5]);
                                Okflg=false;
                            }
                            // Create the departure time and flight time fields
                            double DepTime=Double.parseDouble(Field[4]);
                            double FltTime=Double.parseDouble(Field[5]);
                            // Create the passenger and add it to the good list or the bad list depending on flag
                            Passenger passenger = new Passenger(PassId,FltId,FrmApt,DstApt,DepTime,FltTime);
                            // Additional Code to trap cases where Flight ID has correct format but is a
                            // corruption of a Flight ID that has already been received at least once and
                            // is a Levenshtein distance of not more than 1 away from it 
                            if (Okflg){
                                String fltCodeKey=FltId+FrmApt+DstApt;
                                // Check if this flight code has already been seen
                                if (fltCodeList.containsKey(fltCodeKey)){
                                    // If so increment count for this flight code by 1
                                    fltCodeList.replace(fltCodeKey,(Integer)(fltCodeList.get(fltCodeKey))+1);    
                                } else {
                                    // Otherwise if new, check against all existing flight codes used more than once 
                                    //to see if there are nay which are a Levenshtein distance of only 1 away.
                                    for(Object currentFltCode : fltCodeList.keySet()){
                                        if((Levenshtein(currentFltCode.toString(),fltCodeKey)<2)&&((Integer)fltCodeList.get(currentFltCode)>1)){
                                            System.out.print("\n** Incorrect Closely Matching Flight Code: "+FltId);
                                            Okflg=false;
                                        }
                                    }
                                    if (Okflg){
                                        // This new flight code is OK so add it to lists
                                        fltCodeList.put(fltCodeKey,1);
                                        FltIds.add(FltId);
                                    }
                                }
                            }
                            if (Okflg) {
                                pList.addPassenger(passenger);
                            }
                            else {
                                bpList.addPassenger(passenger);
                                System.out.println("\nFaulty passenger record noted");
                            }
                        }
                    }
                }
                br.close();
                // Iterate through the faulty passenger list correcting each field and add to 
                // error-free passenger list
                System.out.println("\n                   Passenger    Flight    From  Dest");
                for (int i=0;i<bpList.size();i++){
                    Passenger passenger=bpList.getPassenger(i);
                    String PassId=passenger.getId();
                    String FltId=passenger.getFlightId();
                    String FrmApt=passenger.getFromApt();
                    String DstApt=passenger.getDestApt();
                    double DepTime=passenger.getDepTime();
                    double FltTime=passenger.getFltTime();
                    System.out.println("Before Correcting: "+PassId+" | "+FltId+ " | "+FrmApt+ " | "+DstApt);
                    PassId=GetBestMatch(PassId,PassIds);
                    FltId=GetBestMatch(FltId,FltIds);
                    FrmApt=GetBestMatch(FrmApt,aList.getKeysHashSet());
                    DstApt=GetBestMatch(DstApt,aList.getKeysHashSet());
                    System.out.println("After Correcting:  "+PassId+" | "+FltId+ " | "+FrmApt+ " | "+DstApt);
                    Passenger okPassenger=new Passenger(PassId,FltId,FrmApt,DstApt,DepTime,FltTime);
                    pList.addPassenger(okPassenger);  
                }
                System.out.println(pList.size()+" total passenger records after correction.");
        } catch (IOException e) {
            System.out.println("IO Exception");
            e.printStackTrace();
        }
    }
    // Calculates Levenshtein distance between two strings
    // From https://en.wikipedia.org/wiki/Levenshtein_distance accessed on 6/1/21 at 12am.
    public static int Levenshtein(String S1,String S2){
        int m=S1.length();
        int n=S2.length();
        int i,j,subcost;
        int [][] d=new int [m+1][n+1];
        for (i=1;i<=m;i++){
            d[i][0]=i;
        }
        for (j=1;j<=n;j++){
            d[0][j]=j;
        }
        for (j=1;j<=n;j++){
            for (i=1;i<=m;i++){
                if (S1.charAt(i-1)==S2.charAt(j-1)){
                    subcost=0;
                } else {
                    subcost=1;
                }
                d[i][j]=Math.min(Math.min(d[i-1][j]+1,d[i][j-1]+1),d[i-1][j-1]+subcost);
            }
        }
        return d[m][n]; 
    }
    // Compares FieldIn with Set containing possible values and chooses value with
    // smallest Levenshtein distance to be best match.
    public static String GetBestMatch(String FieldIn,HashSet<String> FieldListIn){
        int BestScore=9999;
        int Score=9999;
        String BestMatch="ABCDEF";
        for (String value:FieldListIn){
            Score=Levenshtein(FieldIn,value);
            if (Score<BestScore){
                BestMatch=value;
                BestScore=Score;
            }
        }
        return BestMatch;
    }
}