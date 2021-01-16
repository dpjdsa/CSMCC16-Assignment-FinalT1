package core;
import java.util.Map;
import java.util.*;
import java.util.regex.*;
import java.io.*;
/** Class used to store list of airports
 * @author BD837672
 * @version 20th December 2020
 */
public class AirportList {
    private Map<String, Airport> apList;
    public int MAX;
    /**Constructor
     * 
     */
    public AirportList(int MaxIn)
    {        
        apList=new HashMap<>();
        MAX=MaxIn;
    }
    public void addAirport(Airport airportIn)
    {
        apList.put(airportIn.getCode(),airportIn);
    }
    public int size()
    {
        return apList.size();
    }
    public String getName(String airportCodeIn)
    {
        return apList.get(airportCodeIn).getName();
    }
    public Set<String> getKeys()
    {
        return apList.keySet();
    }
    public boolean validCode(String codeIn)
    {
        if (apList.containsKey(codeIn)){
            return true;
        }   else {
            return false;
        }
    }
    public HashSet<String> getKeysHashSet()
    {
        HashSet<String> set=new HashSet<String>();
        set.addAll(apList.keySet());
        return set; 
    }
    public String toString()
    {
        return apList.toString();
    }
}
