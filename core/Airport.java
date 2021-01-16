package core;
/** Class used to store details of a single airport
 * @author BD837672
 * @version 20th December 2020
 */
public class Airport {
    private String name;
    private String code;
    private double latitude;
    private double longitude;

    /** Constructor
     * 
     */
    public Airport(String nameIn, String codeIn, double latIn, double lonIn)
    {
        name=nameIn;
        code=codeIn;
        latitude=latIn;
        longitude=lonIn;
    }
    public String getName()
    {
        return name;
    }
    public String getCode()
    {
        return code;
    }
    public double getLat()
    {
        return latitude;
    }
    public double getLon()
    {
        return longitude;
    }
    @Override
    public String toString()
    {
        return "( Airport: "+code+","+name+" Lat: "+latitude+" Lon: "+longitude+" )"; 
    }
}
