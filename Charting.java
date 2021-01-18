
import javafx.application.Application;
import javafx.scene.Scene;
import javafx.scene.text.Font;
import javafx.scene.text.Text;
import javafx.stage.Stage;
import javafx.scene.chart.BarChart;
import javafx.scene.chart.CategoryAxis;
import javafx.scene.chart.NumberAxis;
import javafx.scene.chart.XYChart;
import javafx.scene.layout.VBox;
import java.io.*;
import java.util.*;
import java.util.Map.*;
/**
 * Assignment
 * Task 1 Charting 
 * Reads in Airport Name and number of departing flights as csv from "Task_1_Output.csv"
 * Sorts airports in order by number of departing flights and displays
 * results as a bar chart.
 * 
 * To Compile:
 * First define environment variable to point to javafx sdk lib folder
 * in this case it was:
 * export PATH_TO_FX /Applications/javafx-sdk-11.0.2/lib/
 * 
 * Then compile the module using the $PATH_TO_FX variable:
 * 
 * javac --module-path $PATH_TO_FX --add-modules javafx.controls Charting.java
 * 
 * finally run by:
 * 
 * java --module-path $PATH_TO_FX --add-modules javafx.controls Charting
 */
// From tutorials.jenkov.com/javafx/barchart.html accessed on 17/1/21 at 10.00am
// JavaFX using JavaFX Charts release 2.2 E20487-08 Jan 2014 from docs.oracle.com/javafx/2/charts/jfxpub-chsarts.pdf
// accessed on 17/1/21 at 2.00pm
public class Charting extends Application
{
    public static XYChart.Series dataSeries1=new XYChart.Series();
    @Override
    public void start(Stage stage) throws Exception
    {
        stage.setTitle("Chart of results for Task 1");
        CategoryAxis xAxis = new CategoryAxis();
        xAxis.setLabel("Airport");
        xAxis.setTickLabelsVisible(true);
        NumberAxis yAxis=new NumberAxis(0,5,1);
        yAxis.setMinorTickCount(1);
        yAxis.setLabel("Number of Departing Flights");
        BarChart barChart=new BarChart(xAxis,yAxis);
        barChart.setTitle("Departing Flights per Airport");
        barChart.getData().add(dataSeries1);
        barChart.setBarGap(0);
        barChart.setCategoryGap(3.0);
        barChart.setLegendVisible(false);
        VBox vbox =new VBox(barChart);
        Scene scene = new Scene (vbox,500,500);
        stage.setScene(scene);
        stage.setHeight(500);
        stage.setWidth(1000);
        stage.show();
    }
    public static void main(String[] args)
    {
        // map to hold airport, number of departing flights
        // used for sorting
        HashMap <String, Double> map = new HashMap<>();
        // Define input CSV file
        String FileName="Task_1_Output.csv";
        // Read in input file
        BufferedReader br = null;
        String line = "";
        try {
                br = new BufferedReader(new FileReader(FileName));
                while((line=br.readLine())!=null){
                    if (line.length()>0){
                        String[] Field = line.split(",");
                        String name=Field[0];
                        double numDepFlts=Double.parseDouble(Field[1]);              
                        map.put(name,numDepFlts);
                    }          
                }
                br.close();
        } catch (IOException e) {
            System.out.println("IO Exception");
            e.printStackTrace();
        }
        // Sorting the map containing the airport name and number of departing flights
        // by number of flights
        // using method in http://pragmaticnotes.com/2017/08/10/benchmarking-approaches-to-sort-java-map-by-value/
        // accessed on 17/1/21 at 9pm
        List<Entry<String,Double>> list = new LinkedList<>(map.entrySet());
        Collections.sort(list, new Comparator<Map.Entry<String,Double>>(){
            @Override
            public int compare(Entry<String, Double> o1,Entry<String,Double> o2){
                return o2.getValue().compareTo(o1.getValue());
            }
        });
        // add points to data series
        for (Entry<String, Double> entry:list){
            dataSeries1.getData().add(new XYChart.Data(entry.getKey(),entry.getValue()));
        }
        // display chart
        launch(args);
    }
}