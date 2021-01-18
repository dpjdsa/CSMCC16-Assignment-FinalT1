# CSMCC16-Assignment-FinalT1
Repository Containing Final Code for Assignment
Assignment
Task 1 Charting
First requires Task_1 to be executed to create the output file "Task_1_Output.csv":
java Task_1.java Top30_airports_LatLong.csv AComp_Passenger_data.csv

Reads in Airport Name and number of departing flights as csv from "Task_1_Output.csv"
Sorts airports in order by number of departing flights and displays
results as a bar chart.

To Compile:
First define environment variable to point to javafx sdk lib folder
in this case it was:
export PATH_TO_FX /Applications/javafx-sdk-11.0.2/lib/
 
Then compile the module using the $PATH_TO_FX variable:
 
javac --module-path $PATH_TO_FX --add-modules javafx.controls Charting.java 

finally run by:
 
java --module-path $PATH_TO_FX --add-modules javafx.controls Charting
