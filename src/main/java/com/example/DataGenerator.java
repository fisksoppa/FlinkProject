
package FlinkProject.FlinkProject.src.main.java.com.example;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 * Utility class for generating household energy consumption data.
 * This class creates simulated readings for multiple households over time.
 */
public class DataGenerator {
    
    private static final int NUM_HOUSEHOLDS = 11;
    private static final int NUM_HOURS = 240;  // 10 days worth of hourly readings
    private static final int RANDOM_SEED = 1;
  
    
    //Represents a single energy reading for a household at a specific time.    
    public static class HouseholdReading {
        public int householdId;
        public long timestamp;
        public double reading;
        
        
        // Default constructor required for POJO         
        public HouseholdReading() {}
        

        public HouseholdReading(int householdId, long timestamp, double reading) {
            this.householdId = householdId;
            this.timestamp = timestamp;
            this.reading = reading;
        }
        
        // Standard getters and setters required for POJO
        public int getHouseholdId() { return householdId; }
        public void setHouseholdId(int householdId) { this.householdId = householdId; }
        
        public long getTimestamp() { return timestamp; }
        public void setTimestamp(long timestamp) { this.timestamp = timestamp; }
        
        public double getReading() { return reading; }
        public void setReading(double reading) { this.reading = reading; }
    }
    
    /**
     * Generates synthetic household energy consumption data.
     * Creates a series of readings for multiple households over time.
     * @return List of household readings spanning multiple hours
     */
    public static List<HouseholdReading> generateData() {
        List<HouseholdReading> data = new ArrayList<>();
        Random random = new Random(RANDOM_SEED);  // Fixed seed for reproducibility


        // Calculate timestamp for current reading
        for (int household = 1; household <= NUM_HOUSEHOLDS; household++) {
            // Generate hourly readings for each household
            for (int hour = 0; hour < NUM_HOURS; hour++) {            
            
                long timestamp = System.currentTimeMillis() + (hour * 3600 * 1000L);
                // Generate base consumption value
                double reading = random.nextDouble() * 100.0;
                
                if(household == 11) {
                    //Add a guaranteed always increasing household
                    reading = hour + 0.1;
                }
                data.add(new HouseholdReading(household, timestamp, reading));
            }
        }
        
        return data;
    }
}

