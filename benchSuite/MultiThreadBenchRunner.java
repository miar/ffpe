import java.util.Random;
import java.io.FileReader;
import java.io.Reader;
import java.io.Writer;
import java.io.FileWriter;
import java.io.BufferedWriter;
import java.io.IOException;


public class MultiThreadBenchRunner {

    public static void main(String[] args) throws InterruptedException {		
	MultiThreadInsertLookupRemoveSpeedup bench = new 
	  MultiThreadInsertLookupRemoveSpeedup (); 	
	bench.run();	
    }
}

