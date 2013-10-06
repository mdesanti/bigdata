package ar.edu.itba.it.bigdata.mapreduce;

import java.io.FileNotFoundException;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import ar.edu.itba.it.bigdata.mapreduce.cancelledFlights.CancelledFlightsMapper;
import ar.edu.itba.it.bigdata.mapreduce.cancelledFlights.CancelledFlightsReducer;
import ar.edu.itba.it.bigdata.mapreduce.delay.TakeOffDelayMapper;
import ar.edu.itba.it.bigdata.mapreduce.delay.TakeOffDelayReducer;
import ar.edu.itba.it.bigdata.mapreduce.flightHours.FlightHoursMapper;
import ar.edu.itba.it.bigdata.mapreduce.flightHours.FlightHoursReducer;
import ar.edu.itba.it.bigdata.mapreduce.flownMiles.FlownMilesMapper;
import ar.edu.itba.it.bigdata.mapreduce.flownMiles.FlownMilesReducer;

/**
 * Created with IntelliJ IDEA.
 * User: cris
 * Date: 23/03/13
 * Time: 14:27
 * To change this template use File | Settings | File Templates.
 */
@SuppressWarnings("all")
public class CLIParser {
    private static final String PLANE_TYPE = "planeType";
	private static final String CARRIERS_FILE = "carriersFile";
	private static final String OUT_PATH = "outPath";
	private static final String IN_FILE = "inFile";
	private static final String FLOWN_MILES = "flownMiles";
	private static final String FLIGHT_HOURS = "flightHours";
	private static final String TAKE_OFF_DELAY = "takeOffDelay";
	private static final String CANCELLED_FLIGHTS = "cancelledFlights";

	private static Options getInputOptions() {
        final Options opts = new Options();


        final Option cancelledFlights = OptionBuilder
                .withLongOpt(CANCELLED_FLIGHTS)
                .withDescription(
                        "Calculates the amount of cancelled flights according to carrier")
                .create(CANCELLED_FLIGHTS);

        final Option takeOffDelay = OptionBuilder
                .withLongOpt(TAKE_OFF_DELAY)
                .withDescription("Calculates the average take off delay for each airport")
                .create(TAKE_OFF_DELAY);

        final Option flightHours = OptionBuilder
                .withLongOpt(FLIGHT_HOURS)
                .withDescription(
                        "Calculates the amount of flight hours for each plane of the given plane type")
                .create(FLIGHT_HOURS);

        final Option flownMiles = OptionBuilder.withLongOpt(FLOWN_MILES)
                .withDescription("The amount of flown miles")
                .hasArg().create(FLOWN_MILES);
        
        final Option inFile = OptionBuilder.withLongOpt(IN_FILE)
        						.withDescription("The path to the input file")
        						.hasArg()
        						.create(IN_FILE);
        
        final Option outPath = OptionBuilder.withLongOpt(OUT_PATH)
				.withDescription("The path to the output file")
				.hasArg()
				.create(OUT_PATH);
        
        final Option carriers = OptionBuilder.withLongOpt(CARRIERS_FILE)
				.withDescription("The path to the carriers file")
				.hasArg()
				.create(CARRIERS_FILE);
        
        final Option planeType = OptionBuilder.withLongOpt(PLANE_TYPE)
				.withDescription("The plane type")
				.hasArg()
				.create(PLANE_TYPE);


        opts.addOption(cancelledFlights);
        opts.addOption(takeOffDelay);
        opts.addOption(flightHours);
        opts.addOption(flownMiles);
        opts.addOption(inFile);
        opts.addOption(carriers);
        opts.addOption(outPath);
        opts.addOption(planeType);
        
        return opts;
    }


    private static AppConfig parseOptions(final Options opts,
                                          final CommandLine line) throws FileNotFoundException,
            ParseException {

        if (line.hasOption("help")) {
            return null;
        } else {
        	if(!line.hasOption(IN_FILE) || !line.hasOption(OUT_PATH)) {
        		return null;
        	}
            AppConfig config = new AppConfig();
            config.setInPath(line.getOptionValue(IN_FILE));
            config.setOutPath(line.getOptionValue(OUT_PATH));
            
            if (line.hasOption(CANCELLED_FLIGHTS) && line.hasOption(CARRIERS_FILE)) {
            	config.setMapper(CancelledFlightsMapper.class);
            	config.setReducer(CancelledFlightsReducer.class);
            	config.setCarriersPath(line.getOptionValue(CARRIERS_FILE));
            	
            }else if(line.hasOption(TAKE_OFF_DELAY)) {
            	config.setMapper(TakeOffDelayMapper.class);
            	config.setReducer(TakeOffDelayReducer.class);
            	
            } else if(line.hasOption(FLIGHT_HOURS) && line.hasOption(PLANE_TYPE)) {
            	config.setMapper(FlightHoursMapper.class);
            	config.setReducer(FlightHoursReducer.class);
            	config.setPlaneType(line.getOptionValue(PLANE_TYPE));
            	
            } else if(line.hasOption(FLOWN_MILES) && line.hasOption(CARRIERS_FILE)) {
            	config.setMapper(FlownMilesMapper.class);
            	config.setReducer(FlownMilesReducer.class);
            	config.setCarriersPath(line.getOptionValue(CARRIERS_FILE));
            	
            } else {
            	return null;
            }

            return config;
        }
    }

    private static void printHelp(Options opts) {
        new HelpFormatter().printHelp("hadoop jar bd-tp1-jar-with-dependencies.jar", opts);
    }

    public static AppConfig getAppConfig(String[] args) throws ParseException, FileNotFoundException {
        AppConfig config;
        final CommandLineParser parser = new GnuParser();
        final Options opts = getInputOptions();
        final CommandLine line = parser.parse(opts, args);
        config = parseOptions(opts, line);
        if (config == null) {
            printHelp(opts);
        }
        return config;

    }
}
