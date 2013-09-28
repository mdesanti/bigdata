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
    private static Options getInputOptions() {
        final Options opts = new Options();


        final Option cancelledFlights = OptionBuilder
                .withLongOpt("cancelledFlights")
                .withDescription(
                        "Calculates the amount of cancelled flights according to carrier")
                .create("cancelledFlights");

        final Option takeOffDelay = OptionBuilder
                .withLongOpt("takeOffDelay")
                .withDescription("Calculates the average take off delay for each airport")
                .create("takeOffDelay");

        final Option flightHours = OptionBuilder
                .withLongOpt("flightHours")
                .withDescription(
                        "Calculates the amount of flight hours for each plane of the given plane type")
                .create("flightHours");

        final Option flownMiles = OptionBuilder.withLongOpt("flownMiles")
                .withDescription("The amount of flown miles")
                .hasArg().create("flownMiles");
        
        final Option inFile = OptionBuilder.withLongOpt("inFile")
        						.withDescription("The path to the input file")
        						.hasArg()
        						.create("inFile");
        
        final Option outPath = OptionBuilder.withLongOpt("outPath")
				.withDescription("The path to the output file")
				.hasArg()
				.create("outPath");
        
        final Option carriers = OptionBuilder.withLongOpt("carriersFile")
				.withDescription("The path to the carriers file")
				.hasArg()
				.create("carriersFile");
        
        final Option planeType = OptionBuilder.withLongOpt("planeType")
				.withDescription("The plane type")
				.hasArg()
				.create("planeType");


        opts.addOption(cancelledFlights);
        opts.addOption(takeOffDelay);
        opts.addOption(flightHours);
        opts.addOption(flownMiles);
        opts.addOption(inFile);
        opts.addOption(carriers);
        opts.addOption(outPath);
        
        return opts;
    }


    private static AppConfig parseOptions(final Options opts,
                                          final CommandLine line) throws FileNotFoundException,
            ParseException {

        if (line.hasOption("help")) {
            return null;
        } else {
        	if(!line.hasOption("inFile") || !line.hasOption("outPath")) {
        		return null;
        	}
            AppConfig config = new AppConfig();
            config.setInPath(line.getOptionValue("inFile"));
            config.setOutPath(line.getOptionValue("outPath"));
            
            if (line.hasOption("cancelledFlights") && line.hasOption("carriersFile")) {
            	config.setMapper(CancelledFlightsMapper.class);
            	config.setReducer(CancelledFlightsReducer.class);
            	config.setCarriersPath(line.getOptionValue("carriersFile"));
            	
            }else if(line.hasOption("takeOffDelay")) {
            	config.setMapper(TakeOffDelayMapper.class);
            	config.setReducer(TakeOffDelayReducer.class);
            	
            } else if(line.hasOption("flightHours") && line.hasOption("planeType")) {
            	config.setMapper(FlightHoursMapper.class);
            	config.setReducer(FlightHoursReducer.class);
            	config.setPlaneType(line.getOptionValue("planeType"));
            	
            } else if(line.hasOption("flownMiles") && line.hasOption("carriersFile")) {
            	config.setMapper(FlownMilesMapper.class);
            	config.setReducer(FlownMilesReducer.class);
            	config.setCarriersPath(line.getOptionValue("carriersFile"));
            	
            } else {
            	return null;
            }

            return config;
        }
    }

    private static void printHelp(Options opts) {
        new HelpFormatter().printHelp("hadoop jar bd-tp1.jar", opts);
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
