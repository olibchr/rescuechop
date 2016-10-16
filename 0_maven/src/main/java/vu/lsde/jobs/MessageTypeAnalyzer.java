package vu.lsde.jobs;

import org.apache.spark.Accumulator;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;
import org.opensky.libadsb.msgs.*;
import vu.lsde.core.model.SensorDatum;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Reads sensor data in CSV format and analyzes the kind of messages that are in there.
 */
public class MessageTypeAnalyzer extends JobBase {

    public static void main(String[] args) throws IOException {
        String inputPath = args[0];
        String outputPath = args[1];

        SparkConf sparkConf = new SparkConf().setAppName("LSDE09 MessageTypeAnalyzer");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);

        // Load sensor data
        JavaRDD<SensorDatum> sensorData = readSensorDataCsv(sc, inputPath);
        long recordsCount = sensorData.count();

        // Accumulators for counting
        final Accumulator<Integer> airPosMsgAcc = sc.accumulator(0);
        final Accumulator<Integer> airSpeedHeadingMsgAcc = sc.accumulator(0);
        final Accumulator<Integer> altReplyMsgAcc = sc.accumulator(0);
        final Accumulator<Integer> commBAltReplyAcc = sc.accumulator(0);
        final Accumulator<Integer> commBIdentifyReplyAcc = sc.accumulator(0);
        final Accumulator<Integer> commDExtLengthMsgAcc = sc.accumulator(0);
        final Accumulator<Integer> emergOrPrioStatusMsgAcc = sc.accumulator(0);
        final Accumulator<Integer> idMsgAcc = sc.accumulator(0);
        final Accumulator<Integer> idReplyAcc = sc.accumulator(0);
        final Accumulator<Integer> longAcasAcc = sc.accumulator(0);
        final Accumulator<Integer> militaryExtSquitterAcc = sc.accumulator(0);
        final Accumulator<Integer> opStatusMsgAcc = sc.accumulator(0);
        final Accumulator<Integer> shortAcasAcc = sc.accumulator(0);
        final Accumulator<Integer> surfPosMsgAcc = sc.accumulator(0);
        final Accumulator<Integer> tcasResolAdvMsg = sc.accumulator(0);
        final Accumulator<Integer> veloOverGrndMsg = sc.accumulator(0);
        final Accumulator<Integer> otherAcc = sc.accumulator(0);
        final Accumulator<Integer> invalidAcc = sc.accumulator(0);
        final Accumulator<Integer> extSquitterAcc = sc.accumulator(0);

        // Count message types
        sensorData.foreach(new VoidFunction<SensorDatum>() {
            public void call(SensorDatum sd) {
                ModeSReply msg = sd.getDecodedMessage();
                if (!sd.isValidMessage())
                    invalidAcc.add(1);
                else if (msg instanceof AirbornePositionMsg)
                    airPosMsgAcc.add(1);
                else if (msg instanceof AltitudeReply)
                    altReplyMsgAcc.add(1);
                else if (msg instanceof AirspeedHeadingMsg)
                    airSpeedHeadingMsgAcc.add(1);
                else if (msg instanceof CommBAltitudeReply)
                    commBAltReplyAcc.add(1);
                else if (msg instanceof CommBIdentifyReply)
                    commBIdentifyReplyAcc.add(1);
                else if (msg instanceof CommDExtendedLengthMsg)
                    commDExtLengthMsgAcc.add(1);
                else if (msg instanceof EmergencyOrPriorityStatusMsg)
                    emergOrPrioStatusMsgAcc.add(1);
                else if (msg instanceof IdentificationMsg)
                    idMsgAcc.add(1);
                else if (msg instanceof IdentifyReply)
                    idReplyAcc.add(1);
                else if (msg instanceof LongACAS)
                    longAcasAcc.add(1);
                else if (msg instanceof MilitaryExtendedSquitter)
                    militaryExtSquitterAcc.add(1);
                else if (msg instanceof OperationalStatusMsg)
                    opStatusMsgAcc.add(1);
                else if (msg instanceof ShortACAS)
                    shortAcasAcc.add(1);
                else if (msg instanceof SurfacePositionMsg)
                    surfPosMsgAcc.add(1);
                else if (msg instanceof TCASResolutionAdvisoryMsg)
                    tcasResolAdvMsg.add(1);
                else if (msg instanceof VelocityOverGroundMsg)
                    veloOverGrndMsg.add(1);
                else if (msg instanceof ExtendedSquitter)
                    extSquitterAcc.add(1);
                else
                    otherAcc.add(1);
            }
        });

        // Print statistics
        List<String> statistics = new ArrayList<String>();
        statistics.add(numberOfItemsStatistic("input records               ", recordsCount));
        statistics.add(numberOfItemsStatistic("AirbornePositionMsg         ", airPosMsgAcc.value(), recordsCount));
        statistics.add(numberOfItemsStatistic("AltitudeReply               ", altReplyMsgAcc.value(), recordsCount));
        statistics.add(numberOfItemsStatistic("AirspeedHeadingMsg          ", airSpeedHeadingMsgAcc.value(), recordsCount));
        statistics.add(numberOfItemsStatistic("CommBAltitudeReply          ", commBAltReplyAcc.value(), recordsCount));
        statistics.add(numberOfItemsStatistic("CommBIdentifyReply          ", commBIdentifyReplyAcc.value(), recordsCount));
        statistics.add(numberOfItemsStatistic("CommDExtendedLengthMsg      ", commDExtLengthMsgAcc.value(), recordsCount));
        statistics.add(numberOfItemsStatistic("EmergencyOrPriorityStatusMsg", emergOrPrioStatusMsgAcc.value(), recordsCount));
        statistics.add(numberOfItemsStatistic("IdentificationMsg           ", idMsgAcc.value(), recordsCount));
        statistics.add(numberOfItemsStatistic("IdentifyReply               ", idReplyAcc.value(), recordsCount));
        statistics.add(numberOfItemsStatistic("LongACAS                    ", longAcasAcc.value(), recordsCount));
        statistics.add(numberOfItemsStatistic("MilitaryExtendedSquitter    ", militaryExtSquitterAcc.value(), recordsCount));
        statistics.add(numberOfItemsStatistic("OperationalStatusMsg        ", opStatusMsgAcc.value(), recordsCount));
        statistics.add(numberOfItemsStatistic("ShortACAS                   ", shortAcasAcc.value(), recordsCount));
        statistics.add(numberOfItemsStatistic("SurfacePositionMsg          ", surfPosMsgAcc.value(), recordsCount));
        statistics.add(numberOfItemsStatistic("TCASResolutionAdvisoryMsg   ", tcasResolAdvMsg.value(), recordsCount));
        statistics.add(numberOfItemsStatistic("VelocityOverGroundMsg       ", veloOverGrndMsg.value(), recordsCount));
        statistics.add(numberOfItemsStatistic("Unknown ExtendedSquitter    ", extSquitterAcc.value(), recordsCount));
        statistics.add(numberOfItemsStatistic("other                       ", otherAcc.value(), recordsCount));
        statistics.add(numberOfItemsStatistic("invalid                     ", invalidAcc.value(), recordsCount));
        saveStatisticsAsTextFile(sc, outputPath, statistics);
    }
}
