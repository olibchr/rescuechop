package vu.lsde.jobs.functions;

import com.clearspring.analytics.util.Lists;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.opensky.libadsb.Position;
import org.opensky.libadsb.PositionDecoder;
import org.opensky.libadsb.msgs.*;
import scala.Tuple2;
import vu.lsde.core.model.FlightDatum;
import vu.lsde.core.model.SensorDatum;
import vu.lsde.core.util.Grouping;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.SortedMap;

public class FlightDataFunctions {
    public static Function<SensorDatum, Boolean> onlyFlightDataMsgs() {
        return new Function<SensorDatum, Boolean>() {
            public Boolean call(SensorDatum sd) {
                return sd.getDecodedMessage() instanceof AirbornePositionMsg
                        || sd.getDecodedMessage() instanceof SurfacePositionMsg
                        || sd.getDecodedMessage() instanceof AirspeedHeadingMsg
                        || sd.getDecodedMessage() instanceof VelocityOverGroundMsg
                        || sd.getDecodedMessage() instanceof AltitudeReply
                        || sd.getDecodedMessage() instanceof CommBAltitudeReply;
            }
        };
    }

    public static PairFunction<Tuple2<String, Iterable<SensorDatum>>, String, Iterable<FlightDatum>> sensorDataByAircraftToFlightDataByAircraft() {
        return new PairFunction<Tuple2<String, Iterable<SensorDatum>>, String, Iterable<FlightDatum>>() {
            public Tuple2<String, Iterable<FlightDatum>> call(Tuple2<String, Iterable<SensorDatum>> tuple) {
                String icao = tuple._1;
                Iterable<SensorDatum> sensorData = tuple._2;

                // Sort sensor data on time received for PositionDecoder
                List<SensorDatum> sensorDataList = Lists.newArrayList(sensorData);
                Collections.sort(sensorDataList);

                // Decode positions
                PositionDecoder decoder = new PositionDecoder();
                List<FlightDatum> flightData = new ArrayList<>();
                for (SensorDatum sd : sensorDataList) {
                    ModeSReply message = sd.getDecodedMessage();

                    if (message instanceof AirbornePositionMsg || message instanceof SurfacePositionMsg) {
                        Position position;
                        Position sensorPosition = sd.getSensorPosition();

                        if (message instanceof AirbornePositionMsg) {
                            if (sensorPosition != null) {
                                position = decoder.decodePosition(sd.getTimeAtServer(), sensorPosition, (AirbornePositionMsg) message);
                            } else {
                                position = decoder.decodePosition(sd.getTimeAtServer(), (AirbornePositionMsg) message);
                            }
                        } else {
                            if (sensorPosition != null) {
                                position = decoder.decodePosition(sd.getTimeAtServer(), sensorPosition, (SurfacePositionMsg) message);
                            } else {
                                position = decoder.decodePosition(sd.getTimeAtServer(), (SurfacePositionMsg) message);
                            }
                        }
                        if (position != null && position.isReasonable()) {
                            flightData.add(new FlightDatum(icao, sd.getTimeAtServer(), position));
                        }
                    } else if (message instanceof AltitudeReply || message instanceof CommBAltitudeReply) {
                        Double altitude;
                        if (message instanceof AltitudeReply) {
                            altitude = ((AltitudeReply) message).getAltitude();
                        } else {
                            altitude = ((CommBAltitudeReply) message).getAltitude();
                        }
                        if (altitude != null) {
                            flightData.add(new FlightDatum(icao, sd.getTimeAtServer(), altitude));
                        }
                    } else if (message instanceof AirspeedHeadingMsg) {
                        flightData.add(new FlightDatum(icao, sd.getTimeAtServer(), (AirspeedHeadingMsg) message));
                    } else if (message instanceof VelocityOverGroundMsg) {
                        flightData.add(new FlightDatum(icao, sd.getTimeAtServer(), (VelocityOverGroundMsg) message));
                    }

                }
                return new Tuple2<String, Iterable<FlightDatum>>(icao, flightData);
            }
        };
    }

    public static PairFunction<Tuple2<String, Iterable<FlightDatum>>, String, Iterable<FlightDatum>> mergeFlightData()  {
        return new PairFunction<Tuple2<String, Iterable<FlightDatum>>, String, Iterable<FlightDatum>>() {
            public Tuple2<String, Iterable<FlightDatum>> call(Tuple2<String, Iterable<FlightDatum>> tuple) throws Exception {
                String icao = tuple._1;
                Iterable<FlightDatum> flightData = tuple._2;

                List<FlightDatum> result = new ArrayList<>();

                // Group by 5s
                SortedMap<Long, List<FlightDatum>> flightDatumPer5Seconds = Grouping.groupFlightDataByTimeWindow(flightData, 5);

                // Map to merged flight data
                for (long timeWindow : flightDatumPer5Seconds.keySet()) {
                    result.add(FlightDatum.merge(flightDatumPer5Seconds.get(timeWindow)));
                }

                return new Tuple2<String, Iterable<FlightDatum>>(icao, result);
            }
        };
    }
}
