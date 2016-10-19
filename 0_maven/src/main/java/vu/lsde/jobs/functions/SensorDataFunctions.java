package vu.lsde.jobs.functions;

import org.apache.spark.api.java.function.Function;
import org.opensky.libadsb.msgs.*;
import scala.Tuple2;
import vu.lsde.core.model.SensorDatum;

/**
 * Created by richa on 18/10/2016.
 */
public class SensorDataFunctions {
    public static Function<SensorDatum, Boolean> validSensorData() {
        return new Function<SensorDatum, Boolean>() {
            public Boolean call(SensorDatum sensorDatum) throws Exception {
                return sensorDatum.isValidMessage();
            }
        };
    }

    public static Function<SensorDatum, Boolean> positionSensorData() {
        return new Function<SensorDatum, Boolean>() {
            @Override
            public Boolean call(SensorDatum sensorDatum) throws Exception {
                ModeSReply msg = sensorDatum.getDecodedMessage();
                switch (msg.getType()) {
                    case ADSB_AIRBORN_POSITION:
                    case ADSB_SURFACE_POSITION:
                        return true;
                }
                return false;
            }
        };
    }

    public static Function<SensorDatum, Boolean> altitudeSensorData() {
        return new Function<SensorDatum, Boolean>() {
            @Override
            public Boolean call(SensorDatum sensorDatum) throws Exception {
                ModeSReply msg = sensorDatum.getDecodedMessage();
                switch (msg.getType()) {
                    case ALTITUDE_REPLY:
                    case COMM_B_ALTITUDE_REPLY:
                    case ADSB_AIRBORN_POSITION:
                        return true;
                }
                return false;
            }
        };
    }

    public static Function<SensorDatum, Boolean> velocitySensorData() {
        return new Function<SensorDatum, Boolean>() {
            @Override
            public Boolean call(SensorDatum sensorDatum) throws Exception {
                ModeSReply msg = sensorDatum.getDecodedMessage();
                switch (msg.getType()) {
                    case ADSB_AIRSPEED:
                    case ADSB_VELOCITY:
                        return true;
                }
                return false;
            }
        };
    }

    public static Function<SensorDatum, Boolean> flightDataSensorData() {
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

    public static Function<SensorDatum, Boolean> usefulSensorData() {
        return new Function<SensorDatum, Boolean>() {
            public Boolean call(SensorDatum sensorDatum) throws Exception {
                switch (sensorDatum.getDecodedMessage().getType()) {
                    case MODES_REPLY:
                    case SHORT_ACAS:
                    case LONG_ACAS:
                    case EXTENDED_SQUITTER:
                    case COMM_D_ELM:
                    case ADSB_EMERGENCY:
                    case ADSB_TCAS:
                        return false;
                }
                return true;
            }
        };
    }

    public static Function<Tuple2<String, Iterable<SensorDatum>>, Boolean> rotorcraftSensorData() {
        return new Function<Tuple2<String, Iterable<SensorDatum>>, Boolean>() {
            public Boolean call(Tuple2<String, Iterable<SensorDatum>> tuple) throws Exception {
                for (SensorDatum sd: tuple._2) {
                    if (sd.getDecodedMessage() instanceof IdentificationMsg) {
                        IdentificationMsg msg = (IdentificationMsg) sd.getDecodedMessage();
                        return msg.getCategoryDescription().equals("Rotorcraft");
                    }
                }
                return false;
            }
        };
    }
}
