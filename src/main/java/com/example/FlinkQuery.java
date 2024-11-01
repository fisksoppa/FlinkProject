package FlinkProject.FlinkProject.src.main.java.com.example;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.IterativeCondition;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.util.List;
import java.util.Map;

/**
 * Detects sequences of strictly increasing average consumption over 6-hour windows.
 */
public class FlinkQuery {
    private static final int WINDOW_SIZE_HOURS = 6;

    public static void main(String[] args) throws Exception {
        if (args.length != 1) {
            throw new IllegalArgumentException("Need input argument for parallelism");
        }

        final int parallelism = Integer.parseInt(args[0]);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(parallelism);

        // Create input stream with timestamps and watermarks
        DataStream<DataGenerator.HouseholdReading> input = env
                .fromCollection(DataGenerator.generateData())
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<DataGenerator.HouseholdReading>forMonotonousTimestamps()
                                .withTimestampAssigner((event, timestamp) -> event.getTimestamp()));

        // Calculate 6-hour averages
        DataStream<Tuple3<Integer, Long, Double>> avgStream = input
                .keyBy(DataGenerator.HouseholdReading::getHouseholdId)
                .timeWindow(Time.hours(WINDOW_SIZE_HOURS))
                .aggregate(new AverageAggregate());

        // Define and apply pattern for strictly increasing sequences
        Pattern<Tuple3<Integer, Long, Double>, ?> pattern = Pattern
                .<Tuple3<Integer, Long, Double>>begin("start")
                .next("middle").where(new StrictlyIncreasingCondition("start"))
                .next("end").where(new StrictlyIncreasingCondition("middle"));

        PatternStream<Tuple3<Integer, Long, Double>> patternStream = CEP.pattern(avgStream.keyBy(value -> value.f0), pattern);

        // Convert matches to human-readable output
        patternStream.select(new PatternToStringConverter()).print();

        env.execute("GO");
    }

    /**
     * Aggregation function to calculate average readings over a time window.
     */
    public static class AverageAggregate implements AggregateFunction<
            DataGenerator.HouseholdReading,
            Tuple3<Integer, Long, Double>,
            Tuple3<Integer, Long, Double>> {

        @Override
        public Tuple3<Integer, Long, Double> createAccumulator() {
            return new Tuple3<>(0, 0L, 0.0);
        }

        @Override
        public Tuple3<Integer, Long, Double> add(DataGenerator.HouseholdReading value, Tuple3<Integer, Long, Double> accumulator) {
            return new Tuple3<>(value.getHouseholdId(), value.getTimestamp(), accumulator.f2 + value.getReading());
        }

        @Override
        public Tuple3<Integer, Long, Double> getResult(Tuple3<Integer, Long, Double> accumulator) {
            return new Tuple3<>(accumulator.f0, accumulator.f1, accumulator.f2 / WINDOW_SIZE_HOURS);
        }

        @Override
        public Tuple3<Integer, Long, Double> merge(Tuple3<Integer, Long, Double> a, Tuple3<Integer, Long, Double> b) {
            return new Tuple3<>(a.f0, Math.min(a.f1, b.f1), a.f2 + b.f2);
        }
    }

    /**
     * Condition that ensures the current value is strictly greater than the previous value.
     */
    public static class StrictlyIncreasingCondition extends IterativeCondition<Tuple3<Integer, Long, Double>> {
        private final String previousPattern;

        public StrictlyIncreasingCondition(String previousPattern) {
            this.previousPattern = previousPattern;
        }

        @Override
        public boolean filter(Tuple3<Integer, Long, Double> current, Context<Tuple3<Integer, Long, Double>> ctx) throws Exception {
            for (Tuple3<Integer, Long, Double> previous : ctx.getEventsForPattern(previousPattern)) {
                if (current.f2 <= previous.f2) {
                    return false;
                }
            }
            return true;
        }
    }

    /**
     * Converts matched patterns to human-readable strings.
     */
    private static class PatternToStringConverter implements PatternSelectFunction<Tuple3<Integer, Long, Double>, String> {
        @Override
        public String select(Map<String, List<Tuple3<Integer, Long, Double>>> pattern) {
            Tuple3<Integer, Long, Double> start = pattern.get("start").get(0);
            Tuple3<Integer, Long, Double> middle = pattern.get("middle").get(0);
            Tuple3<Integer, Long, Double> end = pattern.get("end").get(0);

            return String.format("Household %d has 3 strictly increasing windows: %.2f -> %.2f -> %.2f", start.f0, start.f2, middle.f2, end.f2);
        }
    }
}
