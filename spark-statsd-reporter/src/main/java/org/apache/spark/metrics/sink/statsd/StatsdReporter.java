package org.apache.spark.metrics.sink.statsd;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.Metric;
import com.codahale.metrics.MetricAttribute;
import com.codahale.metrics.MetricFilter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.ScheduledReporter;
import com.codahale.metrics.Snapshot;
import com.codahale.metrics.Timer;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.spark.metrics.sink.statsd.MetricType.*;

public class StatsdReporter extends ScheduledReporter {
    private final static Logger logger = LoggerFactory.getLogger(StatsdReporter.class);
    private final InetSocketAddress address;
    private final MetricFormatter metricFormatter;
    private MetricFilter filter;
    private MetricAttributeFilter attributeFilter;

    protected StatsdReporter(MetricRegistry registry, MetricFormatter metricFormatter, String reporterName, TimeUnit rateUnit, TimeUnit durationUnit, MetricFilter filter, MetricAttributeFilter attributeFilter, String host, int port) {
        super(registry, reporterName, filter, rateUnit, durationUnit);
        this.filter = filter;
        this.attributeFilter = attributeFilter;
        this.address = new InetSocketAddress(host, port);
        this.metricFormatter = metricFormatter;
    }

    @Override
    public void report(SortedMap<String, Gauge> gauges, SortedMap<String, Counter> counters, SortedMap<String, Histogram> histograms, SortedMap<String, Meter> meters, SortedMap<String, Timer> timers) {
        try (DatagramSocket socket = new DatagramSocket()) {
            try {
                reportGauges(getFilteredMetrics(gauges), socket);
                reportCounters(getFilteredMetrics(counters), socket);
                reportHistograms(getFilteredMetrics(histograms), socket);
                reportMeters(getFilteredMetrics(meters), socket);
                reportTimers(getFilteredMetrics(timers), socket);
            } catch (StatsdReporterException e) {
                logger.warn("Unable to send packets to StatsD", e);
            }
        } catch (IOException e) {
            logger.warn("StatsD datagram socket construction failed", e);
        }
    }
    
    private <T extends Metric> SortedMap<String, T> getFilteredMetrics(SortedMap<String, T> metrics) {
    	final TreeMap<String, T> filteredMetrics = new TreeMap<>();
        for (Map.Entry<String, T> entry : metrics.entrySet()) {
            if (filter.matches(entry.getKey(), entry.getValue())) {
            	filteredMetrics.put(entry.getKey(), entry.getValue());
            }
        }
        return Collections.unmodifiableSortedMap(filteredMetrics);
    }
    
    private void reportGauges(SortedMap<String, Gauge> gauges, DatagramSocket socket) {
        gauges.forEach((name, value) ->
                send(socket, metricFormatter.buildMetricString(name, value.getValue(), GAUGE))
        );
    }

    private void reportCounters(SortedMap<String, Counter> counters, DatagramSocket socket) {
        counters.forEach((name, value) ->
                send(socket, metricFormatter.buildMetricString(name, value.getCount(), COUNTER))
        );
    }

    private void reportHistograms(SortedMap<String, Histogram> histograms, DatagramSocket socket) {
        histograms.forEach((name, histogram) -> {
            Snapshot snapshot = histogram.getSnapshot();
            List<String> metrics = new ArrayList<>();
            if (attributeFilter.matches(MetricAttribute.COUNT)) {
            	metrics.add(metricFormatter.buildMetricString(name, "count", histogram.getCount(), GAUGE));
            }
            if (attributeFilter.matches(MetricAttribute.MAX)) {
            	metrics.add(metricFormatter.buildMetricString(name, "max", snapshot.getMax(), TIMER));
            }
            if (attributeFilter.matches(MetricAttribute.MEAN)) {
            	metrics.add(metricFormatter.buildMetricString(name, "mean", snapshot.getMean(), TIMER));
            }
            if (attributeFilter.matches(MetricAttribute.MIN)) {
            	metrics.add(metricFormatter.buildMetricString(name, "min", snapshot.getMin(), TIMER));
            }
            if (attributeFilter.matches(MetricAttribute.STDDEV)) {
            	metrics.add(metricFormatter.buildMetricString(name, "stddev", snapshot.getStdDev(), TIMER));
            }
            if (attributeFilter.matches(MetricAttribute.P50)) {
            	metrics.add(metricFormatter.buildMetricString(name, "p50", snapshot.getMedian(), TIMER));
            }
            if (attributeFilter.matches(MetricAttribute.P75)) {
            	metrics.add(metricFormatter.buildMetricString(name, "p75", snapshot.get75thPercentile(), TIMER));
            }
            if (attributeFilter.matches(MetricAttribute.P95)) {
            	metrics.add(metricFormatter.buildMetricString(name, "p95", snapshot.get95thPercentile(), TIMER));
            }
            if (attributeFilter.matches(MetricAttribute.P98)) {
            	metrics.add(metricFormatter.buildMetricString(name, "p98", snapshot.get98thPercentile(), TIMER));
            }
            if (attributeFilter.matches(MetricAttribute.P99)) {
            	metrics.add(metricFormatter.buildMetricString(name, "p99", snapshot.get99thPercentile(), TIMER));
            }
            if (attributeFilter.matches(MetricAttribute.P999)) {
            	metrics.add(metricFormatter.buildMetricString(name, "p999", snapshot.get999thPercentile(), TIMER));
            }
            send(socket, metrics.toArray(new String[metrics.size()]));
        });
    }

    private void reportMeters(SortedMap<String, Meter> meters, DatagramSocket socket) {
        meters.forEach((name, meter) -> {
        	List<String> metrics = new ArrayList<>();
        	if (attributeFilter.matches(MetricAttribute.COUNT)) {
            	metrics.add(metricFormatter.buildMetricString(name, "count", meter.getCount(), GAUGE));
            }
        	if (attributeFilter.matches(MetricAttribute.M1_RATE)) {
            	metrics.add(metricFormatter.buildMetricString(name, "m1_rate", convertRate(meter.getOneMinuteRate()), TIMER));
            }
        	if (attributeFilter.matches(MetricAttribute.M5_RATE)) {
            	metrics.add(metricFormatter.buildMetricString(name, "m5_rate", convertRate(meter.getFiveMinuteRate()), TIMER));
            }
        	if (attributeFilter.matches(MetricAttribute.M15_RATE)) {
            	metrics.add(metricFormatter.buildMetricString(name, "m15_rate", convertRate(meter.getFifteenMinuteRate()), TIMER));
            }
        	if (attributeFilter.matches(MetricAttribute.MEAN_RATE)) {
            	metrics.add(metricFormatter.buildMetricString(name, "mean_rate", convertRate(meter.getMeanRate()), TIMER));
            }
            send(socket, metrics.toArray(new String[metrics.size()]));
        });
    }

    private void reportTimers(SortedMap<String, Timer> timers, DatagramSocket socket) {
        timers.forEach((name, timer) -> {
            Snapshot snapshot = timer.getSnapshot();
            List<String> metrics = new ArrayList<>();
            if (attributeFilter.matches(MetricAttribute.MAX)) {
            	metrics.add(metricFormatter.buildMetricString(name, "max", convertDuration(snapshot.getMax()), TIMER));
            }
            if (attributeFilter.matches(MetricAttribute.MEAN)) {
            	metrics.add(metricFormatter.buildMetricString(name, "mean", convertDuration(snapshot.getMean()), TIMER));
            }
            if (attributeFilter.matches(MetricAttribute.MIN)) {
            	metrics.add(metricFormatter.buildMetricString(name, "min", convertDuration(snapshot.getMin()), TIMER));
            }
            if (attributeFilter.matches(MetricAttribute.STDDEV)) {
            	metrics.add(metricFormatter.buildMetricString(name, "stddev", convertDuration(snapshot.getStdDev()), TIMER));
            }
            if (attributeFilter.matches(MetricAttribute.P50)) {
            	metrics.add(metricFormatter.buildMetricString(name, "p50", convertDuration(snapshot.getMedian()), TIMER));
            }
            if (attributeFilter.matches(MetricAttribute.P75)) {
            	metrics.add(metricFormatter.buildMetricString(name, "p75", convertDuration(snapshot.get75thPercentile()), TIMER));
            }
            if (attributeFilter.matches(MetricAttribute.P95)) {
            	metrics.add(metricFormatter.buildMetricString(name, "p95", convertDuration(snapshot.get95thPercentile()), TIMER));
            }
            if (attributeFilter.matches(MetricAttribute.P98)) {
            	metrics.add(metricFormatter.buildMetricString(name, "p98", convertDuration(snapshot.get98thPercentile()), TIMER));
            }
            if (attributeFilter.matches(MetricAttribute.P99)) {
            	metrics.add(metricFormatter.buildMetricString(name, "p99", convertDuration(snapshot.get99thPercentile()), TIMER));
            }
            if (attributeFilter.matches(MetricAttribute.P999)) {
            	metrics.add(metricFormatter.buildMetricString(name, "p999", convertDuration(snapshot.get999thPercentile()), TIMER));
            }
        	if (attributeFilter.matches(MetricAttribute.M1_RATE)) {
            	metrics.add(metricFormatter.buildMetricString(name, "m1_rate", convertRate(timer.getOneMinuteRate()), TIMER));
            }
        	if (attributeFilter.matches(MetricAttribute.M5_RATE)) {
            	metrics.add(metricFormatter.buildMetricString(name, "m5_rate", convertRate(timer.getFiveMinuteRate()), TIMER));
            }
        	if (attributeFilter.matches(MetricAttribute.M15_RATE)) {
            	metrics.add(metricFormatter.buildMetricString(name, "m15_rate", convertRate(timer.getFifteenMinuteRate()), TIMER));
            }
        	if (attributeFilter.matches(MetricAttribute.MEAN_RATE)) {
            	metrics.add(metricFormatter.buildMetricString(name, "mean_rate", convertRate(timer.getMeanRate()), TIMER));
            }
            send(socket, metrics.toArray(new String[metrics.size()]));
        });
    }

    private void send(DatagramSocket socket, String...metrics) {
        for (String metric: metrics) {
    		byte[] bytes = metric.getBytes(UTF_8);
    		DatagramPacket packet = new DatagramPacket(bytes, bytes.length, address);
    		try {
    			socket.send(packet);
    		} catch (IOException e) {
    			throw new StatsdReporterException(e);
    		}
    	}
    }
}