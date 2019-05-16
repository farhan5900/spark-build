package org.apache.spark.metrics.sink.statsd;

import com.codahale.metrics.MetricRegistry;

import io.dropwizard.metrics.BaseReporterFactory;

public class StatsdReporterFactory extends BaseReporterFactory {
	private String host = "127.0.0.1";
	private int port = 8125;
	private String prefix = "";
	private MetricFormatter metricFormatter;
	
	public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }
    
    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }
    
    public String getPrefix() {
        return prefix;
    }

    public void setPrefix(String prefix) {
        this.prefix = prefix;
    }
    
    public MetricFormatter getFormatter() {
    	return metricFormatter;
    }
    
    public void setFormatter(MetricFormatter metricFormatter) {
    	this.metricFormatter = metricFormatter;
    }

	@Override
	public StatsdReporter build(MetricRegistry registry) {
		return StatsdReporter.forRegistry(registry)
				.formatter(metricFormatter)
				.host(host)
				.port(port)
				.convertDurationsTo(getDurationUnit())
                .convertRatesTo(getRateUnit())
                .filter(getFilter())
                .build();
	}

}