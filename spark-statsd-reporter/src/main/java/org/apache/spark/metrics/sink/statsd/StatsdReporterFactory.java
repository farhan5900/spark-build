package org.apache.spark.metrics.sink.statsd;

import com.codahale.metrics.MetricRegistry;

import io.dropwizard.metrics.BaseReporterFactory;

public class StatsdReporterFactory extends BaseReporterFactory {
	
    private String host = "127.0.0.1";
    private int port = 8125;
    private String prefix = "";
    private String reporterName = "spark-statsd-reporter";
    private MetricFormatter metricFormatter = null;

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
    
    public String getReporterName() {
    	return reporterName;
    }
    
    public void setReporterName(String reporterName) {
    	this.reporterName = reporterName;
    }
    
    public MetricFormatter getFormatter() {
    	return metricFormatter;
    }
    
    public void setFormatter(MetricFormatter metricFormatter) {
    	this.metricFormatter = metricFormatter;
    }
    
    public MetricAttributeFilter getAttributeFilter() {
    	return (attribute) -> {
    		return !getExcludesAttributes().contains(attribute) &&
    				getIncludesAttributes().contains(attribute);
        };
    }

    @Override
    public StatsdReporter build(MetricRegistry registry) {
    	return new StatsdReporter(registry, metricFormatter, reporterName, getRateUnit(), getDurationUnit(), getFilter(), getAttributeFilter(), host, port);
    }
}
