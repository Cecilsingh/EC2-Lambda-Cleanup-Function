package com.example.lambda;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.ec2.Ec2Client;
import software.amazon.awssdk.services.ec2.model.*;
import software.amazon.awssdk.services.cloudwatch.CloudWatchClient;
import software.amazon.awssdk.services.cloudwatch.model.*;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.stream.Collectors;

public class InstanceCleanupHandler implements RequestHandler<Map<String, String>, String> {
    
    private static final String TARGET_TAG_KEY = "Provisioner";
    private static final String TARGET_TAG_VALUE = "Terraform via Semaphore";
    private static final double CPU_THRESHOLD = 1.0;
    private static final int STOP_AFTER_DAYS = 1;
    private static final int DELETE_AFTER_DAYS = 2;
    
    private final Ec2Client ec2Client;
    private final CloudWatchClient cloudWatchClient;
    
    public InstanceCleanupHandler() {
        this.ec2Client = Ec2Client.builder()
                .region(Region.US_WEST_2) 
                .build();
        this.cloudWatchClient = CloudWatchClient.builder()
                .region(Region.US_WEST_2) 
                .build();
    }
    
    //Constructor
    public InstanceCleanupHandler(Ec2Client ec2Client, CloudWatchClient cloudWatchClient) {
        this.ec2Client = ec2Client;
        this.cloudWatchClient = cloudWatchClient;
    }

    @Override
    public String handleRequest(Map<String, String> input, Context context) {
        context.getLogger().log("Starting instance cleanup process\n");
        
        int stopped = 0;
        int terminated = 0;
        
        try {
            //Obtain all instances with the Provisioner: Terraform via Semaphore tag
            List<Instance> targetInstances = getTargetInstances();
            context.getLogger().log("Found " + targetInstances.size() + " instances with target tag\n");
            
            for (Instance instance : targetInstances) {
                String instanceId = instance.instanceId();
                InstanceStateName state = instance.state().name();
                
                context.getLogger().log("Processing instance: " + instanceId + " (State: " + state + ")\n");
                
                if (state == InstanceStateName.RUNNING) {
                    //Check CPU for running instances
                    double avgCpu = getAverageCpuUtilization(instanceId, STOP_AFTER_DAYS);
                    context.getLogger().log("Instance " + instanceId + " avg CPU: " + avgCpu + "%\n");
                    
                    if (avgCpu < CPU_THRESHOLD) {
                        stopInstance(instanceId);
                        tagInstanceWithStopTime(instanceId);
                        stopped++;
                        context.getLogger().log("Stopped instance: " + instanceId + "\n");
                    }
                } else if (state == InstanceStateName.STOPPED) {
                    //See how long its been stopped
                    Instant stopTime = getStopTime(instance);
                    if (stopTime != null) {
                        long daysStopped = ChronoUnit.DAYS.between(stopTime, Instant.now());
                        context.getLogger().log("Instance " + instanceId + " stopped for " + daysStopped + " days\n");
                        
                        if (daysStopped >= DELETE_AFTER_DAYS) {
                            terminateInstance(instanceId);
                            terminated++;
                            context.getLogger().log("Terminated instance: " + instanceId + "\n");
                        }
                    }
                }
            }
            
            String result = String.format("Cleanup completed. Stopped: %d, Terminated: %d", stopped, terminated);
            context.getLogger().log(result + "\n");
            return result;
            
        } catch (Exception e) {
            context.getLogger().log("Error: " + e.getMessage() + "\n");
            throw new RuntimeException(e);
        }
    }
    
    private List<Instance> getTargetInstances() {
        Filter tagFilter = Filter.builder()
                .name("tag:" + TARGET_TAG_KEY)
                .values(TARGET_TAG_VALUE)
                .build();
        
        Filter stateFilter = Filter.builder()
                .name("instance-state-name")
                .values("running", "stopped")
                .build();
        
        DescribeInstancesRequest request = DescribeInstancesRequest.builder()
                .filters(tagFilter, stateFilter)
                .build();
        
        DescribeInstancesResponse response = ec2Client.describeInstances(request);
        
        return response.reservations().stream()
                .flatMap(r -> r.instances().stream())
                .collect(Collectors.toList());
    }
    
    private double getAverageCpuUtilization(String instanceId, int days) {
        Instant endTime = Instant.now();
        Instant startTime = endTime.minus(days, ChronoUnit.DAYS);
        
        Dimension instanceDimension = Dimension.builder()
                .name("InstanceId")
                .value(instanceId)
                .build();
        
        GetMetricStatisticsRequest request = GetMetricStatisticsRequest.builder()
                .namespace("AWS/EC2")
                .metricName("CPUUtilization")
                .dimensions(instanceDimension)
                .startTime(startTime)
                .endTime(endTime)
                .period(3600) //Set to 1 hour
                .statistics(Statistic.AVERAGE)
                .build();
        
        GetMetricStatisticsResponse response = cloudWatchClient.getMetricStatistics(request);
        
        if (response.datapoints().isEmpty()) {
            return 0.0; //No data available, consider as low usage
        }
        
        return response.datapoints().stream()
                .mapToDouble(Datapoint::average)
                .average()
                .orElse(0.0);
    }
    
    private void stopInstance(String instanceId) {
        StopInstancesRequest request = StopInstancesRequest.builder()
                .instanceIds(instanceId)
                .build();
        
        ec2Client.stopInstances(request);
    }
    
    private void terminateInstance(String instanceId) {
        TerminateInstancesRequest request = TerminateInstancesRequest.builder()
                .instanceIds(instanceId)
                .build();
        
        ec2Client.terminateInstances(request);
    }
    
    private void tagInstanceWithStopTime(String instanceId) {
        Tag tag = Tag.builder()
                .key("AutoStopTime")
                .value(Instant.now().toString())
                .build();
        
        CreateTagsRequest request = CreateTagsRequest.builder()
                .resources(instanceId)
                .tags(tag)
                .build();
        
        ec2Client.createTags(request);
    }
    
    private Instant getStopTime(Instance instance) {
        //Check to see if we tagged it with AutoStopTime
        Optional<Tag> autoStopTag = instance.tags().stream()
                .filter(tag -> tag.key().equals("AutoStopTime"))
                .findFirst();
        
        if (autoStopTag.isPresent()) {
            return Instant.parse(autoStopTag.get().value());
        }
        
        //Fallback to StateTransitionReason if available
        String stateReason = instance.stateTransitionReason();
        if (stateReason != null && stateReason.contains("(")) {
            try {
                //StateTransitionReason format: "User initiated (2024-10-23 12:34:56 GMT)"
                String dateStr = stateReason.substring(
                    stateReason.indexOf("(") + 1, 
                    stateReason.indexOf(")")
                );
                return Instant.parse(dateStr.replace(" ", "T").replace(" GMT", "Z"));
            } catch (Exception e) {
                //If parsing fails, return null
                return null;
            }
        }
        
        return null;
    }
}
