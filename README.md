hbase-localityaware-loadbalancer
================================

## Locality Aware Load Balancer for HBase

This is the locality aware load balancer for HBase. Please check the wiki paage for more details.

## Steps to run balancer

* Copy `LocalityAwareLoadBalancer.java` file to master/balancer. 
* Build HBase using: `mvn package -DSkipTests`
* Change the property `hbase.master.loadbalancer.class` in hbase-site.xml to `org.apache.hadoop.hbase.master.balancer.LocalityAwareLoadBalancer`. Check example `hbase-site.xml`.
