## Running the integration tests

Generally the integration tests should "just work" when run via Maven on the command line and in an IDE. However we have seen issues where there is an 
interaction between Dropwizard metrics classes (e.g. if you modify the version in use) and the AspectJ code and the Maven plugin we use to add monitoring to 
the `FederatedHMSHandler`. This manifests itself in tests that pass on the command line but fail in the IDE, usually with a failure like

    [ERROR] Failures: 
    [ERROR] WaggleDanceIntegrationTest.typicalWithGraphite:275->assertMetric:295 Metric 'graphitePrefix.counter.com.hotels.bdp.waggledance.server.FederatedHMSHandler.get_all_databases.all.calls.count 2' not found

This indicates that the weaving of the Aspect hasn't been activated in the IDE. One solution to this is to do the weaving using Maven on the command line 
and then re-run the tests in the IDE. The following steps should achieve that: 

1. Run `mvn -DskipTests install` in the top-level Waggle Dance parent project (this should weave the aspects using the Maven plugin and generate the 
correct class files).
2. Refresh the project in the IDE (this should get the IDE to pick up the above classes).
3. Run the integration tests (which should now pass!)

## Legal
This project is available under the [Apache 2.0 License](http://www.apache.org/licenses/LICENSE-2.0.html).

Copyright 2016-2018 Expedia Inc.
