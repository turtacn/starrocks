# StarRocks Partial Availability Feature - Test Plan

This document outlines the testing strategy for the Partial Availability feature in StarRocks. Due to current build environment constraints, this plan focuses on verifying the feature's architecture and logic flow using a mock-based testing framework.

## 1. Test Scope and Objectives

### 1.1. Feature Scope
The Partial Availability feature is designed to allow a single-replica StarRocks cluster to remain partially operational during a Backend (BE) node failure.

**In-Scope:**
-   Detection of BE node failures.
-   Identification of tablets affected by the failure.
-   Filtering of queries to exclude failed tablets, allowing queries on healthy tablets to succeed.
-   Routing of write operations for failed tablets to a temporary storage location.
-   A framework for merging data from temporary storage back to the original tablets upon BE recovery.
-   A mock-based test framework to verify the interaction and logic of these components.

**Out-of-Scope (for this implementation):**
-   A fully functional, production-ready data merging implementation. The current `MVCCDataMerger` is a placeholder.
-   Performance testing and tuning.
-   Testing with multiple concurrent BE failures.

### 1.2. Objectives
-   Verify that the `FailureDetector` correctly identifies failed BE nodes (in a mocked environment).
-   Verify that the `QueryFilter` correctly prunes queries that require failed tablets.
-   Verify that the `WriteRouter` correctly redirects write operations for failed tablets to temporary storage.
-   Verify the end-to-end logical flow of the failure, write redirection, and recovery process using the mocked test framework.
-   Document the testing strategy for future implementation of full integration and e2e tests.

### 1.3. Assumptions and Limitations
-   All testing is performed using a mock framework (`PartialAvailabilityTestFramework`) and does not involve a real running StarRocks cluster.
-   The build and execution environment is currently non-functional, preventing the compilation and execution of these tests.
-   The core data merging logic is incomplete and is mocked to always return success.

## 2. Test Environment Setup

### 2.1. Mock Configurations
The test framework relies on a set of mock components to simulate the StarRocks environment and its dependencies. These mocks are located in the `fe/fe-core/src/test/java/com/starrocks/` directory.

-   **`MockMVCCDataMerger`**: Simulates the data merging process, always returning a successful result.
-   **`MockTabletScheduler`**: Simulates the tablet scheduling process for recovery.
-   **`MockFrontendService`**: Simulates the FE's role in providing tablet status information.
-   **`MockTabletFailureRepository`**: An in-memory mock of the `TabletFailureRepository` interface, replacing the need for a real MySQL database during tests.

### 2.2. Simulated Failure Scenarios
The tests will simulate the following scenarios:
-   A single BE node goes down.
-   Tablets on the failed BE are marked as `FAILED`.
-   The failed BE node comes back online, and its tablets are marked as `RECOVERING`.

### 2.3. Test Data Requirements
The tests will use mock objects for test data, such as `MockQueryPlan` and `MockWriteRequest`, as defined within the `PartialAvailabilityTestFramework`.

## 3. Component Test Scenarios

### 3.1. FailureDetector Tests
-   **Test Case:** `testFailureDetection`
-   **Description:** Verify that when a BE is marked as down in the mock `SystemInfoService`, the `FailureDetector` correctly records the tablets on that BE as failed in the `MockTabletFailureRepository`.
-   **Expected Result:** The `save` method on the repository is called for the correct tablets with the status `FAILED`.

### 3.2. QueryFilter Tests
-   **Test Case:** `testFailureDetectionAndQueryFiltering` (in `PartialAvailabilityTestFramework`)
-   **Description:** Given a query plan that requires a set of tablets, one of which is marked as failed, verify that the `QueryFilter` removes the failed tablet from the plan.
-   **Expected Result:** The final set of tablets in the query plan does not include the failed tablet ID.

### 3.3. WriteRouter Tests
-   **Test Case:** `testWriteRoutingToTemporaryStorage` (in `PartialAvailabilityTestFramework`)
-   **Description:** Given a write request for a tablet that is marked as failed, verify that the `WriteRouter` returns a `RouteResult` indicating that the write should be routed to temporary storage.
-   **Expected Result:** The `RouteResult` has a `RouteType` of `TEMPORARY_STORAGE` and contains a non-null temporary tablet ID.

### 3.4. DataMerger Tests (mocked)
-   **Test Case:** `testMergeIsCalled` (in `DataMergerTest`)
-   **Description:** Verify that when a tablet is in the `RECOVERING` state, the `DataMerger`'s `scanAndMerge` logic calls the `MVCCDataMerger`'s `mergeTabletData` method.
-   **Expected Result:** The `mergeTabletData` method on the mock `MVCCDataMerger` is invoked.

## 4. Integration Test Scenarios

### 4.1. End-to-end failure detection to query filtering
-   **Scenario:**
    1.  Simulate a BE node failure.
    2.  Run the `FailureDetector`'s detection logic.
    3.  Create a query plan that includes tablets from the failed BE.
    4.  Pass the plan to the `QueryFilter`.
-   **Expected Result:** The `QueryFilter` correctly identifies the failed tablets and prunes them from the plan, preventing the query from failing due to unavailable tablets.

### 4.2. Write routing with temporary storage
-   **Scenario:**
    1.  Simulate a BE node failure.
    2.  Run the `FailureDetector`'s detection logic.
    3.  Issue a write request to a tablet on the failed BE.
    4.  Pass the request to the `WriteRouter`.
-   **Expected Result:** The `WriteRouter` correctly identifies the tablet as failed and returns a routing decision to use temporary storage, providing a temporary tablet ID.

### 4.3. Recovery workflow coordination
-   **Scenario:**
    1.  A tablet is in the `FAILED` state.
    2.  Simulate the BE coming back online. The tablet status is updated to `RECOVERING`.
    3.  The `DataMerger` daemon runs its `scanAndMerge` cycle.
-   **Expected Result:** The `DataMerger` finds the `RECOVERING` tablet and invokes the (mocked) `MVCCDataMerger` to perform the data merge. Upon mocked success, the tablet's status is updated to `RECOVERED`.

## 5. Edge Cases and Error Handling
-   **Scenario:** Write to a healthy tablet.
    -   **Expected:** Write is routed to `ORIGINAL_STORAGE`.
-   **Scenario:** Query that does not involve any failed tablets.
    -   **Expected:** Query plan is not modified.
-   **Scenario:** Failure to connect to the MySQL repository (in a real environment).
    -   **Expected:** The system should log errors and potentially enter a safe mode. The feature would be disabled.
-   **Scenario:** Data merge fails.
    -   **Expected:** The tablet should remain in the `RECOVERING` state, and the error should be logged. The system should retry the merge later.

## 6. Performance Considerations
-   The failure detection logic involves iterating over all backends and their tablets, which could have performance implications in clusters with a large number of tablets.
-   The write path now includes an extra check for tablet failure status, which adds a small amount of overhead to every write.
-   The creation of temporary tablets and partitions is a heavyweight operation and should be monitored.

## 7. Future Test Implementation Notes
-   The tests in `PartialAvailabilityTestFramework` are currently designed to run with mock objects. Once the build environment is stable, these tests should be adapted to run as true integration tests against a real, running StarRocks cluster.
-   The `MVCCDataMerger` needs to be fully implemented. Once it is, new tests will be required to verify the correctness of the data merging logic, including version handling and conflict resolution.
-   Performance and stress tests should be developed to evaluate the feature's impact on the system under load.

## 8. Test Execution Scripts

The following scripts demonstrate how to run the automated tests. Note that these commands cannot be executed until the build environment issues are resolved.

### 8.1. Running Mocked Integration Tests

This command runs the specific test framework class which uses the mock components.

```bash
# Navigate to the fe directory
cd fe/

# Run the test framework using Maven
# The MockBuildEnvironment properties can be used to bypass certain build checks
mvn test -Dtest=PartialAvailabilityTestFramework \
    -Dskip.fe.grammar.check=true \
    -Dmock.dependencies=true
```

### 8.2. Running All Partial Availability Unit Tests

This command would run all tests within the `com.starrocks.partial` package.

```bash
# Navigate to the fe directory
cd fe/

# Run all unit tests for the partial availability feature
mvn test -Dtest="com.starrocks.partial.**.*Test"
```

## 9. Manual End-to-End Test Steps

The following steps outline a manual test case for a real, working StarRocks cluster.

### 9.1. Prerequisites
-   A running StarRocks cluster (1 FE, 3 BEs recommended).
-   A running MySQL server accessible from the FE node.
-   The `partial_availability` feature is enabled in `fe.conf`, with `partial_availability_storage_type = mysql` and correct JDBC credentials.

### 9.2. Test Steps

1.  **Setup:**
    *   Connect to the StarRocks cluster via the MySQL client.
    *   Create a new database: `CREATE DATABASE partial_test;`
    *   Use the new database: `USE partial_test;`
    *   Create a single-replica table: `CREATE TABLE users (id INT, name STRING) ENGINE=OLAP DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 3 PROPERTIES ('replication_num' = '1');`
    *   Insert some initial data: `INSERT INTO users VALUES (1, 'Alice'), (2, 'Bob');`
    *   Verify the data: `SELECT * FROM users ORDER BY id;` (Should return 2 rows).

2.  **Simulate BE Failure:**
    *   Identify which BE node holds the tablet for one of the rows (e.g., for `id = 1`). This can be done via `SHOW TABLETS FROM users;`.
    *   Log into the machine for that BE node and stop the BE process (e.g., `./be/bin/stop_be.sh`).
    *   Wait for the `FailureDetector` to run (default interval is 10 seconds).

3.  **Verify Write Path:**
    *   Attempt to insert data that would go to the failed BE: `INSERT INTO users VALUES (3, 'Charlie');` (Assuming `HASH(3)` maps to the failed BE).
    *   **Expected Result:** The `INSERT` statement should succeed without errors.
    *   Connect to the MySQL server and check the `tablet_failures` table. Verify that a record exists for the failed tablet.

4.  **Verify Query Path:**
    *   Run a query on the table: `SELECT * FROM users ORDER BY id;`
    *   **Expected Result:** The query should succeed. It should return the rows from the healthy replicas (e.g., `(2, 'Bob')`) but not the rows from the failed replica. The data inserted during the failure (`Charlie`) will not be visible yet.

5.  **Simulate BE Recovery:**
    *   On the failed BE's machine, restart the BE process: `./be/bin/start_be.sh`.
    *   Wait for the BE to register with the FE and for its tablets to be marked as `RECOVERING`.

6.  **Verify Data Merge:**
    *   The `DataMerger` daemon should automatically detect the `RECOVERING` tablet and trigger the merge process.
    *   Monitor the `fe.log` for messages indicating the start and completion of the data merge.
    *   **Expected Result:** The logs should show a successful merge.

7.  **Final Verification:**
    *   After the merge is complete, query the table again: `SELECT * FROM users ORDER BY id;`
    *   **Expected Result:** The query should now return all data, including the data that was written during the failure (`(1, 'Alice'), (2, 'Bob'), (3, 'Charlie')`).
    *   Check the MySQL `tablet_failures` table again. The status for the recovered tablet should be updated to `RECOVERED`.
    *   Verify that any temporary tablets or partitions created during the failure have been cleaned up.
