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
