package com.starrocks.test.util;

public class MockBuildEnvironment {
    public static void setupMockDependencies() {
        // Set system properties to bypass missing dependencies
        System.setProperty("skip.fe.grammar.check", "true");
        System.setProperty("skip.fe.parser.check", "true");
        System.setProperty("mock.dependencies", "true");
    }

    public static void verifyTestEnvironment() {
        // Check if we're in mock mode
        if (System.getProperty("mock.dependencies") == null) {
            throw new IllegalStateException("Test environment not properly configured");
        }
    }
}
