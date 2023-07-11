package io.conduktor.example.loggerinterceptor;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class LoggerInterceptorPluginTest {
    @Test
    void hasReadme() {
        assertThat(new LoggerInterceptorPlugin().readme())
                .contains("Welcome to Logger interceptor!");
    }

    @Test
    void hasTags() {
        assertThat(new LoggerInterceptorPlugin().tags())
                .containsExactly("data");
    }

    @Test
    void hasExamples() {
        assertThat(new LoggerInterceptorPlugin().examples())
                .hasSize(1);
        assertThat(new LoggerInterceptorPlugin().examples().get(0))
                .contains("\"pluginClass\": \"io.conduktor.example.loggerinterceptor.LoggerInterceptorPlugin\"");

    }
}