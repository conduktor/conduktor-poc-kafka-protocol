package io.conduktor.example.loggerinterceptor;

import org.junit.jupiter.api.Test;

import java.util.Map;

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
                .containsExactlyInAnyOrderEntriesOf(Map.of(
                        "version", "1.0.1-SNAPSHOT",
                        "title", "Virtual SQL Topic",
                        "description", "Don't reinvent the wheel to filter and project your messages, just use SQL!",
                        "parent", "safeguard"
                ));
    }
}