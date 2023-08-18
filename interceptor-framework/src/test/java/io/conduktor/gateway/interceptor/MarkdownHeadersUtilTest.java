package io.conduktor.gateway.interceptor;


import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class MarkdownHeadersUtilTest {
    @Test
    public void validHeader() {
        assertThat(new MarkdownHeadersUtil().extractHeaders("""
                ---
                version: 1.0.1-SNAPSHOT
                title: Virtual SQL Topic
                description: Don't reinvent the wheel to filter and project your messages, just use SQL!
                parent: safeguard
                ---
                # Interceptor"""))
                .containsExactlyInAnyOrderEntriesOf(Map.of(
                        "version", "1.0.1-SNAPSHOT",
                        "title", "Virtual SQL Topic",
                        "description", "Don't reinvent the wheel to filter and project your messages, just use SQL!",
                        "parent", "safeguard"
                ));
    }

    @Test
    public void emptyHeaders() {
        assertThat(new MarkdownHeadersUtil().extractHeaders("""
                ---
                ---
                # Interceptor"""))
                .isEmpty();
    }

    @Test
    public void emptyHeader() {
        assertThat(new MarkdownHeadersUtil().extractHeaders("""
                ---
                version: 
                ---
                # Interceptor"""))
                .containsAllEntriesOf(Map.of(
                        "version", ""
                ));
    }

    @Test
    public void noHeader() {
        assertThat(new MarkdownHeadersUtil().extractHeaders("""
                # Interceptor"""))
                .isEmpty();
    }

    @Test
    public void unclosedHeader() {
        assertThat(new MarkdownHeadersUtil().extractHeaders("""
                ---
                title: Virtual SQL Topic
                # Interceptor"""))
                .isEmpty();
    }

}