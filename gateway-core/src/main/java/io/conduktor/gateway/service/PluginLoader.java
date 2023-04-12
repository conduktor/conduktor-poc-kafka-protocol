package io.conduktor.gateway.service;

import io.conduktor.gateway.interceptor.Plugin;

import java.util.List;
import java.util.ServiceLoader;

@FunctionalInterface
public interface PluginLoader {
    List<? extends ServiceLoader.Provider<Plugin>> load();
}
