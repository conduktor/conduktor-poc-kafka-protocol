package io.conduktor.gateway.service;

import io.conduktor.gateway.interceptor.Plugin;

import java.util.List;
import java.util.ServiceLoader;

public class PluginServiceLoader implements PluginLoader {

    @Override
    public List<? extends ServiceLoader.Provider<Plugin>> load() {
        return ServiceLoader.load(Plugin.class).stream().toList();
    }
}
