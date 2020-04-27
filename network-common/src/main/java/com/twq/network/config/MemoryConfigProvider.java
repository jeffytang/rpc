package com.twq.network.config;

import java.util.HashMap;
import java.util.Map;
import java.util.NoSuchElementException;

public class MemoryConfigProvider extends ConfigProvider {
    private Map<String, String> configMap;

    public MemoryConfigProvider() {
        configMap = new HashMap<>();

        // 在这里放置配置
    }

    @Override
    public String get(String name) {
        String value = configMap.get(name);
        if (value == null)
            throw new NoSuchElementException();
        return value;
    }

    @Override
    public Iterable<Map.Entry<String, String>> getAll() {
        return configMap.entrySet();
    }
}
