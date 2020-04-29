package com.fly.ontime.data.api;


import java.util.Collections;
import java.util.HashMap;
import java.util.Map;


public class ParamBuilder<K,V> {
	final Map<K,V> map = new HashMap<K,V>();

	public ParamBuilder<K, V> add(K key, V value) {
		map.put(key, value);
		return this;
	}

	public Map<K,V> build() {
		return Collections.unmodifiableMap(new HashMap<K,V>(map));
	}
}
 