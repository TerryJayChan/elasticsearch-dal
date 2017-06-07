package org.terry.elasticsearch.query.dal;

import java.util.List;
import java.util.Map;

public interface ESDalClient {
    <T> List<T> queryForObjectList(String sql, Class<T> requiredType);

    List<Map<String, Object>> queryForMapList(String sql);
}
