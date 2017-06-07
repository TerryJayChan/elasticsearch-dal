package org.terry.elasticsearch.query.dal.impl;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import org.terry.elasticsearch.query.dal.ESDalClient;
import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.plugin.nlpcn.executors.CSVResult;
import org.elasticsearch.plugin.nlpcn.executors.CSVResultsExtractor;
import org.elasticsearch.plugin.nlpcn.executors.CsvExtractorException;
import org.nlpcn.es4sql.exception.SqlParseException;
import org.nlpcn.es4sql.query.ESActionFactory;
import org.nlpcn.es4sql.query.QueryAction;

import java.sql.SQLFeatureNotSupportedException;
import java.util.*;

import static org.apache.commons.lang3.StringUtils.split;

@Slf4j
public class ESSQLDalClient implements ESDalClient {

    private static Gson gson = new Gson();

    private Client client;

    @Override
    public <T> List<T> queryForObjectList(String sql, Class<T> requiredType) {
        SearchResponse response = getSearchResponse(sql);
        if (response == null) {
            Collections.emptyList();
        }
        if (response.getAggregations() != null) {
            return getAggregationObjectList(response, requiredType);
        } else {
            return getSearchObjectList(response, requiredType);
        }
    }

    @Override
    public List<Map<String, Object>> queryForMapList(String sql) {
        SearchResponse response = getSearchResponse(sql);
        if (response == null) {
            Collections.emptyList();
        }
        if (response.getAggregations() != null) {
            return getAggregationMapList(response);
        } else {
            return getSearchMapList(response);
        }
    }

    private <T> List<T> getSearchObjectList(SearchResponse response, Class<T> requiredType) {
        List<T> resultList = new ArrayList<>();
        response.getHits().forEach(hit -> {
            T obj = gson.fromJson(hit.getSourceAsString(), requiredType);
            resultList.add(obj);
        });
        return resultList;
    }

    private <T> List<T> getAggregationObjectList(SearchResponse response, Class<T> requiredType) {
        CSVResultsExtractor extractor = new CSVResultsExtractor(false, false, false);
        try {
            CSVResult result = extractor.extractResults(response.getAggregations(), false, ",");
            List<T> resultList = new ArrayList<>();
            List<String> keys = result.getHeaders();
            result.getLines().forEach(line -> {
                Map<String, String> map = new HashMap<>();
                String[] values = split(line, ",");
                for (int i = 0; i < values.length; i++) {
                    map.put(keys.get(i), values[i]);
                }
                JsonElement jsonElement = gson.toJsonTree(map);
                T obj = gson.fromJson(jsonElement, requiredType);
                resultList.add(obj);
            });
            return resultList;
        } catch (CsvExtractorException e) {
            log.error("Error occur when getting aggregation object list", e);
        }
        return Collections.emptyList();
    }

    private List<Map<String, Object>> getSearchMapList(SearchResponse response) {
        List<Map<String, Object>> resultList = new ArrayList<>();
        response.getHits().forEach(hit -> {
            Map<String, Object> hitMap = hit.getSourceAsMap();
            resultList.add(hitMap);
        });
        return resultList;
    }

    private List<Map<String, Object>> getAggregationMapList(SearchResponse response) {
        CSVResultsExtractor extractor = new CSVResultsExtractor(false, false, false);
        try {
            CSVResult result = extractor.extractResults(response.getAggregations(), false, ",");
            List<Map<String, Object>> resultList = new ArrayList<>();
            List<String> keys = result.getHeaders();
            result.getLines().forEach(line -> {
                Map<String, Object> map = new HashMap<>();
                String[] values = split(line, ",");
                for (int i = 0; i < values.length; i++) {
                    map.put(keys.get(i), values[i]);
                }
                resultList.add(map);
            });
            return resultList;
        } catch (CsvExtractorException e) {
            log.error("Error occur when getting aggregation map list", e);
        }
        return Collections.emptyList();
    }

    private SearchResponse getSearchResponse(String sql) {
        SearchResponse response = null;
        try {
            QueryAction queryAction = ESActionFactory.create(client, sql);
            response = (SearchResponse) queryAction.explain().getBuilder().get();
        } catch (SqlParseException | SQLFeatureNotSupportedException e) {
            log.error("Unsupported query:[{}]", sql, e);
        }
        return response;
    }

    public Client getClient() {
        return client;
    }

    public void setClient(Client client) {
        this.client = client;
    }
}
