package com.mozilla.telemetry.util;

import static org.junit.Assert.assertEquals;

import com.fasterxml.jackson.databind.node.ArrayNode;
import com.google.cloud.bigquery.LegacySQLTypeName;
import com.google.cloud.bigquery.Schema;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import org.json.JSONArray;
import org.junit.Test;

public class JsonTest {

  @Test
  public void instantiateJsonForCodeCoverage() {
    new Json();
  }

  @Test
  public void testReadTableRowSuceedsOnEmptyJsonObject() throws Exception {
    Json.readTableRow("{}".getBytes(StandardCharsets.UTF_8));
  }

  @Test(expected = IOException.class)
  public void testReadTableRowThrowsOnNull() throws Exception {
    Json.readTableRow(null);
  }

  @Test(expected = IOException.class)
  public void testReadTableRowThrowsOnEmptyArray() throws Exception {
    Json.readTableRow(new byte[] {});
  }

  @Test(expected = IOException.class)
  public void testReadTableRowThrowsOnNullJson() throws Exception {
    Json.readTableRow("null".getBytes(StandardCharsets.UTF_8));
  }

  @Test
  public void testReadBigQuerySchema() throws Exception {
    Schema schema = Json.readBigQuerySchema(
        "[{\"mode\":\"NULLABLE\",\"name\":\"document_id\",\"type\": \"STRING\"}]"
            .getBytes(StandardCharsets.UTF_8));
    assertEquals(LegacySQLTypeName.STRING, schema.getFields().get(0).getType());
  }

  @Test
  public void testConvertArrayNodeToJsonArray() throws Exception {
    String input = "[1,2,3]";
    ArrayNode arrayNode = Json.readArrayNode(input.getBytes(StandardCharsets.UTF_8));
    JSONArray jsonArray = Json.convertValue(arrayNode, JSONArray.class);
    assertEquals(input, jsonArray.toString());
  }

}
