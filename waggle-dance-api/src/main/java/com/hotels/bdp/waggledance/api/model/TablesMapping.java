package com.hotels.bdp.waggledance.api.model;

import java.util.List;

public class TablesMapping {
  private String databasePattern;
  private List<String> mappedTables;

  public TablesMapping() {
  }

  public TablesMapping(String databasePattern) {
    this.databasePattern = databasePattern;
  }

  public TablesMapping(String databasePattern, List<String> mappedTables) {
    this.databasePattern = databasePattern;
    this.mappedTables = mappedTables;
  }

  public String getDatabasePattern() {
    return databasePattern;
  }

  public void setDatabasePattern(String databasePattern) {
    this.databasePattern = databasePattern;
  }

  public List<String> getMappedTables() {
    return mappedTables;
  }

  public void setMappedTables(List<String> mappedTables) {
    this.mappedTables = mappedTables;
  }
}
