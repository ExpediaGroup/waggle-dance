package com.hotels.bdp.waggledance.api.model;

import java.util.List;

public class MappedTables {
  private String database;
  private List<String> mappedTables;

  public MappedTables() {
  }

  public MappedTables(String database) {
    this.database = database;
  }

  public MappedTables(String database, List<String> mappedTables) {
    this.database = database;
    this.mappedTables = mappedTables;
  }

  public String getDatabase() {
    return database;
  }

  public void setDatabase(String database) {
    this.database = database;
  }

  public List<String> getMappedTables() {
    return mappedTables;
  }

  public void setMappedTables(List<String> mappedTables) {
    this.mappedTables = mappedTables;
  }
}
