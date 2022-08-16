package org.apache.ozone.snapshot;

public class OMDBFactory {

  public static OMDB createOMDB(String dbLocation) throws Exception {
    return new OMDBImpl(dbLocation);
  }

  public static OMDB createOMDBWithCompactionListener(String dbLocation) throws Exception {
    return new OMDBWithCompactionListener(dbLocation);
  }

  public static OMDB createOMDBReadeOnly(String dbLocation, String dbName) throws Exception {
    return new OMDBReadOnly(dbLocation, dbName);
  }
}
