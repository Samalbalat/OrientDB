package test;

import com.orientechnologies.orient.core.db.ODatabaseSession;
import com.orientechnologies.orient.core.db.OrientDB;
import com.orientechnologies.orient.core.db.OrientDBConfig;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.OEdge;
import com.orientechnologies.orient.core.record.OVertex;
import com.orientechnologies.orient.core.sql.executor.OResult;
import com.orientechnologies.orient.core.sql.executor.OResultSet;

import java.util.HashMap;
import java.util.Map;

public class Main {

  public static void main(String[] args) {

    OrientDB orient = new OrientDB("remote:localhost", OrientDBConfig.defaultConfig());
    ODatabaseSession db = orient.open("test", "root", "root");

    OClass person = db.getClass("Person");

    if (person == null) {
      person = db.createVertexClass("Person");
    }

    if (person.getProperty("name") == null) {
      person.createProperty("name", OType.STRING);
      person.createIndex("Person_name_index", OClass.INDEX_TYPE.NOTUNIQUE, "name");
    }

    if (db.getClass("FriendOf") == null) {
      db.createEdgeClass("FriendOf");
    }
    
    OVertex alice = createPerson(db, "Alice", "Foo");
    OVertex bob = createPerson(db, "Bob", "Bar");
    OVertex jim = createPerson(db, "Jim", "Baz");

    OEdge edge1 = alice.addEdge(bob, "FriendOf");
    edge1.save();
    OEdge edge2 = bob.addEdge(jim, "FriendOf");
    edge2.save();
    
    String query = "SELECT expand(out('FriendOf').out('FriendOf')) from Person where name = ?";
    OResultSet rs = db.query(query, "Alice");

    while (rs.hasNext()) {
      OResult item = rs.next();
      System.out.println("friend: " + item.getProperty("name"));
    }

    rs.close();
    
    String query2 =
            " MATCH                                           " +
            "   {class:Person, as:a, where: (name = :name1)}, " +
            "   {class:Person, as:b, where: (name = :name2)}, " +
            "   {as:a} -FriendOf-> {as:x} -FriendOf-> {as:b}  " +
            " RETURN x.name as friend                         ";

        Map<String, Object> params = new HashMap<String, Object>();
        params.put("name1", "Alice");
        params.put("name2", "Jim");

        OResultSet rs2 = db.query(query2, params);

        while (rs2.hasNext()) {
          OResult item = rs.next();
          System.out.println("friend: " + item.getProperty("friend"));
        }

        rs2.close();
    
    db.close();
    orient.close();

  }

  private static OVertex createPerson(ODatabaseSession db, String name, String surname) {
    OVertex result = db.newVertex("Person");
    result.setProperty("name", name);
    result.setProperty("surname", surname);
    result.save();
    return result;
  }
}