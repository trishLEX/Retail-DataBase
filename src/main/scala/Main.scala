import java.sql.{DriverManager, ResultSet}

object Main {

  def main(args: Array[String]): Unit = {
    val connection = "jdbc:postgresql://localhost:5432/maindb?user=postgres&password=0212"

    classOf[org.postgresql.Driver]

    val conn = DriverManager.getConnection(connection)

    try {
      val stmt = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)

      val rs = stmt.executeQuery("SELECT * FROM shopschema.shops")

      while (rs.next())
        println(rs.getString("shopCode"))
    }
    finally {
      conn.close()
    }
  }
}
