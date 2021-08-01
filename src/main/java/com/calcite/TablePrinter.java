/*
 * @author : Oguz Kahraman
 * @since : 2.08.2021
 *
 * Copyright - ApacheCalcite
 **/
package com.calcite;

import org.apache.calcite.adapter.jdbc.JdbcSchema;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.plan.Contexts;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.pretty.SqlPrettyWriter;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.Planner;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.DriverManager;

public class TablePrinter {

    public static void main(String[] args) throws Exception{
        // Build our connection
        Connection connection = DriverManager.getConnection("jdbc:calcite:");

        // Unwrap our connection using the CalciteConnection
        CalciteConnection calciteConnection = connection.unwrap(CalciteConnection.class);

        // Define our parser Configuration
        SqlParser.Config parserConfig = SqlParser.config().withCaseSensitive(false);

        // Get a pointer to our root schema for our Calcite Connection
        SchemaPlus rootSchema = calciteConnection.getRootSchema();

        DataSource ds = JdbcSchema.dataSource("jdbc:postgresql://localhost:5432/example", "org.postgresql.Driver",
                "exampleuser", "123456");

        // Attach our Postgres Jdbc Datasource to our Root Schema
        rootSchema.add("PUBLIC", JdbcSchema.create(rootSchema, "PUBLIC", ds, null, null));

        Frameworks.ConfigBuilder config = Frameworks.newConfigBuilder()
                .defaultSchema(rootSchema)
                .parserConfig(parserConfig)
                .context(Contexts.of(calciteConnection.config()));

        Planner planner = Frameworks.getPlanner(config.build());
        SqlNode node = planner.parse("SELECT * FROM PUBLIC.table_name");

        SqlNode validateNode = planner.validate(node);
        SqlWriter writer = new SqlPrettyWriter();
        validateNode.unparse(writer, 0, 0);

        // Print out our formatted SQL to the console
        System.out.println(writer.toSqlString().getSql());
    }


}
