/*
 * @author : Oguz Kahraman
 * @since : 2.08.2021
 *
 * Copyright - ApacheCalcite
 **/
package com.calcite;

import org.apache.calcite.adapter.jdbc.JdbcSchema;
import org.apache.calcite.interpreter.Bindables;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgram;
import org.apache.calcite.rel.RelHomogeneousShuttle;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttle;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.tools.RelRunner;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

public class CalciteJdbcExample {

    private static final String POSTGRESQL_SCHEMA = "PUBLIC";
    private static final String MYSQL_SCHEMA = "mysql";

    public static void main(String[] args) throws Exception {

        // Build our connection
        Connection connection = DriverManager.getConnection("jdbc:calcite:");

        // Unwrap our connection using the CalciteConnection
        CalciteConnection calciteConnection = connection.unwrap(CalciteConnection.class);

        // Get a pointer to our root schema for our Calcite Connection
        SchemaPlus rootSchema = calciteConnection.getRootSchema();

        // Instantiate a data source, this can be autowired in using Spring as well
        DataSource postgresDataSource = JdbcSchema.dataSource("jdbc:postgresql://localhost:5432/example", "org.postgresql.Driver",
                "exampleuser", "123456");

        // Attach our Postgres Jdbc Datasource to our Root Schema
        rootSchema.add(POSTGRESQL_SCHEMA, JdbcSchema.create(rootSchema, POSTGRESQL_SCHEMA, postgresDataSource, null, null));


        // Build a framework config to attach to our Calcite Planners and  Optimizers
        FrameworkConfig config = Frameworks.newConfigBuilder()
                .defaultSchema(rootSchema)
                .build();

        RelBuilder rb = RelBuilder.create(config);

        RelNode node = rb
                // First parameter is the Schema, the second is the table name
                .scan("PUBLIC", "table_name")
                // If you want to select from more than one table, you can do so by adding a second scan parameter
                .filter(
                        rb.equals(rb.field("id"), rb.literal("1"))
                )
                // These are the fields you want to return from your query
                .project(
                        rb.field("id"),
                        rb.field("example")
                )
                .build();


        HepProgram program = HepProgram.builder().build();
        HepPlanner planner = new HepPlanner(program);

        planner.setRoot(node);

        RelNode optimizedNode = planner.findBestExp();

        final RelShuttle shuttle = new RelHomogeneousShuttle() {
            @Override
            public RelNode visit(TableScan scan) {
                final RelOptTable table = scan.getTable();
                if (scan instanceof LogicalTableScan && Bindables.BindableTableScan.canHandle(table)) {
                    return Bindables.BindableTableScan.create(scan.getCluster(), table);
                }
                return super.visit(scan);
            }
        };

        optimizedNode = optimizedNode.accept(shuttle);

        final RelRunner runner = connection.unwrap(RelRunner.class);
        PreparedStatement ps = runner.prepare(optimizedNode);

        ps.execute();

        ResultSet resultSet = ps.getResultSet();
        int i = 1;
        while (resultSet.next()) {
            System.out.println(resultSet.getLong(1));
            System.out.println(resultSet.getString(2));
        }
    }
}
