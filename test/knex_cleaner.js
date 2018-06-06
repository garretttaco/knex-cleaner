const BPromise = require("bluebird");
const Faker = require("faker");
const chai = require("chai");
const chaiAsPromised = require("chai-as-promised");
const config = require("config");
const knexLib = require("knex");
const knexCleaner = require("../lib/knex_cleaner");
const knexTables = require("../lib/knex_tables");

const knexMySQL = knexLib(config.get("mysql"));
const knexPG = knexLib(config.get("pg"));
const knexSqLite3 = knexLib(config.get("sqlite3"));

const { expect } = chai;
chai.should();
chai.use(chaiAsPromised);

describe("knex_cleaner", function() {
  const clients = [
    { client: "mysql", knex: knexMySQL },
    { client: "postgres", knex: knexPG },
    { client: "sqllite", knex: knexSqLite3 }
  ];

  clients.forEach(function(dbTestValues) {
    const { knex, client } = dbTestValues;
    const prefixTableName = knexTables.prefixTableNameByClient(client);

    describe(dbTestValues.client, function() {
      beforeEach("start with empty db", async function() {
        const tableNames = await knexTables.getTableNames(knex, {
          schemas: ["public", "public2"]
        });
        return Promise.all(
          tableNames.map(tableName => {
            if (tableName !== "sqlite_sequence" && client !== "sqllite") {
              var segments = tableName.split(".");
              var schemaName = segments[0];
              var table = segments[1];
              return knex.schema.withSchema(schemaName).dropTable(table);
            }
          })
        );
      });

      after(function() {
        return dbTestValues.knex.destroy();
      });

      it("handles a database with no tables", function() {
        return knexCleaner.clean(knex);
      });

      describe("basic tests", function() {
        beforeEach(function() {
          return dbTestValues.knex.schema
            .withSchema(client === "postgres" ? "public" : undefined)
            .createTable("test_1", function(table) {
              table.increments();
              table.string("name");
              table.timestamps();
            })
            .then(() => {
              return dbTestValues.knex.schema
                .withSchema(client === "postgres" ? "public" : undefined)
                .createTable("test_2", function(table) {
                  table.increments();
                  table.string("name");
                  table
                    .integer("test_1_id")
                    .unsigned()
                    .references("test_1.id");
                  table.timestamps();
                });
            })
            .then(function() {
              return BPromise.all([
                dbTestValues
                  .knex("test_1")
                  .withSchema(client === "postgres" ? "public" : undefined)
                  .insert({ name: Faker.company.companyName() }),
                dbTestValues
                  .knex("test_1")
                  .withSchema(client === "postgres" ? "public" : undefined)
                  .insert({ name: Faker.company.companyName() }),
                dbTestValues
                  .knex("test_1")
                  .withSchema(client === "postgres" ? "public" : undefined)
                  .insert({ name: Faker.company.companyName() })
              ]).then(function() {
                return dbTestValues
                  .knex("test_1")
                  .withSchema(client === "postgres" ? "public" : undefined)
                  .select()
                  .map(function(row) {
                    return dbTestValues.knex("test_2").insert({
                      name: Faker.company.companyName(),
                      test_1_id: row[0]
                    });
                  });
              });
            });
        });

        afterEach(function() {
          return knexTables.getDropTables(dbTestValues.knex, [
            prefixTableName("test_1"),
            prefixTableName("test_2")
          ]);
        });

        it("can clear all tables with defaults", function() {
          return knexCleaner.clean(dbTestValues.knex).then(function() {
            return BPromise.all([
              knexTables
                .getTableRowCount(dbTestValues.knex, prefixTableName("test_1"))
                .should.eventually.equal(0),
              knexTables
                .getTableRowCount(dbTestValues.knex, prefixTableName("test_2"))
                .should.eventually.equal(0)
            ]);
          });
        });

        it("can clear all tables with delete", function() {
          return knexCleaner
            .clean(dbTestValues.knex, {
              mode: "delete"
            })
            .then(function() {
              return BPromise.all([
                knexTables
                  .getTableRowCount(
                    dbTestValues.knex,
                    prefixTableName("test_1")
                  )
                  .should.eventually.equal(0),
                knexTables
                  .getTableRowCount(
                    dbTestValues.knex,
                    prefixTableName("test_2")
                  )
                  .should.eventually.equal(0)
              ]);
            });
        });

        it("can clear all tables ignoring tables", function() {
          return knexCleaner
            .clean(dbTestValues.knex, {
              ignoreTables: ["test_1"]
            })
            .then(function() {
              return BPromise.all([
                knexTables
                  .getTableRowCount(
                    dbTestValues.knex,
                    prefixTableName("test_1")
                  )
                  .should.eventually.equal(3),
                knexTables
                  .getTableRowCount(
                    dbTestValues.knex,
                    prefixTableName("test_2")
                  )
                  .should.eventually.equal(0)
              ]);
            });
        });
        if (client === "postgres") {
          describe("multiple schemas", () => {
            before(() => {
              return dbTestValues.knex.raw(
                "CREATE SCHEMA IF NOT EXISTS public2"
              );
            });

            beforeEach(() => {
              return dbTestValues.knex.schema
                .withSchema("public2")
                .createTable("test_1", function(table) {
                  table.increments();
                  table.string("name");
                  table.timestamps();
                })
                .then(() => {
                  return dbTestValues.knex.schema
                    .withSchema("public2")
                    .createTable("test_2", function(table) {
                      table.increments();
                      table.string("name");
                      table
                        .integer("test_1_id")
                        .unsigned()
                        .references("test_1.id");
                      table.timestamps();
                    });
                })
                .then(function() {
                  return BPromise.all([
                    dbTestValues
                      .knex("test_1")
                      .withSchema("public2")
                      .insert({ name: Faker.company.companyName() }),
                    dbTestValues
                      .knex("test_1")
                      .withSchema("public2")
                      .insert({ name: Faker.company.companyName() }),
                    dbTestValues
                      .knex("test_1")
                      .withSchema("public2")
                      .insert({ name: Faker.company.companyName() })
                  ]).then(function() {
                    return dbTestValues
                      .knex("test_1")
                      .select()
                      .map(function(row) {
                        return dbTestValues.knex("test_2").insert({
                          name: Faker.company.companyName(),
                          test_1_id: row[0]
                        });
                      });
                  });
                });
            });

            after(() => {
              return dbTestValues.knex.raw("DROP SCHEMA public2 CASCADE");
            });

            it("can clear all tables with more than one schema", function() {
              return knexCleaner
                .clean(dbTestValues.knex, {
                  schemas: ["public", "public2"]
                })
                .then(function() {
                  return knexTables
                    .getTableRowCount(
                      dbTestValues.knex,
                      prefixTableName("test_1", "public2")
                    )
                    .should.eventually.equal(3)
                    .then(() => {
                      return knexTables
                        .getTableRowCount(
                          dbTestValues.knex,
                          prefixTableName("test_2", "public2")
                        )
                        .should.eventually.equal(0);
                    });
                });
            });
          });
        }

        describe("camel cased table names", function() {
          var tableName = prefixTableName("dogBreeds");
          beforeEach(function() {
            return dbTestValues.knex.schema
              .withSchema(client === "postgres" ? "public" : undefined)
              .createTableIfNotExists("dogBreeds", function(table) {
                table.increments();
                table.string("name");
                table.timestamps();
              })
              .then(function() {
                return dbTestValues.knex(tableName).insert({
                  name: "corgi"
                });
              });
          });

          afterEach(function() {
            return knexTables.getDropTables(dbTestValues.knex, [tableName]);
          });

          it("clears the table with defaults", function() {
            return knexCleaner.clean(dbTestValues.knex).then(function() {
              return knexTables
                .getTableRowCount(dbTestValues.knex, tableName)
                .should.eventually.equal(0);
            });
          });

          it("clears the table with delete", function() {
            return knexCleaner
              .clean(dbTestValues.knex, {
                mode: "delete"
              })
              .then(function() {
                return knexTables
                  .getTableRowCount(dbTestValues.knex, tableName)
                  .should.eventually.equal(0);
              });
          });
        });
      });
    });
  });
});
