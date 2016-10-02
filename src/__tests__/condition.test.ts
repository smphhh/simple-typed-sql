import * as Bluebird from 'bluebird';
import * as chai from 'chai';
import * as chaiAsPromised from 'chai-as-promised';
import * as knex from 'knex';

import {
    defineBoolean,
    defineDatetime,
    defineJson,
    defineMapping,
    defineNumber,
    defineString,
    Mapper,
    and,
    or,
    comparison,
    equal
} from '../';

chai.use(chaiAsPromised);

let expect = chai.expect;

let testDatabaseOptions = {
    client: 'pg',
    connection: {
        user: 'samuli',
        password: 'samuli',
        host: '10.0.75.235',
        port: 5433,
        database: 'gio_etl_scheduler_test1'
    }
};

describe("Simple typed SQL condition", function () {

    let testMapping1 = defineMapping(
        'test_model',
        {
            id: defineNumber(),
            externalId: defineString({ fieldName: 'external_id' })
        }
    );

    let testMapping2 = defineMapping(
        'test_model_2',
        {
            id: defineNumber(),
            booleanAttribute: defineBoolean({ fieldName: 'boolean_attribute' }),
            datetimeAttribute: defineDatetime({ fieldName: 'datetime_attribute' }),
            bigIntAttribute: defineNumber({ fieldName: 'big_int_attribute' }),
            dateAttribute: defineDatetime({ fieldName: 'date_attribute' })
        }
    );

    let testMapping3 = defineMapping(
        'test_model_3',
        {
            id: defineNumber(),
            testModel1Id: defineNumber({ fieldName: 'test_model_1_id' }),
            value: defineString()
        }
    );

    let testObject1 = { id: 1, externalId: 'a' };
    let testObject2 = { id: 2, externalId: 'b' };

    let testObject11 = {
        id: 1, booleanAttribute: true,
        datetimeAttribute: new Date(),
        bigIntAttribute: 182739712,
        dateAttribute: new Date(2016, 1, 20)
    };

    let testObject3_1 = { id: 1, testModel1Id: 1, value: 'x' };
    let testObject3_2 = { id: 2, testModel1Id: 2, value: 'z' };

    let mapper: Mapper;
    let knexClient: knex;

    beforeEach(async function () {
        knexClient = knex(testDatabaseOptions);

        await knexClient.schema.dropTableIfExists('test_model');
        await knexClient.schema.createTable('test_model', function (table) {
            table.increments('id').primary();
            table.string('external_id').notNullable().unique();
        });

        await knexClient.schema.dropTableIfExists('test_model_2');
        await knexClient.schema.createTable('test_model_2', function (table) {
            table.increments('id').primary();
            table.boolean('boolean_attribute').notNullable();
            table.timestamp('datetime_attribute').notNullable();
            table.bigInteger('big_int_attribute').notNullable();
            table.date('date_attribute').notNullable();
        });

        await knexClient.schema.dropTableIfExists('test_model_3');
        await knexClient.schema.createTable('test_model_3', function (table) {
            table.increments('id').primary();
            table.integer('test_model_1_id').notNullable();
            table.string('value').notNullable();
        });

        mapper = new Mapper(knexClient, { stringifyJson: false });
    });

    it("should support where clauses comparing two fields", async function () {
        await mapper.insertInto(testMapping1, testObject1);
        await mapper.insertInto(testMapping1, testObject2);

        let query = mapper
            .selectAllFrom(testMapping1)
            .where(comparison(testMapping1.externalId, '=', testMapping1.externalId));

        expect(await query).to.deep.equal([testObject1, testObject2]);        
    });

    it("should support where clauses comparing a field to a value", async function () {
        await mapper.insertInto(testMapping1, testObject1);
        await mapper.insertInto(testMapping1, testObject2);

        let query = mapper
            .selectAllFrom(testMapping1)
            .where(comparison(testMapping1.externalId, '=', testObject1.externalId));

        expect(await query).to.deep.equal([testObject1]);
    });

    it("should support where clauses comparing a value to a field", async function () {
        await mapper.insertInto(testMapping1, testObject1);
        await mapper.insertInto(testMapping1, testObject2);

        let query = mapper
            .selectAllFrom(testMapping1)
            .where(equal(testObject2.id, testMapping1.id));

        expect(await query).to.deep.equal([testObject2]);
    });

    it("should support where clauses with two comparisons combined with and-operator", async function () {
        await mapper.insertInto(testMapping1, testObject1);
        await mapper.insertInto(testMapping1, testObject2);

        let query = mapper
            .selectAllFrom(testMapping1)
            .where(and(
                equal(testObject2.id, testMapping1.id),
                equal(testMapping1.externalId, testObject2.externalId)
            ));

        expect(await query).to.deep.equal([testObject2]);
    });

    it("should support where clauses with two comparisons combined with or-operator", async function () {
        await mapper.insertInto(testMapping1, testObject1);
        await mapper.insertInto(testMapping1, testObject2);

        let query = mapper
            .selectAllFrom(testMapping1)
            .where(or(
                equal(testMapping1.id, testObject1.id),
                equal(testMapping1.id, testObject2.id)
            ))
            .orderBy(testMapping1.id, 'asc');

        expect(await query).to.deep.equal([testObject1, testObject2]);
    });

    it("should support join clauses comparing two fields", async function () {
        await mapper.insertInto(testMapping1, testObject1);
        await mapper.insertInto(testMapping1, testObject2);

        await mapper.insertInto(testMapping3, testObject3_1);

        let query = mapper
            .from(testMapping1)
            .innerJoin(testMapping3, equal(testMapping1.id, testMapping3.testModel1Id))
            .select({
                externalId: testMapping1.externalId,
                value2: testMapping3.value
            });

        expect(await query).to.deep.equal([
            { externalId: testObject1.externalId, value2: testObject3_1.value }
        ]);
    });

    it("should support join clauses with two field pair comparisons", async function () {
        await mapper.insertInto(testMapping1, testObject1);
        await mapper.insertInto(testMapping1, testObject2);

        await mapper.insertInto(testMapping3, testObject3_1);

        let query = mapper
            .from(testMapping1)
            .innerJoin(testMapping3, and(
                equal(testMapping1.id, testMapping3.testModel1Id),
                equal(testMapping1.externalId, testMapping3.value)
            ))
            .select({ value2: testMapping3.value });

        expect(await query).to.deep.equal([]);
    });

    it("should support complex join clauses containing values", async function () {
        await mapper.insertInto(testMapping1, testObject1);
        await mapper.insertInto(testMapping1, testObject2);

        await mapper.insertInto(testMapping3, testObject3_1);
        await mapper.insertInto(testMapping3, testObject3_2);

        let query = mapper
            .from(testMapping1)
            .innerJoin(testMapping3, and(
                equal(testMapping1.id, testMapping3.testModel1Id),
                equal(testMapping1.externalId, testObject1.externalId)
            ))
            .select({ value2: testMapping3.value });

        //console.log(query.toString());

        expect(await query).to.deep.equal([{ value2: testObject3_1.value }]);
    });

});



