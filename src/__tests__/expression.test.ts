import * as Bluebird from 'bluebird';
import * as chai from 'chai';
import * as chaiAsPromised from 'chai-as-promised';
import * as knex from 'knex';

import { createConfig } from '../config';

import {
    defineBoolean,
    defineDatetime,
    defineJson,
    defineMapping,
    defineNumber,
    defineString,
    Mapper,
    avg,
    count,
    countDistinct,
    max,
    min,
    sum
} from '../';

chai.use(chaiAsPromised);

let expect = chai.expect;

let config = createConfig();

describe("Simple typed SQL expressions", function () {

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
        knexClient = knex(config.knexConnection);

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

    it("should support count(*) aggregation function", async function () {
        await mapper.insertInto(testMapping1, testObject1);
        await mapper.insertInto(testMapping1, testObject2);

        let query = mapper
            .from(testMapping1)
            .select({ count: count() });

        expect(await query).to.deep.equal([{ count: 2 }]);
    });

    it("should support count aggregation function", async function () {
        await mapper.insertInto(testMapping3, testObject3_1);
        await mapper.insertInto(testMapping3, testObject3_2);
        await mapper.insertInto(testMapping3, Object.assign({}, testObject3_2, { id: 3 }));

        let query = mapper
            .from(testMapping3)
            .select({ count: count(testMapping3.value) });

        expect(await query).to.deep.equal([{ count: 3 }]);
    });

    it("should support count distinct aggregation function", async function () {
        await mapper.insertInto(testMapping3, testObject3_1);
        await mapper.insertInto(testMapping3, testObject3_2);
        await mapper.insertInto(testMapping3, Object.assign({}, testObject3_2, { id: 3 }));

        let query = mapper
            .from(testMapping3)
            .select({ count: countDistinct(testMapping3.value) });

        expect(await query).to.deep.equal([{ count: 2 }]);
    });

    it("should support sum aggregation function", async function () {
        await mapper.insertInto(testMapping1, testObject1);
        await mapper.insertInto(testMapping1, testObject2);

        let query = mapper
            .from(testMapping1)
            .select({ sum: sum(testMapping1.id) });

        expect(await query).to.deep.equal([{ sum: testObject1.id + testObject2.id }]);
    });

    it("should support avg aggregation function", async function () {
        await mapper.insertInto(testMapping1, testObject1);
        await mapper.insertInto(testMapping1, Object.assign({}, testObject2, { id: 3 }));

        let query = mapper
            .from(testMapping1)
            .select({ avg: avg(testMapping1.id) });

        expect(await query).to.deep.equal([{ avg: 2 }]);
    });

    it("should support max aggregation function", async function () {
        await mapper.insertInto(testMapping1, testObject1);
        await mapper.insertInto(testMapping1, testObject2);

        let query = mapper
            .from(testMapping1)
            .select({ max: max(testMapping1.id) });

        expect(await query).to.deep.equal([{ max: Math.max(testObject1.id, testObject2.id) }]);
    });

    it("should support min aggregation function", async function () {
        await mapper.insertInto(testMapping1, testObject1);
        await mapper.insertInto(testMapping1, testObject2);

        let query = mapper
            .from(testMapping1)
            .select({ min: min(testMapping1.id) });

        expect(await query).to.deep.equal([{ min: Math.min(testObject1.id, testObject2.id) }]);
    });

    it("should support aggregation with group by", async function () {
        await mapper.batchInsertInto(
            testMapping3,
            ['q', 'p', 'p', 'r', 'p', 'r'].map((x, index) => ({ id: index, testModel1Id: 1, value: x }))
        );

        let query = mapper
            .from(testMapping3)
            .select({ idSum: sum(testMapping3.id) })
            .groupBy(testMapping3.value)
            .orderBy(testMapping3.value, 'asc');

        let data = await query;

        expect(data.sort((a, b) => (a.idSum - b.idSum))).to.deep.equal([{ idSum: 0 }, { idSum: 7 }, { idSum: 8 }]);
    });

    it("should support ordering by aggregation expression values", async function () {
        await mapper.batchInsertInto(
            testMapping3,
            ['q', 'p', 'p', 'r', 'p', 'r'].map((x, index) => ({ id: index, testModel1Id: 1, value: x }))
        );

        let query = mapper
            .from(testMapping3)
            .select({ idCount: count(testMapping3.id) })
            .groupBy(testMapping3.value)
            .orderBy(count(testMapping3.id), 'desc');

        let data = await query;

        expect(data).to.deep.equal([{ idCount: 3 }, { idCount: 2 }, { idCount: 1 }]);
    });

    it("should support simple table count", async function () {
        await mapper.batchInsertInto(
            testMapping1,
            [testObject1, testObject2]
        );

        let query = mapper.selectCountFrom(testMapping1);

        expect(await query).to.equal(2);
    });

});



