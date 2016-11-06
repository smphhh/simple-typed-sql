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
    and,
    or,
    comparison
} from '../';

chai.use(chaiAsPromised);

let expect = chai.expect;

let config = createConfig();

describe("Simple typed SQL", function () {

    let testMapping1 = defineMapping(
        'test_model_with_some_extra_padding_plus_some_more',
        {
            id: defineNumber(),
            externalId: defineString({ fieldName: 'external_id_with_some_extra_padding_plus_some_more' })
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

        await knexClient.schema.dropTableIfExists('test_model_with_some_extra_padding_plus_some_more');
        await knexClient.schema.createTable('test_model_with_some_extra_padding_plus_some_more', function (table) {
            table.increments('id').primary();
            table.string('external_id_with_some_extra_padding_plus_some_more').notNullable().unique();
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

        mapper = new Mapper(knexClient);
    });

    it("should allow inserting and selecting simple data", async function () {
        await mapper
            .insertInto(testMapping1, testObject1);

        let data = await mapper
            .selectAllFrom(testMapping1);

        expect(data).to.deep.equal([testObject1]);
    });

    /*it("should support multiple selects", async function () {
        await mapper
            .insertInto(testMapping1, testObject1);

        let data = await mapper
            .from(testMapping1)
            .select({ externalId1: testMapping1.externalId })
            .select({ externalId2: testMapping1.externalId })
            .getOne();

        expect(data).to.deep.equal({ externalId1: testObject1.externalId, externalId2: testObject1.externalId });
    });

    it("should support multiple selects overriding attributes", async function () {
        await mapper
            .insertInto(testMapping1, testObject1);

        let data = await mapper
            .from(testMapping1)
            .select({ foo: testMapping1.externalId })
            .select({ foo: testMapping1.id })
            .getOne();

        expect(data).to.deep.equal({ externalId1: testObject1.externalId, externalId2: testObject1.externalId });
    });*/

    it("should support selecting all fields from a mapping", async function () {
        await mapper
            .insertInto(testMapping1, testObject1);

        let data = await mapper
            .from(testMapping1)
            .selectAll(testMapping1)
            .getOne();

        expect(data).to.deep.equal(testObject1);
    });

    it("should allow inserts with extra data attributes", async function () {
        await mapper
            .insertInto(testMapping1, Object.assign({ extra: 'a' }, testObject1));

        let data = await mapper
            .selectAllFrom(testMapping1);

        expect(data).to.deep.equal([testObject1]);
    });

    it("should support batch inserts", async function () {
        await mapper.batchInsertInto(testMapping1, [
            testObject1,
            testObject2
        ]);

        let data = await mapper
            .selectAllFrom(testMapping1)
            .orderBy(testMapping1.id, 'asc');

        expect(data).to.deep.equal([testObject1, testObject2]);
    });

    it("should support batch inserts with extra attributes", async function () {
        await mapper.batchInsertInto(testMapping1, [
            testObject1,
            Object.assign({ extra: 1 }, testObject2)
        ]);

        let data = await mapper
            .selectAllFrom(testMapping1)
            .orderBy(testMapping1.id, 'asc');

        expect(data).to.deep.equal([testObject1, testObject2]);
    });

    it("should support inserts with returningAll clauses", async function () {
        let data = await mapper
            .insertInto(testMapping1, testObject1)
            .returningAll();

        expect(data).to.deep.equal([testObject1]);
    });

    it("should support inserts with returningAll clauses and default values", async function () {
        let testObject = {
            id: undefined,
            externalId: 'g'
        };

        let data = await mapper
            .insertInto(testMapping1, testObject)
            .returningAll();

        expect(data.length).to.equal(1);
        expect(data[0].externalId).to.equal(testObject.externalId);
        expect(data[0].id).to.be.a('number');
    });

    it("should support boolean, datetime and bigint fields", async function () {
        await mapper
            .insertInto(testMapping2, testObject11);
        let data = await mapper.selectAllFrom(testMapping2);
        
        expect(data).to.deep.equal([testObject11]);
    });

    it("should support where-clauses with equality", async function () {
        await mapper.insertInto(testMapping1, testObject1);
        await mapper.insertInto(testMapping1, testObject2);

        let data = await mapper.selectAllFrom(testMapping1).whereEqual(testMapping1.externalId, 'a');

        expect(data).to.deep.equal([testObject1]);
    });

    it("should support returning returning a result set with one row as a simple instance", async function () {
        await mapper.insertInto(testMapping1, testObject1);
        await mapper.insertInto(testMapping1, testObject2);

        let data = await mapper.selectAllFrom(testMapping1).whereEqual(testMapping1.id, 1).getOne();

        expect(data).to.deep.equal(testObject1);
    });

    it("should rollback transactions", async function () {
        try {
            await mapper.transaction(async (trxMapper) => {
                await trxMapper
                    .insertInto(testMapping1, { id: 1, externalId: 'a' });

                throw new Error("Rollback!");
            });

        } catch (error) {
        }

        let data = await mapper
            .selectAllFrom(testMapping1);

        expect(data).to.deep.equal([]);
    });

    it("should support returning values from transaction", async function () {
        await mapper.insertInto(testMapping1, testObject1);
        await mapper.insertInto(testMapping1, testObject2);

        let data = await mapper.transaction(async (trxMapper) => {
            let localData = await trxMapper.selectAllFrom(testMapping1).whereEqual(testMapping1.id, 1);
            return localData;
        });

        expect(data).to.deep.equal([testObject1]);
    });

    it("should support basic ordering", async function () {
        await mapper.insertInto(testMapping1, testObject1);
        await mapper.insertInto(testMapping1, testObject2);

        let data = await mapper.selectAllFrom(testMapping1).orderBy(testMapping1.id, 'asc');
        expect(data).to.deep.equal([testObject1, testObject2]);

        data = await mapper.selectAllFrom(testMapping1).orderBy(testMapping1.id, 'desc');
        expect(data).to.deep.equal([testObject2, testObject1]);
    });

    it("should support limit clauses", async function () {
        await mapper.insertInto(testMapping1, testObject1);
        await mapper.insertInto(testMapping1, testObject2);

        let data = await mapper
            .selectAllFrom(testMapping1)
            .orderBy(testMapping1.id, 'asc')
            .limit(1);

        expect(data).to.deep.equal([testObject1]);
    });

    it("should support offset clauses", async function () {
        await mapper.insertInto(testMapping1, testObject1);
        await mapper.insertInto(testMapping1, testObject2);

        let data = await mapper
            .selectAllFrom(testMapping1)
            .orderBy(testMapping1.id, 'asc')
            .offset(1);

        expect(data).to.deep.equal([testObject2]);
    });

    it("should support group by clauses", async function () {
        await mapper.batchInsertInto(
            testMapping3,
            ['q', 'p', 'p', 'r', 'p', 'r'].map((x, index) => ({ id: index, testModel1Id: 1, value: x }))
        );

        let query = mapper
            .from(testMapping3)
            .select({ value: testMapping3.value })
            .groupBy(testMapping3.value)
            .orderBy(testMapping3.value, 'asc');

        let data = await query;

        expect(data).to.deep.equal([{ value: 'p' }, { value: 'q' }, { value: 'r' }])
    });

    it("should support less-than where clauses", async function () {
        await mapper.insertInto(testMapping1, testObject1);
        await mapper.insertInto(testMapping1, testObject2);

        let data = await mapper.selectAllFrom(testMapping1).whereLess(testMapping1.id, 2);

        expect(data).to.deep.equal([testObject1]);
    });

    it("should support greater-than where clauses", async function () {
        await mapper.insertInto(testMapping1, testObject1);
        await mapper.insertInto(testMapping1, testObject2);

        let data = await mapper.selectAllFrom(testMapping1).whereGreater(testMapping1.id, 1);

        expect(data).to.deep.equal([testObject2]);
    });

    it("should findOneByKey", async function () {
        await mapper.insertInto(testMapping1, testObject1);
        await mapper.insertInto(testMapping1, testObject2);

        let data = await mapper.tryFindOneByKey(testMapping1, { id: 1 });
        expect(data).to.deep.equal(testObject1);

        data = await mapper.tryFindOneByKey(testMapping1, { externalId: 'b' });
        expect(data).to.deep.equal(testObject2);

        data = await mapper.tryFindOneByKey(testMapping1, { externalId: 'not_found' });
        expect(data).to.be.null;
    });

    it("should support truncating tables", async function () {
        await mapper.insertInto(testMapping1, testObject1);
        await mapper.insertInto(testMapping1, testObject2);

        await mapper.truncate(testMapping1);

        let data = await mapper.selectAllFrom(testMapping1);

        expect(data).to.deep.equal([]);
    });

    it("should support basic updating", async function () {
        await mapper.insertInto(testMapping1, testObject1);
        await mapper.insertInto(testMapping1, testObject2);

        await mapper.updateWith(testMapping1, { externalId: 'updated' }).whereEqual(testMapping1.externalId, testObject1.externalId);

        let data = await mapper.selectAllFrom(testMapping1).orderBy(testMapping1.id, 'asc');

        expect(data).to.deep.equal([Object.assign({}, testObject1, { externalId: 'updated' }), testObject2]);
    });

    it("should return the number of affected rows from update clauses", async function () {
        await mapper.insertInto(testMapping1, testObject1);
        await mapper.insertInto(testMapping1, testObject2);

        let rowCount = await mapper
            .updateWith(testMapping1, { externalId: 'updated' })
            .whereEqual(testMapping1.externalId, testObject1.externalId);

        expect(rowCount).to.equal(1);
    });

    it("should support locking rows for update", async function () {
        await mapper.insertInto(testMapping1, testObject1);
        await mapper.insertInto(testMapping1, testObject2);

        await mapper.transaction(async (trxMapper) => {
            let query = trxMapper.selectAllFrom(testMapping1).forUpdate();

            let data = await query;
        });
    });

    it("should support deleting rows", async function () {
        await mapper.insertInto(testMapping1, testObject1);
        await mapper.insertInto(testMapping1, testObject2);

        await mapper
            .deleteFrom(testMapping1);

        let data = await mapper.selectAllFrom(testMapping1);

        expect(data).to.deep.equal([]);
    });

    it("should support deleting rows with a where clause", async function () {
        await mapper.insertInto(testMapping1, testObject1);
        await mapper.insertInto(testMapping1, testObject2);

        await mapper
            .deleteFrom(testMapping1)
            .whereEqual(testMapping1.id, 1);

        let data = await mapper.selectAllFrom(testMapping1);

        expect(data).to.deep.equal([testObject2]);
    });

    it("should support queries with custom attribute selects", async function () {
        await mapper.insertInto(testMapping1, testObject1);

        let query = mapper
            .from(testMapping1)
            .select({
                id2: testMapping1.id,
                externalId2: testMapping1.externalId
            });

        expect(await query.getOne()).to.deep.equal({ id2: testObject1.id, externalId2: testObject1.externalId });

        expect(await mapper.findOneByKey(testMapping1, { id: testObject1.id })).to.deep.equal(testObject1);
    });

    it("should support simple join queries", async function () {
        await mapper.insertInto(testMapping1, testObject1);
        await mapper.insertInto(testMapping1, testObject2);

        await mapper.insertInto(testMapping3, testObject3_1);

        let data = await mapper
            .from(testMapping1)
            .innerJoinEqual(testMapping3, testMapping1.id, testMapping3.testModel1Id)
            .select({
                externalId: testMapping1.externalId,
                value2: testMapping3.value
            });

        expect(data).to.deep.equal([
            { externalId: testObject1.externalId, value2: testObject3_1.value }
        ]);
    });

    /*it("should support selecting all fields from a mapping in combination with a normal select", async function () {
        await mapper.insertInto(testMapping1, testObject1);
        await mapper.insertInto(testMapping1, testObject2);

        await mapper.insertInto(testMapping3, testObject3_1);

        let data = await mapper
            .from(testMapping1)
            .innerJoinEqual(testMapping3, testMapping1.id, testMapping3.testModel1Id)
            .select({
                externalId: testMapping1.externalId
            })
            .selectAll(testMapping3)
            .getOne();

        expect(data).to.deep.equal(Object.assign({ externalId: testObject1.externalId }, testObject3_1));
    });*/

    it("should support simple join queries with where clauses", async function () {
        await mapper.insertInto(testMapping1, testObject1);
        await mapper.insertInto(testMapping1, testObject2);

        await mapper.insertInto(testMapping3, testObject3_1);
        await mapper.insertInto(testMapping3, testObject3_2);

        let data = await mapper
            .from(testMapping1)
            .innerJoinEqual(testMapping3, testMapping1.id, testMapping3.testModel1Id)
            .select({
                externalId: testMapping1.externalId,
                value2: testMapping3.value
            })
            .whereEqual(testMapping3.value, testObject3_2.value);

        expect(data).to.deep.equal([
            { externalId: testObject2.externalId, value2: testObject3_2.value }
        ]);
    });

    it("should support simple join queries with order by and limit clauses", async function () {
        await mapper.insertInto(testMapping1, testObject1);
        await mapper.insertInto(testMapping1, testObject2);

        await mapper.insertInto(testMapping3, testObject3_1);
        await mapper.insertInto(testMapping3, testObject3_2);

        let data = await mapper
            .from(testMapping1)
            .innerJoinEqual(testMapping3, testMapping1.id, testMapping3.testModel1Id)
            .select({
                externalId: testMapping1.externalId,
                value2: testMapping3.value
            })
            .orderBy(testMapping3.id, 'asc')
            .limit(1);

        expect(data).to.deep.equal([
            { externalId: testObject1.externalId, value2: testObject3_1.value }
        ]);
    });

    it("should lock all joined model rows for update", async function () {
        await mapper.insertInto(testMapping1, testObject1);
        await mapper.insertInto(testMapping1, testObject2);

        await mapper.insertInto(testMapping3, testObject3_1);
        await mapper.insertInto(testMapping3, testObject3_2);

        let modificationPromise = mapper.transaction(async (trxMapper) => {
            let data = await trxMapper
                .from(testMapping1)
                .innerJoinEqual(testMapping3, testMapping1.id, testMapping3.testModel1Id)
                .select({
                    externalId: testMapping1.externalId,
                    value2: testMapping3.value
                })
                .whereEqual(testMapping1.externalId, testObject1.externalId)
                .forUpdate();

            await Bluebird.delay(100);

            await trxMapper
                .updateWith(testMapping1, { externalId: 'updated' })
                .whereEqual(testMapping1.externalId, testObject1.externalId);
        });

        await Bluebird.delay(50);

        let data = await mapper.transaction(async (trxMapper) => {
            return trxMapper
                .selectAllFrom(testMapping1)
                .forUpdate()
                .whereEqual(testMapping1.externalId, testObject1.externalId);

        });

        expect(data.length).to.equal(0);

        await modificationPromise;
    });

    it("should resolve ambiguous field names in join query where clauses", async function () {
        await mapper.insertInto(testMapping1, testObject1);
        await mapper.insertInto(testMapping1, testObject2);
        await mapper.insertInto(testMapping3, testObject3_1);
        await mapper.insertInto(testMapping3, testObject3_2);

        let data = await mapper
            .from(testMapping1)
            .innerJoinEqual(testMapping3, testMapping1.id, testMapping3.testModel1Id)
            .select({ value2: testMapping3.value })
            .whereEqual(testMapping1.id, testObject2.id);

        expect(data).to.deep.equal([{ value2: testObject3_2.value}]);
    });
});
