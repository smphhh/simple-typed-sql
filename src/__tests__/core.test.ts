
import * as chai from 'chai';
import * as chaiAsPromised from 'chai-as-promised';
import * as knex from 'knex';

import {Mapper} from '../core';
import {
    defineBoolean,
    defineDatetime,
    defineJson,
    defineModel,
    defineNumber,
    defineString
} from '../definition';

chai.use(chaiAsPromised);

let expect = chai.expect;

/*let testDatabaseOptions = {
    client: 'sqlite3',
    connection: ':memory:',
    useNullAsDefault: true
};*/

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

describe("Simple typed SQL", function () {

    let testModel1 = defineModel(
        'test_model',
        {
            id: defineNumber(),
            externalId: defineString({ fieldName: 'external_id' })
        }
    );

    let testModel2 = defineModel(
        'test_model_2',
        {
            id: defineNumber(),
            booleanAttribute: defineBoolean({ fieldName: 'boolean_attribute' }),
            datetimeAttribute: defineDatetime({ fieldName: 'datetime_attribute' })
        }
    );

    let testObject1 = { id: 1, externalId: 'a' };
    let testObject2 = { id: 2, externalId: 'b' };

    let testObject11 = { id: 1, booleanAttribute: true, datetimeAttribute: new Date() };

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
        });

        mapper = new Mapper(knexClient, { stringifyJson: false });
    });

    it("should allow inserting and selecting simple data", async function () {
        await mapper
            .insertInto(testModel1, testObject1);

        let data = await mapper
            .selectAllFrom(testModel1);

        expect(data).to.deep.equal([testObject1]);
    });

    it("should support boolean and datetime fields", async function () {
        await mapper
            .insertInto(testModel2, testObject11);
        let data = await mapper.selectAllFrom(testModel2);
        
        expect(data).to.deep.equal([testObject11]);
    });

    it("should support where-clauses with equality", async function () {
        await mapper.insertInto(testModel1, testObject1);
        await mapper.insertInto(testModel1, testObject2);

        let data = await mapper.selectAllFrom(testModel1).whereEqual(testModel1.externalId, 'a');

        expect(data).to.deep.equal([testObject1]);
    });

    it("should support returning returning a result set with one row as a simple instance", async function () {
        await mapper.insertInto(testModel1, testObject1);
        await mapper.insertInto(testModel1, testObject2);

        let data = await mapper.selectAllFrom(testModel1).whereEqual(testModel1.id, 1).getOne();

        expect(data).to.deep.equal(testObject1);
    });

    it("should rollback transactions", async function () {
        try {
            await mapper.transaction(async (trxMapper) => {
                await trxMapper
                    .insertInto(testModel1, { id: 1, externalId: 'a' });

                throw new Error("Rollback!");
            });

        } catch (error) {
        }

        let data = await mapper
            .selectAllFrom(testModel1);

        expect(data).to.deep.equal([]);
    });

    it("should support returning values from transaction", async function () {
        await mapper.insertInto(testModel1, testObject1);
        await mapper.insertInto(testModel1, testObject2);

        let data = await mapper.transaction(async (trxMapper) => {
            let localData = await trxMapper.selectAllFrom(testModel1).whereEqual(testModel1.id, 1);
            return localData;
        });

        expect(data).to.deep.equal([testObject1]);
    });

    it("should support basic ordering", async function () {
        await mapper.insertInto(testModel1, testObject1);
        await mapper.insertInto(testModel1, testObject2);

        let data = await mapper.selectAllFrom(testModel1).orderBy(testModel1.id, 'asc');
        expect(data).to.deep.equal([testObject1, testObject2]);

        data = await mapper.selectAllFrom(testModel1).orderBy(testModel1.id, 'desc');
        expect(data).to.deep.equal([testObject2, testObject1]);
    });

    it("should support less-than where clauses", async function () {
        await mapper.insertInto(testModel1, testObject1);
        await mapper.insertInto(testModel1, testObject2);

        let data = await mapper.selectAllFrom(testModel1).whereLess(testModel1.id, 2);

        expect(data).to.deep.equal([testObject1]);
    });

    it("should support greater-than where clauses", async function () {
        await mapper.insertInto(testModel1, testObject1);
        await mapper.insertInto(testModel1, testObject2);

        let data = await mapper.selectAllFrom(testModel1).whereGreater(testModel1.id, 1);

        expect(data).to.deep.equal([testObject2]);
    });

    it("should findOneByKey", async function () {
        await mapper.insertInto(testModel1, testObject1);
        await mapper.insertInto(testModel1, testObject2);

        let data = await mapper.tryFindOneByKey(testModel1, { id: 1 });
        expect(data).to.deep.equal(testObject1);

        data = await mapper.tryFindOneByKey(testModel1, { externalId: 'b' });
        expect(data).to.deep.equal(testObject2);

        data = await mapper.tryFindOneByKey(testModel1, { externalId: 'not_found' });
        expect(data).to.be.null;
    });

    it("should support truncating tables", async function () {
        await mapper.insertInto(testModel1, testObject1);
        await mapper.insertInto(testModel1, testObject2);

        await mapper.truncate(testModel1);

        let data = await mapper.selectAllFrom(testModel1);

        expect(data).to.deep.equal([]);
    });

    it("should support basic updating", async function () {
        await mapper.insertInto(testModel1, testObject1);
        await mapper.insertInto(testModel1, testObject2);

        await mapper.updateWith(testModel1, { externalId: 'updated' }).whereEqual(testModel1.externalId, testObject1.externalId);

        let data = await mapper.selectAllFrom(testModel1).orderBy(testModel1.id, 'asc');

        expect(data).to.deep.equal([Object.assign({}, testObject1, { externalId: 'updated' }), testObject2]);
    });

    it("should support locking rows for update", async function () {
        await mapper.insertInto(testModel1, testObject1);
        await mapper.insertInto(testModel1, testObject2);

        await mapper.transaction(async (trxMapper) => {
            let query = trxMapper.selectAllFrom(testModel1).forUpdate();

            let data = await query;
        });
    });

    it("should support deleting rows", async function () {
        await mapper.insertInto(testModel1, testObject1);
        await mapper.insertInto(testModel1, testObject2);

        await mapper
            .deleteFrom(testModel1);

        let data = await mapper.selectAllFrom(testModel1);

        expect(data).to.deep.equal([]);
    });

    it("should support deleting rows with a where clause", async function () {
        await mapper.insertInto(testModel1, testObject1);
        await mapper.insertInto(testModel1, testObject2);

        await mapper
            .deleteFrom(testModel1)
            .whereEqual(testModel1.id, 1);

        let data = await mapper.selectAllFrom(testModel1);

        expect(data).to.deep.equal([testObject2]);
    });
});



