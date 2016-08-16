
import * as chai from 'chai';
import * as chaiAsPromised from 'chai-as-promised';
import * as knex from 'knex';

import {Mapper} from '../core';
import {
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

    let testModel = defineModel(
        'test_model',
        {
            id: defineNumber(),
            externalId: defineString({ fieldName: 'external_id' })
        }
    );

    let testObject1 = { id: 1, externalId: 'a' };
    let testObject2 = { id: 2, externalId: 'b' };

    let mapper: Mapper;
    let knexClient: knex;

    beforeEach(async function () {
        knexClient = knex(testDatabaseOptions);

        await knexClient.schema.dropTableIfExists('test_model');
        await knexClient.schema.createTable('test_model', function (table) {
            table.increments('id').primary();
            table.string('external_id').notNullable().unique();
        });

        mapper = new Mapper(knexClient, { stringifyJson: false });
    });

    it("should allow inserting and selecting simple data", async function () {
        await mapper
            .insertInto(testModel, testObject1);

        let data = await mapper
            .selectAllFrom(testModel);

        expect(data).to.deep.equal([testObject1]);
    });

    it("should support where-clauses with equality", async function () {
        await mapper.insertInto(testModel, testObject1);
        await mapper.insertInto(testModel, testObject2);

        let data = await mapper.selectAllFrom(testModel).whereEqual(testModel.externalId, 'a');

        expect(data).to.deep.equal([testObject1]);
    });

    it("should rollback transactions", async function () {
        try {
            await mapper.transaction(async (trxMapper) => {
                await trxMapper
                    .insertInto(testModel, { id: 1, externalId: 'a' });

                throw new Error("Rollback!");
            });

        } catch (error) {
        }

        let data = await mapper
            .selectAllFrom(testModel);

        expect(data).to.deep.equal([]);
    });

    it("should support basic ordering", async function () {
        await mapper.insertInto(testModel, testObject1);
        await mapper.insertInto(testModel, testObject2);

        let data = await mapper.selectAllFrom(testModel).orderBy(testModel.id, 'asc');
        expect(data).to.deep.equal([testObject1, testObject2]);

        data = await mapper.selectAllFrom(testModel).orderBy(testModel.id, 'desc');
        expect(data).to.deep.equal([testObject2, testObject1]);
    });

    it("should support less-than where clauses", async function () {
        await mapper.insertInto(testModel, testObject1);
        await mapper.insertInto(testModel, testObject2);

        let data = await mapper.selectAllFrom(testModel).whereLess(testModel.id, 2);

        expect(data).to.deep.equal([testObject1]);
    });

    it("should support greater-than where clauses", async function () {
        await mapper.insertInto(testModel, testObject1);
        await mapper.insertInto(testModel, testObject2);

        let data = await mapper.selectAllFrom(testModel).whereGreater(testModel.id, 1);

        expect(data).to.deep.equal([testObject2]);
    });

    it("should findOneByKey", async function () {
        await mapper.insertInto(testModel, testObject1);
        await mapper.insertInto(testModel, testObject2);

        let data = await mapper.tryFindOneByKey(testModel, { id: 1 });
        expect(data).to.deep.equal(testObject1);

        data = await mapper.tryFindOneByKey(testModel, { externalId: 'b' });
        expect(data).to.deep.equal(testObject2);

        data = await mapper.tryFindOneByKey(testModel, { externalId: 'not_found' });
        expect(data).to.be.null;
    });

    it("should support truncating tables", async function () {
        await mapper.insertInto(testModel, testObject1);
        await mapper.insertInto(testModel, testObject2);

        await mapper.truncate(testModel);

        let data = await mapper.selectAllFrom(testModel);

        expect(data).to.deep.equal([]);
    });

    it("should support basic updating", async function () {
        await mapper.insertInto(testModel, testObject1);
        await mapper.insertInto(testModel, testObject2);

        await mapper.updateWith(testModel, { externalId: 'updated' }).whereEqual(testModel.externalId, testObject1.externalId);

        let data = await mapper.selectAllFrom(testModel).orderBy(testModel.id, 'asc');

        expect(data).to.deep.equal([Object.assign({}, testObject1, { externalId: 'updated' }), testObject2]);
    });

    it("should support locking rows for update", async function () {
        await mapper.insertInto(testModel, testObject1);
        await mapper.insertInto(testModel, testObject2);

        await mapper.transaction(async (trxMapper) => {
            let query = trxMapper.selectAllFrom(testModel).forUpdate();

            let data = await query;
        });

        /*await knexClient.transaction(async (trx) => {
            let query = trx.select('*').forUpdate().from('test_model');

            console.log(query.toString());

            let data = await query;

            //await trx.insert(testObject1).into
        });*/
    });
});



