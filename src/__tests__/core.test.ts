
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

let testDatabaseOptions = {
    client: 'sqlite3',
    connection: ':memory:',
    useNullAsDefault: true
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

    beforeEach(async function () {
        let knexClient = knex(testDatabaseOptions);

        await knexClient.schema.createTable('test_model', function (table) {
            table.increments('id').primary();
            table.string('external_id').notNullable().unique();
        });

        mapper = new Mapper(knexClient);
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
});



