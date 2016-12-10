import * as Bluebird from 'bluebird';
import * as chai from 'chai';
import * as chaiAsPromised from 'chai-as-promised';
import * as knex from 'knex';

import { createConfig } from '../config';

import {
    Utils,
    and,
    equal,
    Mapper
} from '../';

import { Mappings, Objects, createSchema } from './common';

chai.use(chaiAsPromised);

let expect = chai.expect;

let config = createConfig();

describe("Utils", function () {

    let mapper: Mapper;
    let knexClient: knex;

    beforeEach(async function () {
        knexClient = knex(config.knexConnection);
        await createSchema(knexClient);
        mapper = new Mapper(knexClient, { stringifyJson: false });
    });

    it("should support binding condition attributes to instance values", async function () {
        await mapper.insertInto(Mappings.order, Objects.order1);
        await mapper.insertInto(Mappings.orderDetail, Objects.orderDetail1);
        await mapper.insertInto(Mappings.orderDetail, Objects.orderDetail2);

        let order = await mapper.selectAllFrom(Mappings.order).getOne();

        let joinCondition = equal(Mappings.orderDetail.orderId, Mappings.order.id);
        let boundCondition = Utils.bindConditionAttributes(joinCondition, Mappings.order, order);

        let query = mapper
            .selectAllFrom(Mappings.orderDetail)
            .where(boundCondition);

        expect(await query).to.deep.equal([Objects.orderDetail1]);
    });

    it("should throw when trying to bind condition attributes to null or undefined values", async function () {
        await mapper.insertInto(Mappings.order, Objects.order1);
        await mapper.insertInto(Mappings.orderDetail, Objects.orderDetail1);

        let order = await mapper.selectAllFrom(Mappings.order).getOne();

        let joinCondition = equal(Mappings.orderDetail.orderId, Mappings.order.id);
        
        expect(() => Utils.bindConditionAttributes(
            joinCondition,
            Mappings.order,
            Object.assign({}, order, { id: null })
        )).to.throw(Error);

        expect(() => Utils.bindConditionAttributes(
            joinCondition,
            Mappings.order,
            Object.assign({}, order, { id: undefined })
        )).to.throw(Error);
    });

    it("should support catching null attribute values in condition bindings", async function () {
        await mapper.insertInto(Mappings.order, Objects.order1);
        await mapper.insertInto(Mappings.orderDetail, Objects.orderDetail1);

        let order = await mapper.selectAllFrom(Mappings.order).getOne();

        let joinCondition = and(equal(Mappings.orderDetail.orderId, Mappings.order.id), equal(1, 1));
        
        expect(Utils.bindConditionAttributes(
            joinCondition,
            Mappings.order,
            Object.assign({}, order, { id: null }),
            true
        )).to.be.null;

        expect(() => Utils.bindConditionAttributes(
            joinCondition,
            Mappings.order,
            Object.assign({}, order, { id: undefined })
        )).to.throw(Error);
    });

    it("should support selecting all fields of a mapping with a util function", async function () {
        await mapper.insertInto(Mappings.order, Objects.order1);
        await mapper.insertInto(Mappings.orderDetail, Objects.orderDetail1);

        let query = mapper
            .from(Mappings.order)
            .innerJoinEqual(Mappings.orderDetail, Mappings.order.id, Mappings.orderDetail.orderId)
            .select({ orderTime: Mappings.order.orderTime, ...Utils.selectAll(Mappings.orderDetail) });

        expect(await query.getOne()).to.deep.equal(Object.assign({ orderTime: Objects.order1.orderTime }, Objects.orderDetail1));
    });

    it("should support getting the select query return type from a select expression", function () {
        let obj = Utils.getSelectResultStub({
            id2: Mappings.order.id,
            someDate: Mappings.order.orderTime
        });

        obj as { id2: number, someDate: Date };
    });
});


