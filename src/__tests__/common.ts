
import * as knex from 'knex';

import {
    defineBoolean,
    defineDatetime,
    defineJson,
    defineMapping,
    defineNumber,
    defineString,
    Mapping,
    WrappedMappingData
} from '../'

export namespace Mappings {

    export let order = defineMapping(
        'order',
        {
            id: defineNumber(),
            orderTime: defineDatetime({ fieldName: 'order_time' })
        }
    );

    export let orderDetail = defineMapping(
        'order_detail',
        {
            id: defineNumber(),
            orderId: defineNumber({ fieldName: 'order_id' }),
            quantity: defineNumber()
        }
    );
}

export namespace Objects {

    export let order1 = { id: 1, orderTime: new Date() };
    export let order2 = { id: 2, orderTime: new Date() };

    export let orderDetail1 = { id: 1, orderId: 1, quantity: 2 };
    export let orderDetail2 = { id: 2, orderId: 2, quantity: 5 };
}

export async function createSchema(knexClient: knex) {
    await knexClient.schema.dropTableIfExists('order');
    await knexClient.schema.createTable('order', function (table) {
        table.increments('id').primary();
        table.dateTime('order_time').notNullable();
    });

    await knexClient.schema.dropTableIfExists('order_detail');
    await knexClient.schema.createTable('order_detail', function (table) {
        table.increments('id').primary();
        table.integer('order_id').notNullable();
        table.integer('quantity').notNullable();
    });
}
