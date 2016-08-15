
import * as knex from 'knex';

import {
    AttributeDefinition,
    getAttributes,
    getFieldNames,
    ModelDefinition,
    serializeData,
    deserializeData,
    SerializationOptions
} from './definition';


export class BaseMapper {
    constructor(
        private knexBuilder: knex.QueryInterface
    ) {
    }

    selectAllFrom<T>(model: ModelDefinition<T>) {
        let tableName = model.__metadata.tableName;
        let fieldNames = getFieldNames(model);

        let absoluteFieldNames = fieldNames.map(fieldName => `${tableName}.${fieldName}`);

        let query = this.knexBuilder.select(absoluteFieldNames).from(tableName);

        return new Query(model, query, {});
    }

    insertInto<T extends U, U>(model: ModelDefinition<T>, data: T) {
        let query = this.knexBuilder
            .insert(serializeData(model, data, {}))
            .into(model.__metadata.tableName);

        return new InsertQuery(model, query, {});
    }
}

export class Mapper extends BaseMapper {
    //private query: knex.QueryBuilder;

    constructor(
        private knexClient: knex
    ) {
        super(knexClient);
    }

    transaction(callback: (transactionMapper: BaseMapper) => Promise<void>) {
        this.knexClient.transaction(function (trx) {
            let t: knex.Transaction;
            let transactionMapper = new BaseMapper(trx);
            return callback(transactionMapper);
        });
    }
}

export class Query<ResultType> {
    constructor(
        private resultModel: ModelDefinition<ResultType>,
        private query: knex.QueryBuilder,
        private serializationOptions: SerializationOptions
    ) {
    }

    whereEqual<T>(attribute: T, value: T) {
        let attributeDefinition: AttributeDefinition = attribute as any;
        this.query = this.query.andWhere(attributeDefinition.fieldName, value as any);
        return this;
    }

    async execute() {
        let queryResults: any[] = await this.query;
        return queryResults.map(result => deserializeData(this.resultModel, result, this.serializationOptions));
    }

    then<TResult>(onfulfilled?: (value: ResultType[]) => TResult | PromiseLike<TResult>, onrejected?: (reason: any) => TResult | PromiseLike<TResult>): PromiseLike<TResult> {
        return this.execute().then(onfulfilled, onrejected);
    }
}

export class InsertQuery<InsertDataType> implements PromiseLike<void> {
    constructor(
        private model: ModelDefinition<InsertDataType>,
        private knexQuery: knex.QueryBuilder,
        private serializationOptions: SerializationOptions
    ) {
    }

    /*into<T extends InsertDataType>(model: ModelDefinition<T>) {
        let query = this.knexClient
            .insert(serializeData(model, this.data, this.serializationOptions))
            .into(model.__metadata.tableName);

        return new Query(model, query, this.serializationOptions);
    }*/

    async execute() {
        let queryResults: any[] = await this.knexQuery;
        return;
    }

    then<TResult>(onfulfilled?: () => TResult | PromiseLike<TResult>, onrejected?: (reason: any) => TResult | PromiseLike<TResult>): PromiseLike<TResult> {
        return this.knexQuery.then(onfulfilled, onrejected);
    }
}
