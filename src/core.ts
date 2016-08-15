
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
        private knexBuilder: knex.QueryInterface,
        private options: SerializationOptions
    ) {
    }

    selectAllFrom<T>(model: ModelDefinition<T>) {
        let tableName = model.__metadata.tableName;
        let fieldNames = getFieldNames(model);

        let absoluteFieldNames = fieldNames.map(fieldName => `${tableName}.${fieldName}`);

        let query = this.knexBuilder.select(absoluteFieldNames).from(tableName);

        return new Query(model, query, this.options);
    }

    insertInto<T extends U, U>(model: ModelDefinition<T>, data: T) {
        let query = this.knexBuilder
            .insert(serializeData(model, data, this.options))
            .into(model.__metadata.tableName);

        return new InsertQuery(model, query, this.options);
    }

    async tryFindOneByKey<T extends U, U>(model: ModelDefinition<T>, key: U) {
        let attributeNames = getAttributes(model);
        let whereConditions = serializeData(model, key, this.options);
        let fieldNames = getFieldNames(model);
        let query = this.knexBuilder.columns(fieldNames).select().from(model.__metadata.tableName);
        
        if (Object.keys(whereConditions).length > 0) {
            query = query.where(whereConditions);
        }

        let data = await query.limit(1);
        if (data.length === 1) {
            return deserializeData(model, data[0], this.options);
        } else {
            return null as T;
        }
    }

    async findOneByKey<T extends U, U>(model: ModelDefinition<T>, key: U) {
        let data = await this.tryFindOneByKey(model, key);
        if (!data) {
            throw new Error("Expected to find exactly one row");
        } else {
            return data;
        }
    }
}

export class Mapper extends BaseMapper {
    //private query: knex.QueryBuilder;

    constructor(
        private knexClient: knex,
        options: SerializationOptions
    ) {
        super(knexClient, options);
    }

    transaction(callback: (transactionMapper: BaseMapper) => Promise<void>) {
        this.knexClient.transaction(function (trx) {
            let t: knex.Transaction;
            let transactionMapper = new BaseMapper(trx, this.options);
            return callback(transactionMapper);
        });
    }
}

type ComparisonOperator = '<' | '>' | '<=' | '>=' | '=';

export interface WhereClause {
    operator: string;
    operands: WhereClause[];
}

export class Query<ResultType> {
    constructor(
        private resultModel: ModelDefinition<ResultType>,
        private query: knex.QueryBuilder,
        private serializationOptions: SerializationOptions
    ) {
    }

    where(clause: WhereClause) {
        throw new Error("Not implemented");
    }

    whereEqual<T>(attribute: T, value: T) {
        return this.whereOperator(attribute, '=', value);
    }

    whereLess<T>(attribute: T, value: T) {
        return this.whereOperator(attribute, '<', value);
    }

    whereLessOrEqual<T>(attribute: T, value: T) {
        return this.whereOperator(attribute, '<=', value);
    }

    whereGreater<T>(attribute: T, value: T) {
        return this.whereOperator(attribute, '>', value);
    }

    whereGreaterOrEqual<T>(attribute: T, value: T) {
        return this.whereOperator(attribute, '>=', value);
    }

    limit(count: number) {
        this.query = this.query.limit(count);
        return this;
    }

    orderBy(attribute: any, direction: 'asc' | 'desc') {
        let attributeDefinition: AttributeDefinition = attribute;
        this.query = this.query.orderBy(attributeDefinition.fieldName, direction);
        return this;
    }

    private whereOperator<T>(attribute: T, operator: ComparisonOperator, value: T) {
        let attributeDefinition: AttributeDefinition = attribute as any;
        this.query = this.query.andWhere(attributeDefinition.fieldName, operator, value as any);
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

    async execute() {
        let queryResults: any[] = await this.knexQuery;
        return;
    }

    then<TResult>(onfulfilled?: () => TResult | PromiseLike<TResult>, onrejected?: (reason: any) => TResult | PromiseLike<TResult>): PromiseLike<TResult> {
        return this.knexQuery.then(onfulfilled, onrejected);
    }
}
