
import * as knex from 'knex';

import {
    AttributeDefinition,
    getAttributes,
    getFieldNames,
    getTableName,
    ModelDefinition,
    serializeData,
    deserializeData,
    SerializationOptions
} from './definition';


export class BaseMapper {
    constructor(
        private knexBuilder: knex.QueryInterface,
        protected options: SerializationOptions
    ) {
    }

    selectAllFrom<T>(model: ModelDefinition<T>) {
        let tableName = getTableName(model);
        let fieldNames = getFieldNames(model);

        let absoluteFieldNames = fieldNames.map(fieldName => `${tableName}.${fieldName}`);

        let query = this.knexBuilder.select(absoluteFieldNames).from(tableName);

        return new SelectQuery(model, query, this.options);
    }

    updateWith<T extends U, U>(model: ModelDefinition<T>, data: T) {
        let tableName = getTableName(model);
        let fieldData = serializeData(model, data, this.options);

        let query = this.knexBuilder.table(tableName).update(fieldData);

        return new UpdateQuery(model, query, this.options);
    }

    insertInto<T extends U, U>(model: ModelDefinition<T>, data: T) {
        let query = this.knexBuilder
            .insert(serializeData(model, data, this.options))
            .into(model.__metadata.tableName);

        return new InsertQuery(model, query, this.options);
    }

    truncate<T>(model: ModelDefinition<T>): Promise<void> {
        return Promise.resolve(this.knexBuilder.table(model.__metadata.tableName).truncate());
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

    transaction<T>(callback: (transactionMapper: BaseMapper) => Promise<T>) {
        return Promise.resolve<T>(this.knexClient.transaction((trx) => {
            let t: knex.Transaction;
            let transactionMapper = new BaseMapper(trx, this.options);
            return callback(transactionMapper);
        }));
    }
}

type ComparisonOperator = '<' | '>' | '<=' | '>=' | '=';

export interface WhereClause {
    operator: string;
    operands: WhereClause[];
}

export class BaseQuery {
    constructor(
        protected knexQuery: knex.QueryBuilder
    ) {

    }

    getKnexQuery() {
        return this.knexQuery;
    }
}

export class WhereQuery extends BaseQuery {
    constructor(
        knexQuery: knex.QueryBuilder
    ) {
        super(knexQuery);
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

    private whereOperator<T>(attribute: T, operator: ComparisonOperator, value: T) {
        let attributeDefinition: AttributeDefinition = attribute as any;
        this.knexQuery = this.knexQuery.andWhere(attributeDefinition.fieldName, operator, value as any);
        return this;
    }
}

export class SelectQuery<ResultType> extends WhereQuery {
    constructor(
        private resultModel: ModelDefinition<ResultType>,
        knexQuery: knex.QueryBuilder,
        private serializationOptions: SerializationOptions
    ) {
        super(knexQuery);
    }

    forUpdate() {
        this.knexQuery = this.knexQuery.forUpdate();
        return this;
    }

    limit(count: number) {
        this.knexQuery = this.knexQuery.limit(count);
        return this;
    }

    orderBy(attribute: any, direction: 'asc' | 'desc') {
        let attributeDefinition: AttributeDefinition = attribute;
        this.knexQuery = this.knexQuery.orderBy(attributeDefinition.fieldName, direction);
        return this;
    }

    async getOne() {
        let data = await this.execute();
        if (data.length === 1) {
            return data[0];
        } else {
            throw new Error("Expected exactly one row");
        }
    }

    async execute() {
        let queryResults: any[] = await this.knexQuery;
        return queryResults.map(result => deserializeData(this.resultModel, result, this.serializationOptions));
    }

    then<TResult>(onfulfilled?: (value: ResultType[]) => TResult | PromiseLike<TResult>, onrejected?: (reason: any) => TResult | PromiseLike<TResult>): PromiseLike<TResult> {
        return this.execute().then(onfulfilled, onrejected);
    }
}

export class UpdateQuery<UpdateDataType> extends WhereQuery implements PromiseLike<void> {
    constructor(
        private model: ModelDefinition<UpdateDataType>,
        knexQuery: knex.QueryBuilder,
        private serializationOptions: SerializationOptions
    ) {
        super(knexQuery);
    }
    
    async execute() {
        let queryResults: any[] = await this.knexQuery;
        return;
    }

    then<TResult>(onfulfilled?: () => TResult | PromiseLike<TResult>, onrejected?: (reason: any) => TResult | PromiseLike<TResult>): PromiseLike<TResult> {
        return this.knexQuery.then(onfulfilled, onrejected);
    }
}

export class InsertQuery<InsertDataType> extends BaseQuery implements PromiseLike<void> {
    constructor(
        private model: ModelDefinition<InsertDataType>,
        knexQuery: knex.QueryBuilder,
        private serializationOptions: SerializationOptions
    ) {
        super(knexQuery);
    }

    async execute() {
        let queryResults: any[] = await this.knexQuery;
        return;
    }

    then<TResult>(onfulfilled?: () => TResult | PromiseLike<TResult>, onrejected?: (reason: any) => TResult | PromiseLike<TResult>): PromiseLike<TResult> {
        return this.knexQuery.then(onfulfilled, onrejected);
    }
}
