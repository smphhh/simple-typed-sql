
import * as knex from 'knex';

import {
    AttributeDefinition,
    AttributeDefinitionMap,
    getAttributes,
    getAbsoluteFieldName,
    getAbsoluteFieldNames,
    getFieldNames,
    getTableName,
    getIdentityAliasedName,
    getAbsoluteFieldNameAttributeDefinitionMap,
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

        let selectColumns = getAbsoluteFieldNames(model).map(getIdentityAliasedName);

        let query = this.knexBuilder.select(selectColumns).from(tableName);

        return new SelectQuery<T>(
            getAbsoluteFieldNameAttributeDefinitionMap(model),
            query,
            this.options
        );
    }

    updateWith<T extends U, U>(model: ModelDefinition<T>, data: U) {
        let tableName = getTableName(model);
        let fieldData = serializeData(model, data, this.options);

        let query = this.knexBuilder.table(tableName).update(fieldData);

        return new UpdateQuery(model, query, this.options);
    }

    insertInto<T, U extends T>(model: ModelDefinition<T>, data: U) {
        let query = this.knexBuilder
            .insert(serializeData(model, data as T, this.options))
            .into(model.__metadata.tableName);

        return new InsertQuery(model, query, this.options);
    }

    deleteFrom<T>(model: ModelDefinition<T>) {
        let query = this.knexBuilder
            .from(getTableName(model))
            .del();
        
        return new DeleteQuery(model, query, this.options);
    }

    truncate<T>(model: ModelDefinition<T>): Promise<void> {
        return Promise.resolve(this.knexBuilder.table(model.__metadata.tableName).truncate());
    }

    from<T>(model: ModelDefinition<T>) {
        let query = this.knexBuilder.from(getTableName(model));
        return new SimpleFromQuery(model, query, this.options);
    }

    async tryFindOneByKey<T extends U, U>(model: ModelDefinition<T>, key: U) {
        let attributeNames = getAttributes(model);
        let whereConditions = serializeData(model, key, this.options);
        let fieldNames = getAbsoluteFieldNames(model);
        let query = this.knexBuilder.select(fieldNames.map(getIdentityAliasedName)).from(getTableName(model));
        
        if (Object.keys(whereConditions).length > 0) {
            query = query.where(whereConditions);
        }

        let data = await query.limit(1);
        if (data.length === 1) {
            return deserializeData<T>(getAbsoluteFieldNameAttributeDefinitionMap(model), data[0], this.options);
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

export class SimpleFromQuery extends BaseQuery {
    private models: Map<string, ModelDefinition<any>>;

    constructor(
        model: ModelDefinition<any>,
        knexQuery: knex.QueryBuilder,
        private serializationOptions: SerializationOptions
    ) {
        super(knexQuery);

        this.models = new Map();
        this.models.set(getTableName(model), model);
    }

    innerJoin<T>(joinModel: ModelDefinition<any>, field1: T, field2: T) {
        return this.join("innerJoin", joinModel, field1, field2);
    }

    leftOuterJoin<T>(joinModel: ModelDefinition<any>, field1: T, field2: T) {
        return this.join("leftOuterJoin", joinModel, field1, field2);
    }

    rightOuterJoin<T>(joinModel: ModelDefinition<any>, field1: T, field2: T) {
        return this.join("rightOuterJoin", joinModel, field1, field2);
    }

    select<T>(selectClause: T) {
        let fieldMap: AttributeDefinitionMap = {};
        Object.keys(selectClause).forEach(key => {
            let attributeDefinition: AttributeDefinition = selectClause[key];
            let tableName = attributeDefinition.tableName;
            if (!this.models.has(tableName)) {
                throw new Error(`Invalid select expression for attribute "${key}": the table ${tableName} is missing a from-clause entry.`);
            }
            fieldMap[getAbsoluteFieldName(attributeDefinition)] = Object.assign(
                {},
                attributeDefinition,
                { attributeName: key }
            );
        });

        let knexQuery = this.knexQuery.select(
            Object.keys(fieldMap).map(fieldName => `${fieldName} as ${fieldName}`)
        );

        return new SelectQuery<T>(fieldMap, knexQuery, this.serializationOptions);
    }

    private join<T>(joinType: "innerJoin" | "leftOuterJoin" | "rightOuterJoin", joinModel: ModelDefinition<any>, field1: T, field2: T) {
        let attribute1: AttributeDefinition = field1 as any;
        let attribute2: AttributeDefinition = field2 as any;
        let joinTableName = getTableName(joinModel);
        if (this.models.has(joinTableName)) {
            throw new Error(`Invalid join. The same table (${joinTableName}) can be referred to in one from-clause only in SimpleFromQuery.`);
        }
        this.models.set(joinTableName, joinModel);
        this.knexQuery = this.knexQuery[joinType](
            getTableName(joinModel),
            getAbsoluteFieldName(attribute1),
            getAbsoluteFieldName(attribute2)
        );

        return this;
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
        this.knexQuery = this.knexQuery.andWhere(getAbsoluteFieldName(attributeDefinition), operator, value as any);
        return this;
    }
}

export class SelectQuery<ResultType> extends WhereQuery {
    constructor(
        private fields: AttributeDefinitionMap,
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
        this.knexQuery = this.knexQuery.orderBy(getAbsoluteFieldName(attributeDefinition), direction);
        return this;
    }

    async tryGetOne(): Promise<ResultType> {
        let data = await this.execute();
        if (data.length === 1) {
            return data[0];
        } else {
            return null;
        }
    }

    async getOne() {
        let data = await this.tryGetOne();
        if (data === null) {
            throw new Error("Expected exactly one row");
        } else {
            return data;
        }
    }

    async execute() {
        let queryResults: any[] = await this.knexQuery;
        return queryResults.map(result => deserializeData<ResultType>(this.fields, result, this.serializationOptions));
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

export class DeleteQuery<UpdateDataType> extends WhereQuery implements PromiseLike<void> {
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
