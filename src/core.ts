
import * as knex from 'knex';

import {
    AttributeDefinition,
    AttributeDefinitionMap,
    SerializationOptions,
    ValueType
} from './definition';

import {
    ComparisonOperator,
    ConditionClause
} from './condition';

import {
    AggregationExpression
} from './expression';

import {
    BaseMappingData,
    defineMapping,
    Mapping,
    WrappedMappingData
} from './mapping';

import {
    deserializeData,
    serializeData
} from './serialization';

export class BaseMapper {
    constructor(
        protected knexClient: knex,
        private knexBuilder: knex.QueryInterface,
        protected options: SerializationOptions
    ) {
    }

    selectAllFrom<T>(mapping: Mapping<T>) {
        let mappingData = WrappedMappingData.getMappingData(mapping);
        return this.from(mapping).select(mappingData.getAttributeDefinitionMap() as any);
    }

    selectCountFrom<T>(mapping: Mapping<T>) {
        let mappingData = WrappedMappingData.getMappingData(mapping);

        let alias = "value";
        let aggregationExpression = new AggregationExpression("count");
        let fieldMap: AttributeDefinitionMap = { value: aggregationExpression.getAttributeDefinition(alias) };
        let knexQuery = this.knexBuilder
            .select(aggregationExpression.buildAggregationClause(this.knexClient, alias))
            .from(mappingData.getTableName());

        return new SingleValueSelectQuery<number>(this.knexClient, fieldMap, knexQuery, this.options);
    }

    updateWith<T extends U, U>(wrappedMapping: Mapping<T>, data: U) {
        let mapping = WrappedMappingData.getMappingData(wrappedMapping);
        let tableName = mapping.getTableName();
        let fieldData = serializeData(mapping, data, this.options);

        let query = this.knexBuilder.table(tableName).update(fieldData);

        return new UpdateQuery(this.knexClient, mapping, query, this.options);
    }

    insertInto<T, U extends T>(wrappedMapping: Mapping<T>, data: U) {
        let mapping = WrappedMappingData.getMappingData(wrappedMapping);
        let query = this.knexBuilder
            .insert(serializeData(mapping, data as T, this.options))
            .into(mapping.getTableName());

        return new InsertQuery(this.knexClient, mapping, query, this.options);
    }

    batchInsertInto<T, U extends T>(wrappedMapping: Mapping<T>, data: U[]) {
        let mapping = WrappedMappingData.getMappingData(wrappedMapping);
        let serializedData = data.map(item => serializeData(mapping, item as T, this.options));
        let query = this.knexBuilder
            .insert(serializedData)
            .into(mapping.getTableName());

        return new InsertQuery(this.knexClient, mapping, query, this.options);
    }

    deleteFrom<T>(wrappedMapping: Mapping<T>) {
        let mapping = WrappedMappingData.getMappingData(wrappedMapping);
        let query = this.knexBuilder
            .from(mapping.getTableName())
            .del();

        return new DeleteQuery(this.knexClient, mapping, query, this.options);
    }

    truncate<T>(wrappedMapping: Mapping<T>): Promise<void> {
        let mapping = WrappedMappingData.getMappingData(wrappedMapping);
        return Promise.resolve(this.knexBuilder.table(mapping.getTableName()).truncate());
    }

    from<T>(wrappedMapping: Mapping<T>) {
        let mapping = WrappedMappingData.getMappingData(wrappedMapping);
        let query = this.knexBuilder.from(mapping.getTableName());
        return new FromQuery(this.knexClient, query, mapping, this.options);
    }

    async tryFindOneByKey<T extends U, U>(wrappedMapping: Mapping<T>, key: U) {
        let mapping = WrappedMappingData.getMappingData(wrappedMapping);
        let attributeNames = mapping.getAttributes();
        let whereConditions = serializeData(mapping, key, this.options);
        let fieldNames = mapping.getAbsoluteFieldNames();
        let query = this.knexBuilder.select(mapping.getAttributeDefinitions().map(BaseMappingData.getAliasedAttributeName)).from(mapping.getTableName());

        if (Object.keys(whereConditions).length > 0) {
            query = query.where(whereConditions);
        }

        let data = await query.limit(1);
        if (data.length === 1) {
            return deserializeData<T>(mapping.getAttributeDefinitionMap(), data[0], this.options);
        } else {
            return null as T;
        }
    }

    async findOneByKey<T extends U, U>(wrappedMapping: Mapping<T>, key: U) {
        let data = await this.tryFindOneByKey(wrappedMapping, key);
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
        knexClient: knex,
        options: SerializationOptions = {}
    ) {
        super(knexClient, knexClient, options);
    }

    transaction<T>(callback: (transactionMapper: BaseMapper) => Promise<T>) {
        return Promise.resolve<T>(this.knexClient.transaction((trx) => {
            let t: knex.Transaction;
            let transactionMapper = new BaseMapper(this.knexClient, trx, this.options);
            return callback(transactionMapper);
        }));
    }
}

export class BaseQuery {
    constructor(
        protected knexClient: knex,
        protected knexQuery: knex.QueryBuilder
    ) {

    }

    getKnexQuery() {
        return this.knexQuery;
    }

    toString() {
        return this.knexQuery.toString();
    }
}

export type JoinType = "innerJoin" | "leftOuterJoin" | "rightOuterJoin";

export class FromQuery<SourceType> extends BaseQuery {
    private mappings: Map<string, BaseMappingData<any>>;

    constructor(
        knexClient: knex,
        knexQuery: knex.QueryBuilder,
        mapping: BaseMappingData<SourceType>,
        private serializationOptions: SerializationOptions
    ) {
        super(knexClient, knexQuery);

        this.mappings = new Map();
        this.mappings.set(mapping.getTableName(), mapping);
    }

    innerJoin<T>(joinMapping: Mapping<T>, condition: ConditionClause) {
        return this.conditionJoin("innerJoin", joinMapping, condition);
    }

    innerJoinEqual<T>(joinMapping: Mapping<any>, field1: T, field2: T) {
        return this.simpleJoin("innerJoin", joinMapping, field1, field2);
    }

    leftOuterJoin<T>(joinMapping: Mapping<T>, condition: ConditionClause) {
        return this.conditionJoin("leftOuterJoin", joinMapping, condition);
    }

    leftOuterJoinEqual<T>(joinMapping: Mapping<any>, field1: T, field2: T) {
        return this.simpleJoin("leftOuterJoin", joinMapping, field1, field2);
    }

    rightOuterJoin<T>(joinMapping: Mapping<T>, condition: ConditionClause) {
        return this.conditionJoin("rightOuterJoin", joinMapping, condition);
    }

    rightOuterJoinEqual<T>(joinMapping: Mapping<any>, field1: T, field2: T) {
        return this.simpleJoin("rightOuterJoin", joinMapping, field1, field2);
    }

    select<T>(selectInput: T) {
        return SelectQuery.createFromSelect(
            this.knexClient,
            this.knexQuery,
            this.mappings,
            this.serializationOptions,
            selectInput
        );
    }

    selectAll<T>(mapping: Mapping<T>) {
        let mappingData = WrappedMappingData.getMappingData(mapping);
        return SelectQuery.createFromSelect<T>(
            this.knexClient,
            this.knexQuery,
            this.mappings,
            this.serializationOptions,
            mappingData.getAttributeDefinitionMap() as any
        );
    }

    private simpleJoin<T, U>(joinType: JoinType, wrappedJoinMapping: Mapping<U>, field1: T, field2: T) {
        let joinMapping = WrappedMappingData.getMappingData(wrappedJoinMapping);
        let attribute1: AttributeDefinition = field1 as any;
        let attribute2: AttributeDefinition = field2 as any;
        let joinTableName = joinMapping.getTableName();
        if (this.mappings.has(joinTableName)) {
            throw new Error(`Invalid join. The same table (${joinTableName}) can be referred to in one from-clause only in SimpleFromQuery.`);
        }
        this.mappings.set(joinTableName, joinMapping);
        this.knexQuery = this.knexQuery[joinType](
            joinMapping.getTableName(),
            BaseMappingData.getAbsoluteFieldName(attribute1),
            BaseMappingData.getAbsoluteFieldName(attribute2)
        );

        return this;
    }

    private conditionJoin<T>(joinType: JoinType, wrappedJoinMapping: Mapping<T>, conditionClause: ConditionClause) {
        let joinMapping = WrappedMappingData.getMappingData(wrappedJoinMapping);
        let joinTableName = joinMapping.getTableName();
        if (this.mappings.has(joinTableName)) {
            throw new Error(`Invalid join. The same table (${joinTableName}) can be referred to in one from-clause only in SimpleFromQuery.`);
        }
        this.mappings.set(joinTableName, joinMapping);

        this.knexQuery = this.knexQuery[joinType](
            joinMapping.getTableName(),
            conditionClause.buildJoinConditionClause(this.knexClient)
        );

        return this;
    }
}

export class WhereQuery extends BaseQuery {
    constructor(
        knexClient: knex,
        knexQuery: knex.QueryBuilder
    ) {
        super(knexClient, knexQuery);
    }

    where(clause: ConditionClause) {
        this.knexQuery = this.knexQuery.where(clause.buildWhereConditionClause(this.knexClient));
        return this;
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
        this.knexQuery = this.knexQuery.andWhere(BaseMappingData.getAbsoluteFieldName(attributeDefinition), operator, value as any);
        return this;
    }
}

interface Doo {
    [key: string]: string | knex.Raw;
}

export interface SelectDefinition {
    [key: string]: string | knex.Raw;
}

export class SelectQuery<ResultType> extends WhereQuery {
    constructor(
        knexClient: knex,
        knexQuery: knex.QueryBuilder,
        private mappings: Map<string, BaseMappingData<any>>,
        private serializationOptions: SerializationOptions,
        private fields: AttributeDefinitionMap,
        private selectDefinition: SelectDefinition
    ) {
        super(knexClient, knexQuery);
    }

    /*select<T>(selectInput: T) {
        let attributeData = SelectQuery.parseSelectInput(this.knexClient, this.mappings, selectInput);
        let fieldMap = Object.assign({}, this.fields, attributeData.fieldMap);
        let selectDefinition = Object.assign({}, this.selectDefinition, attributeData.selectDefinition);
        return new SelectQuery<ResultType & T>(
            this.knexClient,
            this.knexQuery,
            this.mappings,
            this.serializationOptions,
            fieldMap,
            selectDefinition
        );
    }

    selectAll<T>(mapping: Mapping<T>) {
        let mappingData = WrappedMappingData.getMappingData(mapping);
        return this.select<T>(mappingData.getAttributeDefinitionMap() as any);
    }*/

    private static parseSelectInput<T>(knexClient: knex, mappings: Map<string, BaseMappingData<any>>, input: T) {
        return Object.keys(input).reduce((attributeData, key) => {
            let expression: AttributeDefinition | AggregationExpression = input[key];

            if (expression instanceof AttributeDefinition) {
                let attributeDefinition: AttributeDefinition = input[key];
                let tableName = attributeDefinition.tableName;
                if (!mappings.has(tableName)) {
                    throw new Error(`Invalid select expression for attribute "${key}": the table ${tableName} is missing a from-clause entry.`);
                }

                let newAttributeDefinition = new AttributeDefinition();
                Object.assign(
                    newAttributeDefinition,
                    attributeDefinition,
                    { attributeName: key }
                );

                attributeData.fieldMap[key] = newAttributeDefinition;
                attributeData.selectDefinition[key] = BaseMappingData.getAliasedAttributeName(newAttributeDefinition);

                //return BaseMappingData.getAliasedAttributeName(newAttributeDefinition);

            } else if (expression instanceof AggregationExpression) {
                attributeData.fieldMap[key] = expression.getAttributeDefinition(key);
                attributeData.selectDefinition[key] = expression.buildAggregationClause(knexClient, key);
            }

            return attributeData;

        }, { fieldMap: {} as AttributeDefinitionMap, selectDefinition: {} as SelectDefinition });
    }

    static createFromSelect<T>(
        knexClient: knex,
        knexQuery: knex.QueryBuilder,
        mappings: Map<string, BaseMappingData<any>>,
        serializationOptions: SerializationOptions,
        selectInput: T
    ) {
        let attributeData = SelectQuery.parseSelectInput(knexClient, mappings, selectInput);
        return new SelectQuery<T>(
            knexClient,
            knexQuery,
            mappings,
            serializationOptions,
            attributeData.fieldMap,
            attributeData.selectDefinition
        );
    }

    forUpdate() {
        this.knexQuery.forUpdate();
        return this;
    }

    limit(count: number) {
        this.knexQuery.limit(count);
        return this;
    }

    offset(offset: number) {
        this.knexQuery.offset(offset);
        return this;
    }

    orderBy(attribute: AttributeDefinition | AggregationExpression | ValueType, direction: 'asc' | 'desc') {
        if (attribute instanceof AttributeDefinition) {
            this.knexQuery.orderBy(BaseMappingData.getAbsoluteFieldName(attribute), direction);
            return this;

        } else if (attribute instanceof AggregationExpression) {
            this.knexQuery.orderBy(attribute.buildAggregationClause(this.knexClient) as any, direction);
            return this;

        } else {
            throw new Error(`Invalid order by attribute: ${attribute}`);
        }
    }

    groupBy(...attributes: (AttributeDefinition | ValueType)[]) {
        let fieldNames = attributes.map(item => {
            if (item instanceof AttributeDefinition) {
                return BaseMappingData.getAbsoluteFieldName(item);
            } else {
                throw new Error(`Invalid group by attribute: ${item}`);
            }
        });

        this.knexQuery.groupBy(fieldNames);

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
        let query = this.knexQuery.select(Object.keys(this.selectDefinition).map(item => this.selectDefinition[item]));
        let queryResults: any[] = await query;
        return queryResults.map(result => deserializeData<ResultType>(this.fields, result, this.serializationOptions));
    }

    then<TResult>(onfulfilled?: (value: ResultType[]) => TResult | PromiseLike<TResult>, onrejected?: (reason: any) => TResult | PromiseLike<TResult>): PromiseLike<TResult> {
        return this.execute().then(onfulfilled, onrejected);
    }
}

export class SingleValueSelectQuery<ResultType> extends WhereQuery {
    constructor(
        knexClient: knex,
        private fields: AttributeDefinitionMap,
        knexQuery: knex.QueryBuilder,
        private serializationOptions: SerializationOptions
    ) {
        super(knexClient, knexQuery);
    }

    async execute() {
        let queryResults: any[] = await this.knexQuery;
        if (queryResults.length === 0) {
            return null as ResultType;

        } else if (queryResults.length === 1) {
            return deserializeData<{ value: ResultType }>(this.fields, queryResults[0], this.serializationOptions).value;

        } else {
            throw new Error(`Invalid query result with length ${queryResults.length}`);
        }
    }

    then<TResult>(onfulfilled?: (value: ResultType) => TResult | PromiseLike<TResult>, onrejected?: (reason: any) => TResult | PromiseLike<TResult>): PromiseLike<TResult> {
        return this.execute().then(onfulfilled, onrejected);
    }
}

export class UpdateQuery<UpdateDataType> extends WhereQuery implements PromiseLike<number> {
    constructor(
        knexClient: knex,
        private mapping: BaseMappingData<UpdateDataType>,
        knexQuery: knex.QueryBuilder,
        private serializationOptions: SerializationOptions
    ) {
        super(knexClient, knexQuery);
    }

    async execute() {
        let rowCount: number = await this.knexQuery;
        return rowCount;
    }

    then<TResult>(onfulfilled?: (rowCount: number) => TResult | PromiseLike<TResult>, onrejected?: (reason: any) => TResult | PromiseLike<TResult>): PromiseLike<TResult> {
        return this.execute().then(onfulfilled, onrejected);
    }
}

export class DeleteQuery<UpdateDataType> extends WhereQuery implements PromiseLike<void> {
    constructor(
        knexClient: knex,
        private mapping: BaseMappingData<UpdateDataType>,
        knexQuery: knex.QueryBuilder,
        private serializationOptions: SerializationOptions
    ) {
        super(knexClient, knexQuery);
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
        knexClient: knex,
        private mapping: BaseMappingData<InsertDataType>,
        knexQuery: knex.QueryBuilder,
        private serializationOptions: SerializationOptions
    ) {
        super(knexClient, knexQuery);
    }

    returningAll() {
        let returnColumns = this.mapping.getAttributeDefinitions().map(BaseMappingData.getAliasedAttributeName);
        let knexQuery = this.knexQuery.returning(returnColumns);

        return new ReturningInsertQuery<InsertDataType>(
            this.knexClient,
            knexQuery,
            this.mapping.getAttributeDefinitionMap(),
            this.serializationOptions
        );
    }

    async execute() {
        let queryResults: any[] = await this.knexQuery;
        return;
    }

    then<TResult>(onfulfilled?: () => TResult | PromiseLike<TResult>, onrejected?: (reason: any) => TResult | PromiseLike<TResult>): PromiseLike<TResult> {
        return this.knexQuery.then(onfulfilled, onrejected);
    }
}

export class ReturningInsertQuery<ResultType> extends BaseQuery {
    constructor(
        knexClient: knex,
        knexQuery: knex.QueryBuilder,
        private fields: AttributeDefinitionMap,
        private serializationOptions: SerializationOptions
    ) {
        super(knexClient, knexQuery);
    }

    async execute() {
        let queryResults: any[] = await this.knexQuery;
        return queryResults.map(result => deserializeData<ResultType>(this.fields, result, this.serializationOptions));
    }

    then<TResult>(onfulfilled?: (value: ResultType[]) => TResult | PromiseLike<TResult>, onrejected?: (reason: any) => TResult | PromiseLike<TResult>): PromiseLike<TResult> {
        return this.execute().then(onfulfilled, onrejected);
    }
}
