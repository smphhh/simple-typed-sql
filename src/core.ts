
import * as knex from 'knex';

import {
    SerializationOptions,
    ValueType
} from './definition';

import {
    comparison,
    ComparisonOperator,
    ComparisonValueType,
    ConditionClause
} from './condition';

import {
    BaseAggregationExpression,
    AggregationExpression
} from './expression';

import {
    BaseAttribute,
    AttributeDefinitionMap,
    BaseMappingData,
    defineMapping,
    Mapping,
    OperandType,
    Attribute,
    AttributeMap,
    WrappedMappingData
} from './mapping';

import {
    deserializeData,
    serializeData
} from './serialization';

export class BaseMapper {
    constructor(
        protected knexClient: knex,
        private knexInterface: knex.QueryInterface,
        protected options: SerializationOptions
    ) {
    }

    selectAllFrom<T>(mapping: Mapping<T>) {
        return this.from(mapping).selectAll(mapping);
    }

    selectCountFrom<T>(mapping: Mapping<T>) {
        let mappingData = WrappedMappingData.getMappingData(mapping);

        let alias = "value";
        let aggregationExpression = new BaseAggregationExpression("count");
        let fieldMap: AttributeDefinitionMap = { value: aggregationExpression.getAttributeDefinition(alias) };
        let knexQuery = this.knexInterface
            .select(aggregationExpression.buildAggregationClause(this.knexClient, alias))
            .from(mappingData.getTableName());

        return new SingleValueSelectQuery<number>(this.knexClient, fieldMap, knexQuery, this.options);
    }

    updateWith<T extends U, U>(mapping: Mapping<T>, data: U) {
        return UpdateQuery.updateWith(this.knexClient, this.knexInterface, this.options, mapping, data);
    }

    update<T>(mapping: Mapping<T>) {
        return UpdateQuery.update(this.knexClient, this.knexInterface, this.options, mapping);
    }

    insertInto<T, U extends T>(mapping: Mapping<T>, data: U) {
        return InsertQuery.insertInto(this.knexClient, this.knexInterface, this.options, mapping, data);
    }

    batchInsertInto<T, U extends T>(wrappedMapping: Mapping<T>, data: U[]) {
        return InsertQuery.batchInsertInto(this.knexClient, this.knexInterface, this.options, wrappedMapping, data);
    }

    deleteFrom<T>(wrappedMapping: Mapping<T>) {
        let mapping = WrappedMappingData.getMappingData(wrappedMapping);
        let query = this.knexInterface
            .from(mapping.getTableName())
            .del();

        return new DeleteQuery(this.knexClient, mapping, query, this.options);
    }

    truncate<T>(wrappedMapping: Mapping<T>): Promise<void> {
        let mapping = WrappedMappingData.getMappingData(wrappedMapping);
        return Promise.resolve(this.knexInterface.table(mapping.getTableName()).truncate());
    }

    from<T>(wrappedMapping: Mapping<T>) {
        let mapping = WrappedMappingData.getMappingData(wrappedMapping);
        let query = this.knexInterface.from(mapping.getTableName());
        return new FromQuery(this.knexClient, query, mapping, this.options);
    }

    async tryFindOneByKey<T extends U, U>(wrappedMapping: Mapping<T>, key: U) {
        let mapping = WrappedMappingData.getMappingData(wrappedMapping);
        let attributeNames = mapping.getAttributeNames();
        let whereConditions = serializeData(mapping, key, this.options);
        let fieldNames = mapping.getAbsoluteFieldNames();
        let query = this.knexInterface
            .select(
                mapping.getAttributeDefinitions().map(item => item.getAliasedAttributeName())
            )
            .from(mapping.getTableName()
        );

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

export type SelectDefinition<T> = Attribute<T> | AggregationExpression<T>;  
export type BaseSelectDefinitionMap = { [key: string]: SelectDefinition<any>; };
export type SelectDefinitionMap<T> = {
    [P in keyof T]: SelectDefinition<T[P]>;
}
export type SelectExpression<T> = SelectDefinitionMap<T> & BaseSelectDefinitionMap;

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

    innerJoinEqual<T>(joinMapping: Mapping<any>, attribute1: Attribute<T>, attribute2: Attribute<T>) {
        return this.simpleJoin("innerJoin", joinMapping, attribute1, attribute2);
    }

    leftOuterJoin<T>(joinMapping: Mapping<T>, condition: ConditionClause) {
        return this.conditionJoin("leftOuterJoin", joinMapping, condition);
    }

    leftOuterJoinEqual<T>(joinMapping: Mapping<any>, attribute1: Attribute<T>, attribute2: Attribute<T>) {
        return this.simpleJoin("leftOuterJoin", joinMapping, attribute1, attribute2);
    }

    rightOuterJoin<T>(joinMapping: Mapping<T>, condition: ConditionClause) {
        return this.conditionJoin("rightOuterJoin", joinMapping, condition);
    }

    rightOuterJoinEqual<T>(joinMapping: Mapping<any>, attribute1: Attribute<T>, attribute2: Attribute<T>) {
        return this.simpleJoin("rightOuterJoin", joinMapping, attribute1, attribute2);
    }

    select<T>(selectInput: SelectExpression<T>) {
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
            SelectQuery.selectAll(mapping)
        );
    }

    private simpleJoin<T, U>(joinType: JoinType, wrappedJoinMapping: Mapping<U>, attribute1: Attribute<T>, attribute2: Attribute<T>) {
        let joinMapping = WrappedMappingData.getMappingData(wrappedJoinMapping);
        let joinTableName = joinMapping.getTableName();
        if (this.mappings.has(joinTableName)) {
            throw new Error(`Invalid join. The same table (${joinTableName}) can be referred to in one from-clause only in SimpleFromQuery.`);
        }
        this.mappings.set(joinTableName, joinMapping);
        this.knexQuery = this.knexQuery[joinType](
            joinMapping.getTableName(),
            Attribute.getBaseAttribute(attribute1).getAbsoluteFieldName(),
            Attribute.getBaseAttribute(attribute2).getAbsoluteFieldName()
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
            conditionClause.buildJoinConditionClause(this.knexClient) as any
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

    whereEqual<T extends ComparisonValueType>(operand1: T | Attribute<T>, operand2: T | Attribute<T>) {
        return this.where(comparison(operand1, '=', operand2));
    }

    whereLess<T extends ComparisonValueType>(operand1: T | Attribute<T>, operand2: T | Attribute<T>) {
        return this.where(comparison(operand1, '<', operand2));
    }

    whereLessOrEqual<T extends ComparisonValueType>(operand1: T | Attribute<T>, operand2: T | Attribute<T>) {
        return this.where(comparison(operand1, '<=', operand2));
    }

    whereGreater<T extends ComparisonValueType>(operand1: T | Attribute<T>, operand2: T | Attribute<T>) {
        return this.where(comparison(operand1, '>', operand2));
    }

    whereGreaterOrEqual<T extends ComparisonValueType>(operand1: T | Attribute<T>, operand2: T | Attribute<T>) {
        return this.where(comparison(operand1, '>=', operand2));
    }
}

export interface SelectQueryDefinition {
    [key: string]: string | knex.Raw;
}

export class SelectQuery<ResultType> extends WhereQuery {
    constructor(
        knexClient: knex,
        knexQuery: knex.QueryBuilder,
        private mappings: Map<string, BaseMappingData<any>>,
        private serializationOptions: SerializationOptions,
        private fields: AttributeDefinitionMap
    ) {
        super(knexClient, knexQuery);
    }

    private static parseSelectInput<T>(knexClient: knex, mappings: Map<string, BaseMappingData<any>>, input: SelectExpression<T>) {
        return Object.keys(input).reduce((attributeData, key) => {
            let expression = input[key];

            if (expression instanceof Attribute) {
                let attributeDefinition = Attribute.getBaseAttribute(expression);
                let attributeMappingData = WrappedMappingData.getMappingData(attributeDefinition.mapping);
                let tableName = attributeMappingData.getTableName();
                if (!mappings.has(tableName)) {
                    throw new Error(`Invalid select expression for attribute "${key}": the table ${tableName} is missing a from-clause entry.`);
                }

                let newAttributeDefinition = new BaseAttribute(
                    attributeDefinition.mapping,
                    attributeDefinition.dataType,
                    key,
                    attributeDefinition.fieldName
                );

                attributeData.fieldMap[key] = newAttributeDefinition;
                attributeData.selectDefinition[key] = newAttributeDefinition.getAliasedAttributeName();

                //return BaseMappingData.getAliasedAttributeName(newAttributeDefinition);

            } else if (expression instanceof AggregationExpression) {
                attributeData.fieldMap[key] = expression.getAttributeDefinition(key);
                attributeData.selectDefinition[key] = expression.buildAggregationClause(knexClient, key);
            }

            return attributeData;

        }, { fieldMap: {} as AttributeDefinitionMap, selectDefinition: {} as SelectQueryDefinition });
    }

    static createFromSelect<T>(
        knexClient: knex,
        knexQuery: knex.QueryBuilder,
        mappings: Map<string, BaseMappingData<any>>,
        serializationOptions: SerializationOptions,
        selectInput: SelectExpression<T>
    ) {
        let attributeData = SelectQuery.parseSelectInput(knexClient, mappings, selectInput);
        let query = knexQuery.select(Object.keys(attributeData.selectDefinition).map(item => attributeData.selectDefinition[item]));
        return new SelectQuery<T>(
            knexClient,
            query,
            mappings,
            serializationOptions,
            attributeData.fieldMap
        );
    }

    static selectAll<T>(mapping: Mapping<T>): SelectExpression<T> {
        let mappingData = WrappedMappingData.getMappingData(mapping);
        return mappingData.getAttributeMap() as any;
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

    orderBy(attribute: Attribute<any> | AggregationExpression<any>, direction: 'asc' | 'desc') {
        if (attribute instanceof Attribute) {
            this.knexQuery.orderBy(Attribute.getBaseAttribute(attribute).getAbsoluteFieldName(), direction);
            return this;

        } else if (attribute instanceof AggregationExpression) {
            this.knexQuery.orderBy(attribute.buildAggregationClause(this.knexClient) as any, direction);
            return this;

        } else {
            throw new Error(`Invalid order by attribute: ${attribute}`);
        }
    }

    groupBy(...attributes: Attribute<any>[]) {
        let fieldNames = attributes.map(item => Attribute.getBaseAttribute(item).getAbsoluteFieldName());
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
        let queryResults: any[] = await this.knexQuery;
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

export class UpdateQuery<T, U> extends WhereQuery implements PromiseLike<number> {
    constructor(
        knexClient: knex,
        knexQuery: knex.QueryBuilder,
        private serializationOptions: SerializationOptions,
        private mappingData: BaseMappingData<T>
    ) {
        super(knexClient, knexQuery);
    }

    set<T extends ValueType>(attribute: Attribute<T>, value: T) {
        const baseAttribute = Attribute.getBaseAttribute(attribute);
        this.knexQuery.update(baseAttribute.getFieldName(), value);
        return this;
    }

    static updateWith<T extends U, U>(knexClient: knex, knexInterface: knex.QueryInterface, options: SerializationOptions, mapping: Mapping<T>, data: U) {
        let mappingData = WrappedMappingData.getMappingData(mapping);
        let tableName = mappingData.getTableName();
        let fieldData = serializeData(mappingData, data, options);

        let query = knexInterface.table(tableName).update(fieldData);

        return new UpdateQuery(knexClient, query, options, mappingData);
    }

    static update<T>(knexClient: knex, knexInterface: knex.QueryInterface, options: SerializationOptions, mapping: Mapping<T>) {
        let mappingData = WrappedMappingData.getMappingData(mapping);
        let tableName = mappingData.getTableName();

        let query = knexInterface.table(tableName);

        return new UpdateQuery(knexClient, query, options, mappingData);
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
        knexQuery: knex.QueryBuilder,
        private mapping: BaseMappingData<InsertDataType>,
        private serializationOptions: SerializationOptions
    ) {
        super(knexClient, knexQuery);
    }

    static insertInto<T, U extends T>(
        knexClient: knex,
        knexInterface: knex.QueryInterface,
        options: SerializationOptions,
        mapping: Mapping<T>,
        data: U
    ) {
        let mappingData = WrappedMappingData.getMappingData(mapping);
        let query = knexInterface
            .insert(serializeData(mappingData, data as T, options))
            .into(mappingData.getTableName());

        return new InsertQuery(knexClient, query, mappingData, options);
    }

    static batchInsertInto<T, U extends T>(
        knexClient: knex,
        knexInterface: knex.QueryInterface,
        options: SerializationOptions,
        mapping: Mapping<T>,
        data: U[]
    ) {
        let mappingData = WrappedMappingData.getMappingData(mapping);
        let serializedData = data.map(item => serializeData(mappingData, item as T, options));
        let query = knexInterface
            .insert(serializedData)
            .into(mappingData.getTableName());

        return new InsertQuery(knexClient, query, mappingData, options);
    }

    returningAll() {
        let returnColumns = this.mapping.getAttributeDefinitions().map(item => item.getAliasedAttributeName());
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
