
import * as knex from 'knex';

import {
    AttributeDefinition,
    getAbsoluteFieldName
} from './definition';

export type ComparisonOperator = '<' | '>' | '<=' | '>=' | '=' | '!=';
export type LogicalOperator = 'and' | 'or';

export type WhereConditionCallback = (queryBuilder: knex.QueryBuilder) => any;
export type JoinConditionCallback = (queryBuilder: knex.QueryBuilder) => any;

export abstract class ConditionClause {
    abstract buildWhereConditionClause(knexClient: knex): WhereConditionCallback;
    abstract buildJoinConditionClause(knexClient: knex): JoinConditionCallback;
}

export class LogicalClause extends ConditionClause {
    constructor(
        public operator: LogicalOperator,
        public operands: ConditionClause[]
    ) {
        super();
    }

    buildWhereConditionClause(knexClient: knex) {
        let that = this;
        let whereFunctionName = this.operator === 'and' ? 'andWhere' : 'orWhere';
        return function () {
            for (let operand of that.operands) {
                this[whereFunctionName](operand.buildWhereConditionClause(knexClient));
            }
        };
    }

    buildJoinConditionClause(knexClient: knex) {
        let that = this;
        let joinFunctionName = this.operator === 'and' ? 'andOn' : 'orOn';
        return function () {
            for (let operand of that.operands) {
                this[joinFunctionName](operand.buildJoinConditionClause(knexClient));
            }
        };
    }
}

export type ComparisonValueType = string | number | boolean | Date;
export type ComparisonOperandType = AttributeDefinition | ComparisonValueType;

export class ComparisonClause extends ConditionClause {
    constructor(
        public operator: ComparisonOperator,
        public operand1: ComparisonOperandType,
        public operand2: ComparisonOperandType
    ) {
        super();
    }

    buildWhereConditionClause(knexClient: knex) {
        let that = this;
        return function () {
            this.where(
                ComparisonClause.makeRawExpression(knexClient, that.operand1),
                that.operator,
                ComparisonClause.makeRawExpression(knexClient, that.operand2)
            );
        };
    }

    buildJoinConditionClause(knexClient: knex) {
        let that = this;
        return function () {
            this.on(
                ComparisonClause.makeRawExpression(knexClient, that.operand1),
                that.operator,
                ComparisonClause.makeRawExpression(knexClient, that.operand2)
            )
        };
    }

    private static makeRawExpression(knexClient: knex, operand: ComparisonOperandType) {
        if (operand instanceof AttributeDefinition) {
            return knexClient.raw('??', getAbsoluteFieldName(operand));
        } else {
            return knexClient.raw('?', operand);
        }
    }
}

export function and(...conditions: ConditionClause[]) {
    return new LogicalClause("and", conditions);
}

export function or(...conditions: ConditionClause[]) {
    return new LogicalClause("or", conditions);
}

export function comparison<T extends ComparisonValueType>(operand1: T, operator: ComparisonOperator, operand2: T) {
    return new ComparisonClause(operator, operand1, operand2);
}

export function equal<T extends ComparisonValueType>(operand1: T, operand2: T) {
    return comparison(operand1, '=', operand2);
}