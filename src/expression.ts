
import * as knex from 'knex';

import {
    AttributeDefinition,
    ValueType
} from './definition';

import {
    BaseMappingData
} from './mapping';

export type AggregationFunction = 'sum' | 'avg' | 'count' | 'countDistinct' | 'max' | 'min';

export type AggregationOperandType = AttributeDefinition | ValueType;

export class AggregationExpression {
    private operandAttribute: AttributeDefinition;

    constructor(
        private aggregationFunction: AggregationFunction,
        operand?: AttributeDefinition | ValueType
    ) {
        if (operand instanceof AttributeDefinition) {
            this.operandAttribute = operand;
        } else if (operand === undefined) {

        } else {
            throw new Error(`Invalid aggregation function argument: ${operand}`);
        }
    }

    buildAggregationClause(knexClient: knex, alias: string): knex.Raw {
        if (this.operandAttribute !== undefined && this.aggregationFunction !== "countDistinct") {
            return knexClient.raw(`${this.aggregationFunction}(??) as ??`, [BaseMappingData.getAbsoluteFieldName(this.operandAttribute), alias]);

        } else if (this.operandAttribute !== undefined) {
            return knexClient.raw(`count(distinct ??) as ??`, [BaseMappingData.getAbsoluteFieldName(this.operandAttribute), alias]);

        } else {
            return knexClient.raw(`${this.aggregationFunction}(*) as ??`, [alias]);
        }
    }

    getAttributeDefinition(alias: string) {
        let attributeDefinition = new AttributeDefinition();
        
        if (this.operandAttribute !== undefined) {
            Object.assign(
                attributeDefinition,
                this.operandAttribute
            );
        }

        attributeDefinition.attributeName = alias;

        if (this.aggregationFunction === "count" || this.aggregationFunction === "countDistinct") {
            attributeDefinition.dataType = "number";
        }
 
        return attributeDefinition;
    }
}

export function sum<T extends ValueType>(operand: T): T {
    return new AggregationExpression("sum", operand) as any;
}

export function avg<T extends ValueType>(operand: T): T {
    return new AggregationExpression("avg", operand) as any;
}

export function max<T extends ValueType>(operand: T): T {
    return new AggregationExpression("max", operand) as any;
}

export function min<T extends ValueType>(operand: T): T {
    return new AggregationExpression("avg", operand) as any;
}

export function count(operand?: AggregationOperandType): number {
    return new AggregationExpression("count", operand) as any;
}

export function countDistinct(operand?: AggregationOperandType): number {
    return new AggregationExpression("countDistinct", operand) as any;
}
