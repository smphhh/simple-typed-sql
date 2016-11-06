
import * as knex from 'knex';

import {
    AttributeDefinition,
    DataType,
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

    buildAggregationClause(knexClient: knex, alias?: string): knex.Raw {
        let bindings: string[] = [];
        let expressionString;

        if (this.operandAttribute !== undefined && this.aggregationFunction !== "countDistinct") {
            expressionString = `${this.aggregationFunction}(??)`;
            bindings.push(this.operandAttribute.getAbsoluteFieldName());

        } else if (this.operandAttribute !== undefined) {
            expressionString = "count(distinct ??)";
            bindings.push(this.operandAttribute.getAbsoluteFieldName());

        } else if (this.aggregationFunction !== "countDistinct") {
            expressionString = `${this.aggregationFunction}(*)`;

        } else {
            throw new Error("Invalid aggregation");
        }

        if (alias !== undefined) {
            expressionString += " as ??";
            bindings.push(alias);
        }

        return knexClient.raw(expressionString, bindings);
    }

    getAttributeDefinition(alias: string) {
        
        let dataType: DataType;

        if (this.operandAttribute !== undefined) {
            dataType = this.operandAttribute.dataType;
        }

        let attributeName = alias;

        if (this.aggregationFunction === "count" || this.aggregationFunction === "countDistinct") {
            dataType = "number";
        }
 
        return new AttributeDefinition(dataType, attributeName, undefined, undefined);
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
