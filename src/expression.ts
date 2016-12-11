
import * as knex from 'knex';

import {
    AttributeTypeName,
    ValueType
} from './definition';

import {
    BaseAttribute,
    BaseMappingData,
    Mapping,
    Attribute
} from './mapping';

export type AggregationFunction = 'sum' | 'avg' | 'count' | 'countDistinct' | 'max' | 'min';

export type AggregationOperandType = BaseAttribute | ValueType;

export class BaseAggregationExpression {
    private operandAttribute: BaseAttribute;

    constructor(
        private aggregationFunction: AggregationFunction,
        operand?: BaseAttribute | ValueType
    ) {
        if (operand instanceof BaseAttribute) {
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
        let mapping: Mapping<{}>;
        let dataType: AttributeTypeName;

        if (this.operandAttribute !== undefined) {
            dataType = this.operandAttribute.dataType;
            mapping = this.operandAttribute.mapping;
        }

        let attributeName = alias;

        if (this.aggregationFunction === "count" || this.aggregationFunction === "countDistinct") {
            dataType = "number";
        }
 
        return new BaseAttribute(mapping, dataType, attributeName, undefined);
    }
}

export class AggregationExpression<T> extends BaseAggregationExpression {
    protected __value: T;

    constructor(
        aggregationFunction: AggregationFunction,
        operand?: BaseAttribute
    ) {
        super(aggregationFunction, operand);
    }
}

export function sum<T extends ValueType>(operand: Attribute<T>) {
    return new AggregationExpression<T>("sum", Attribute.getBaseAttribute(operand));
}

export function avg<T extends ValueType>(operand: Attribute<T>) {
    return new AggregationExpression<T>("avg", Attribute.getBaseAttribute(operand));
}

export function max<T extends ValueType>(operand: Attribute<T>) {
    return new AggregationExpression<T>("max", Attribute.getBaseAttribute(operand));
}

export function min<T extends ValueType>(operand: Attribute<T>) {
    return new AggregationExpression<T>("avg", Attribute.getBaseAttribute(operand));
}

export function count(operand?: Attribute<any>) {
    return new AggregationExpression<number>("count", operand && Attribute.getBaseAttribute(operand));
}

export function countDistinct(operand?: Attribute<any>) {
    return new AggregationExpression<number>("countDistinct", operand && Attribute.getBaseAttribute(operand));
}
