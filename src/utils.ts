
import {
    ConditionClause,
    LogicalClause,
    ComparisonClause,
    ComparisonOperandType
} from './condition';
import { SelectExpression } from './core';
import { CustomError } from './definition';
import {
    AttributeMap,
    BaseAttribute,
    BaseMappingData,
    Mapping,
    WrappedMappingData
} from './mapping';

export namespace Utils {

    /**
     * Bind the attributes in a condition clause to actual values defined by an instance of the mappings apparent type.
     */
    export function bindConditionAttributes<T>(
        conditionClause: ConditionClause,
        mapping: Mapping<T>,
        instance: T,
        catchNullBinds = false
    ): ConditionClause {
        try {
            if (conditionClause instanceof LogicalClause) {
                return new LogicalClause(
                    conditionClause.operator,
                    conditionClause.operands.map(operand => bindConditionAttributes(operand, mapping, instance))
                );

            } else if (conditionClause instanceof ComparisonClause) {
                let mappingData = WrappedMappingData.getMappingData(mapping);

                return new ComparisonClause(
                    conditionClause.operator,
                    bindOperand(conditionClause.operand1, mappingData, instance),
                    bindOperand(conditionClause.operand2, mappingData, instance)
                );
            }
        } catch (error) {
            if (error instanceof NullBindError && catchNullBinds) {
                return null;
            } else {
                throw error;
            }
        }
    }

    /**
     * Return a select definition selecting all attributes of a Mapping.
     */
    export function selectAll<T>(mapping: Mapping<T>): AttributeMap<T> {
        let mappingData = WrappedMappingData.getMappingData(mapping);
        return mappingData.getAttributeDefinitionMap() as any;
    }

    /**
     * Return a stub object with the same type as the result of the select expression.
     */
    export function getSelectResultStub<T>(selectExpression: SelectExpression<T>) {
        return {} as T;
    }
}

function bindOperand<T>(operand: ComparisonOperandType, mappingData: BaseMappingData<T>, instance: T): ComparisonOperandType {
    if (operand instanceof BaseAttribute && operand.getTableName() === mappingData.getTableName()) {
        let value = instance[operand.getAttributeName()];
        if (value === null) {
            throw new NullBindError("Binding instance attribute value may not be null.");
        } else if (value === undefined) {
            throw new Error("Binding instance attribute value may not be undefined.");
        }

        return value;

    } else {
        return operand;
    }
}

class NullBindError extends CustomError {}

