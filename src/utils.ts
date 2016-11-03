
import {
    ConditionClause,
    LogicalClause,
    ComparisonClause,
    ComparisonOperandType
} from './condition';

import { AttributeDefinition } from './definition';

import { BaseMappingData, Mapping, WrappedMappingData } from './mapping';

export namespace Utils {

    export function bindConditionAttributes<T>(conditionClause: ConditionClause, mapping: Mapping<T>, instance: T): ConditionClause {
        if (conditionClause instanceof LogicalClause) {
            return new LogicalClause(
                conditionClause.operator,
                conditionClause.operands.map(operand => bindConditionAttributes(operand, mapping, instance))
            );

        } else if (conditionClause instanceof ComparisonClause) {
            let mappingData = WrappedMappingData.getMapping(mapping);

            return new ComparisonClause(
                conditionClause.operator,
                bindOperand(conditionClause.operand1, mappingData, instance),
                bindOperand(conditionClause.operand2, mappingData, instance)
            );
        }
    }
}

function bindOperand<T>(operand: ComparisonOperandType, mappingData: BaseMappingData<T>, instance: T): ComparisonOperandType {
    if (operand instanceof AttributeDefinition && operand.getTableName() === mappingData.getTableName()) {
        let value = instance[operand.getAttributeName()];
        if (value === null || value === undefined) {
            throw new Error("Binding instance missing required attribute");
        }

        return value;

    } else {
        return operand;
    }
}
