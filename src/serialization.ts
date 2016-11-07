import {
    SerializationOptions
} from './definition';

import {
    AttributeDefinitionMap,
    BaseMappingData
} from './mapping';

export function serializeData<ModelDataType extends DataType, DataType>(
    mapping: BaseMappingData<ModelDataType>,
    data: DataType,
    serializationOptions: SerializationOptions
) {
    let attributeNames = Object.keys(data);

    let attributeDefinitionMap = mapping.getAttributeDefinitionMap();

    let fieldData: any = {};
    for (let attributeName of attributeNames) {
        let attributeDefinition = attributeDefinitionMap[attributeName];
        // Skip extra attributes
        if (attributeDefinition === undefined) {
            continue;
        }

        if (serializationOptions.stringifyJson && attributeDefinition.dataType === 'json') {
            fieldData[attributeDefinition.fieldName] = JSON.stringify(data[attributeName]);
        } else {
            fieldData[attributeDefinition.fieldName] = data[attributeName];
        }
    }

    return fieldData;
}

export function deserializeData<ModelDataType>(
    outputFields: AttributeDefinitionMap,
    data: any,
    serializationOptions: SerializationOptions
) {
    let fieldNames = Object.keys(data);
    for (let fieldName of fieldNames) {
        let attributeDefinition = outputFields[fieldName];
        let fieldValue = data[fieldName];
        let attributeName = attributeDefinition.attributeName;
        let dataType = attributeDefinition.dataType;

        if (serializationOptions.stringifyJson && dataType === 'json') {
            data[fieldName] = JSON.parse(fieldValue);
        } else if (dataType === 'datetime' && typeof fieldValue === 'number') {
            data[fieldName] = new Date(fieldValue);
        } else if (dataType === 'number' && typeof fieldValue === 'string') {
            data[fieldName] = parseInt(fieldValue);
        }
    }

    return data as ModelDataType;
}
