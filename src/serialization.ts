import {
    AttributeDefinitionMap,
    getAbsoluteFieldName,
    getAliasedName,
    getDataAttributes,
    ModelDefinition,
    SerializationOptions
} from './definition';

import {
    BaseMappingData
} from './mapping';

export function serializeData<ModelDataType extends DataType, DataType>(
    mapping: BaseMappingData<ModelDataType>,
    data: DataType,
    serializationOptions: SerializationOptions
) {
    let attributeNames = getDataAttributes(data);

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
    fields: AttributeDefinitionMap,
    fieldData: any,
    serializationOptions: SerializationOptions
) {
    let data: any = {};

    let outputFields: AttributeDefinitionMap = {};
    for (let fieldName in fields) {
        outputFields[getAliasedName(fieldName)] = fields[fieldName];
    }

    let fieldNames = getDataAttributes(fieldData);
    for (let fieldName of fieldNames) {
        let attributeDefinition = outputFields[fieldName];
        let fieldValue = fieldData[getAliasedName(getAbsoluteFieldName(attributeDefinition))];
        let attributeName = attributeDefinition.attributeName;
        let dataType = attributeDefinition.dataType;

        if (serializationOptions.stringifyJson && dataType === 'json') {
            data[attributeName] = JSON.parse(fieldValue);
        } else if (dataType === 'datetime' && typeof fieldValue === 'number') {
            data[attributeName] = new Date(fieldValue);
        } else if (dataType === 'number' && typeof fieldValue === 'string') {
            data[attributeName] = parseInt(fieldValue);
        } else {
            data[attributeName] = fieldValue;
        }
    }

    return data as ModelDataType;
}
