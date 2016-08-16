
export interface Metadata {
    tableName: string;
    attributes: {
        [attributeName: string]: AttributeDefinition
    };
    fields: {
        [fieldName: string]: AttributeDefinition
    };
}

export interface MetadataDefinition {
    __metadata: Metadata;
}

export type ModelDefinition<InstanceType> = MetadataDefinition & InstanceType;

export function defineModel<InstanceType>(
    tableName: string,
    prototypeDefinition: InstanceType
): ModelDefinition<InstanceType> {

    let definition: MetadataDefinition = {
        __metadata: {
            tableName: tableName,
            attributes: {},
            fields: {}
        }
    };

    let metadata = definition.__metadata;

    let attributeNames = getDataAttributes(prototypeDefinition);
    for (let attributeName of attributeNames) {
        let attributeDefinition = {
            dataType: prototypeDefinition[attributeName]._type,
            attributeName: attributeName,
            fieldName: prototypeDefinition[attributeName].fieldName || attributeName
        };

        // TODO: Check for duplicate field names
        //definition.prototypeDefinition[attributeName] = attributeDefinition;
        metadata.attributes[attributeName] = attributeDefinition;
        metadata.fields[attributeDefinition.fieldName] = attributeDefinition;
    }

    return Object.assign(definition, (metadata.attributes as any) as InstanceType)
}

export function defineString(options?: AttributeOptions) {
    let definition: any = options || {};
    definition._type = 'string';
    return definition as string;
}

export function defineNumber(options?: AttributeOptions) {
    let definition: any = options || {};
    definition._type = 'number';
    return definition as number;
}

export function defineJson<Subtype>(options?: AttributeOptions) {
    let definition: any = options || {};
    definition._type = 'json';
    return definition as Subtype;
}

export function defineDatetime(options?: AttributeOptions) {
    let definition: any = options || {};
    definition._type = 'datetime';
    return definition as Date;
}

export type DataMapperOptions = SerializationOptions;

export interface AttributeOptions {
    fieldName?: string
}

export type DataType = 'number' | 'string' | 'json' | 'datetime';

export interface AttributeDefinition {
    dataType: DataType;
    attributeName: string;
    fieldName?: string;
}

export interface SerializationOptions {
    stringifyJson?: boolean;
}

export function getTableName<T>(definition: ModelDefinition<T>) {
    return definition.__metadata.tableName;
}

export function getAttributes<T>(definition: ModelDefinition<T>) {
    return Object.keys(definition.__metadata.attributes);
}

export function getFieldNames<T>(definition: ModelDefinition<T>) {
    return Object.keys(definition.__metadata.fields);
}

export function getDataAttributes(data: any) {
    return Object.keys(data);
}

export function serializeData<ModelDataType extends DataType, DataType>(
    model: ModelDefinition<ModelDataType>,
    data: DataType,
    serializationOptions: SerializationOptions
) {
    let attributeNames = getDataAttributes(data);

    let fieldData: any = {};
    for (let attributeName of attributeNames) {
        let attributeDefinition = model.__metadata.attributes[attributeName];
        if (serializationOptions.stringifyJson && attributeDefinition.dataType === 'json') {
            fieldData[attributeDefinition.fieldName] = JSON.stringify(data[attributeName]);
        } else {
            fieldData[attributeDefinition.fieldName] = data[attributeName];
        }
    }

    return fieldData;
}

export function deserializeData<ModelDataType>(
    model: ModelDefinition<ModelDataType>,
    fieldData: any,
    serializationOptions: SerializationOptions
) {
    let data: any = {};

    let fieldNames = getDataAttributes(fieldData);
    for (let fieldName of fieldNames) {
        let attributeDefinition = model.__metadata.fields[fieldName];
        let fieldValue = fieldData[attributeDefinition.fieldName];
        if (serializationOptions.stringifyJson && attributeDefinition.dataType === 'json') {
            data[attributeDefinition.attributeName] = JSON.parse(fieldValue);
        } else if (attributeDefinition.dataType === 'datetime' && typeof fieldValue === 'number') {
            data[attributeDefinition.attributeName] = new Date(fieldData[attributeDefinition.fieldName]);
        } else {
            data[attributeDefinition.attributeName] = fieldValue;
        }
    }

    return data as ModelDataType;
}
