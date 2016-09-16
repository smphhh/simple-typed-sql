
export interface AttributeDefinitionMap {
    [key: string]: AttributeDefinition
}

export interface Metadata {
    tableName: string;
    attributes: AttributeDefinitionMap;
    fields: AttributeDefinitionMap;
}

export interface MetadataDefinition {
    __metadata: Metadata;
}

export interface PrototypeDefinition<InstanceType> {
    __prototypes: {
        instance: InstanceType
    }
}

export type ModelDefinition<InstanceType> = MetadataDefinition & PrototypeDefinition<InstanceType> & InstanceType;

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

    let prototypes = {
        __prototypes: {
            instance: {} as InstanceType
        }
    };

    let metadata = definition.__metadata;

    let attributeNames = getDataAttributes(prototypeDefinition);
    for (let attributeName of attributeNames) {
        let attributeDefinition = {
            dataType: prototypeDefinition[attributeName]._type,
            attributeName: attributeName,
            fieldName: prototypeDefinition[attributeName].fieldName || attributeName,
            tableName: tableName
        };

        // TODO: Check for duplicate field names
        //definition.prototypeDefinition[attributeName] = attributeDefinition;
        metadata.attributes[attributeName] = attributeDefinition;
        metadata.fields[attributeDefinition.fieldName] = attributeDefinition;
    }

    return Object.assign(definition, prototypes, (metadata.attributes as any) as InstanceType)
}

export function defineBoolean(options?: AttributeOptions) {
    let definition: any = options || {};
    definition._type = 'boolean';
    return definition as boolean;
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
    tableName: string;
}

export interface SerializationOptions {
    stringifyJson?: boolean;
}

export function getTableName<T>(definition: ModelDefinition<T>) {
    return definition.__metadata.tableName;
}

export function getAbsoluteFieldName(attributeDefinition: AttributeDefinition) {
    return `${attributeDefinition.tableName}.${attributeDefinition.fieldName}`;
}

export function getAttributeDefinitions<T>(definition: ModelDefinition<T>) {
    return getAttributes(definition).map(key => definition.__metadata.attributes[key]);
}

export function getAttributes<T>(definition: ModelDefinition<T>) {
    return Object.keys(definition.__metadata.attributes);
}

export function getFieldNames<T>(definition: ModelDefinition<T>) {
    return Object.keys(definition.__metadata.fields);
}

export function getAbsoluteFieldNames<T>(definition: ModelDefinition<T>) {
    return getAttributeDefinitions(definition).map(getAbsoluteFieldName);
}

export function getDataAttributes(data: any) {
    return Object.keys(data);
}

export function getIdentityAliasedName(name: string) {
    return `${name} as ${name}`;
}

export function getAbsoluteFieldNameAttributeDefinitionMap<T>(model: ModelDefinition<T>) {
    let map: AttributeDefinitionMap = {};
    getAttributeDefinitions(model).map(definition => {
        map[getAbsoluteFieldName(definition)] = definition;
    });
    return map;
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
    fields: AttributeDefinitionMap,
    fieldData: any,
    serializationOptions: SerializationOptions
) {
    let data: any = {};

    let fieldNames = getDataAttributes(fieldData);
    for (let fieldName of fieldNames) {
        let attributeDefinition = fields[fieldName];
        let fieldValue = fieldData[getAbsoluteFieldName(attributeDefinition)];
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
