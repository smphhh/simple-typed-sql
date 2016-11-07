
export interface FieldDefinitionMap {
    [key: string]: FieldDefinition;
}

export interface Metadata {
    tableName: string;
    attributes: FieldDefinitionMap;
    fields: FieldDefinitionMap;
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


export type ValueType = string | number | boolean | Date;

export interface FieldDefinition {
    dataType: DataType;
    fieldName: string;
}

export interface SerializationOptions {
    stringifyJson?: boolean;
}
