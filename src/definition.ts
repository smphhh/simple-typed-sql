
export interface FieldDefinitionMap {
    [key: string]: FieldDefinition;
}

export interface Metadata {
    tableName: string;
    attributes: FieldDefinitionMap;
    fields: FieldDefinitionMap;
}

export function defineBoolean(options?: AttributeOptions): AttributeDefinitionData<boolean> {
    return {
        ...options,
        __value: null as boolean,
        __type: 'boolean'
    };
}

export function defineString(options?: AttributeOptions): AttributeDefinitionData<string> {
    return {
        ...options,
        __value: null as string,
        __type: 'string'
    };
}

export function defineNumber(options?: AttributeOptions): AttributeDefinitionData<number> {
    return {
        ...options,
        __value: null as number,
        __type: 'number'
    };
}

export function defineJson<Subtype>(options?: AttributeOptions): AttributeDefinitionData<Subtype> {
    return {
        ...options,
        __value: null as Subtype,
        __type: 'json'
    };
}

export function defineDatetime(options?: AttributeOptions): AttributeDefinitionData<Date> {
    return {
        ...options,
        __value: null as Date,
        __type: 'datetime'
    };
}

export type DataMapperOptions = SerializationOptions;

export interface AttributeOptions {
    fieldName?: string
}

export type AttributeTypeName = 'number' | 'string' | 'json' | 'datetime' | 'boolean';

export interface AttributeDefinitionData<T> extends AttributeOptions {
    fieldName?: string;
    __value: T;
    __type: AttributeTypeName;
}

export type ValueType = string | number | boolean | Date;

export interface FieldDefinition {
    dataType: AttributeTypeName;
    fieldName: string;
}

export interface SerializationOptions {
    stringifyJson?: boolean;
}

export class CustomError extends Error {
    constructor(message) {
        super();
        this.message = message;
        this.stack = (new Error()).stack;
        this.name = (this.constructor as any).name;
    }
}
