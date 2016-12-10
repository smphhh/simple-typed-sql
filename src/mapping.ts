
let md5 = require('md5');

import {
    AttributeDefinitionData,
    AttributeTypeName,
    ValueType,
    FieldDefinition,
    FieldDefinitionMap,
    Metadata
} from './definition';

export class BaseMappingData<T> {
    protected __metadata: Metadata;
    protected __prototype: T;

    constructor(
        tableName: string,
        prototypeDefinition: MappingDefinition<T> & Record<string, AttributeDefinitionData<any>>
    ) {
        let metadata: Metadata = {
            tableName: tableName,
            attributes: {},
            fields: {}
        };

        let attributeNames = Object.keys(prototypeDefinition);

        for (let attributeName of attributeNames) {
            let fieldDefinition = {
                dataType: prototypeDefinition[attributeName].__type,
                fieldName: prototypeDefinition[attributeName].fieldName || attributeName
            };

            // TODO: Check for duplicate field names
            metadata.attributes[attributeName] = fieldDefinition;
            metadata.fields[fieldDefinition.fieldName] = fieldDefinition;
        }

        this.__metadata = metadata;
        this.__prototype = {} as T;
    }

    getAbsoluteFieldNameAttributeDefinitionMap() {
        return this.getAttributeDefinitions().reduce((map, definition) => {
            map[definition.getAbsoluteFieldName()] = definition;
            return map;
        }, {} as AttributeDefinitionMap);
    }

    getAbsoluteFieldNames() {
        return this.getAttributeDefinitions().map(item => item.getAbsoluteFieldName());
    }

    getAttributeDefinitionMap() {
        return this.getAttributes().reduce((definitionMap, name) => {
            definitionMap[name] = this.getAttributeDefinition(name);
            return definitionMap;
        }, {} as AttributeDefinitionMap)
    }

    getAttributeDefinitions() {
        return this.getAttributes().map(key => this.getAttributeDefinition(key));
    }

    getAttributeDefinition(name: string): BaseAttribute {
        let fieldDefinition = this.__metadata.attributes[name];
        if (fieldDefinition) {
            return new BaseAttribute(
                (this as any) as Mapping<{}>,
                fieldDefinition.dataType,
                name,
                fieldDefinition.fieldName
            );
        } else {
            throw new Error(`Invalid attribute name: ${name}.`);
        }
    }

    getAttributes() {
        return Object.keys(this.__metadata.attributes);
    }

    getFieldNames() {
        return Object.keys(this.__metadata.fields);
    }

    getFieldNameAttributeDefinitionMap() {
        return this.getAttributeDefinitions().reduce((map, definition) => {
            map[definition.getFieldName()] = definition;
            return map;
        }, {} as AttributeDefinitionMap);
    }

    getInstanceStub() {
        return this.__prototype;
    }

    getTableName() {
        return this.__metadata.tableName;
    }
}

export class BaseAttribute {
    constructor(
        public mapping: Mapping<{}>,
        public dataType: AttributeTypeName,
        public attributeName: string,
        public fieldName: string
    ) {}

    getAbsoluteFieldName() {
        let mappingData = WrappedMappingData.getMappingData(this.mapping);
        return `${mappingData.getTableName()}.${this.fieldName}`;
    }

    getAliasedAttributeName() {
        return `${this.getAbsoluteFieldName()} as ${this.attributeName}`;
    }

    getAttributeName() {
        return this.attributeName;
    }

    getFieldName() {
        return this.fieldName;
    }

    getTableName() {
        let mappingData = WrappedMappingData.getMappingData(this.mapping);
        return mappingData.getTableName();
    }
}

export class Attribute<T> extends BaseAttribute {
    protected __value: T;

    constructor(
        mapping: Mapping<{}>,
        dataType: AttributeTypeName,
        attributeName: string,
        fieldName: string
    ) {
        super(mapping, dataType, attributeName, fieldName);
    }
}

export interface AttributeDefinitionMap {
    [key: string]: BaseAttribute;
}

export class WrappedMappingData<T> {
    static getMappingData<T>(wrapper: WrappedMappingData<T>) {
        return (wrapper as any) as BaseMappingData<T>;
    }
}

export function getInstanceStub<T>(mapping: Mapping<T>) {
    return WrappedMappingData.getMappingData(mapping).getInstanceStub();
}

export type AttributeMap<T> = {
    readonly [P in keyof T]: Attribute<T[P]>
}

/**
 * A type representing a mapping between an SQL table and a JavaScript/Typescript object.
 */
export type Mapping<T> = WrappedMappingData<T> & AttributeMap<T>;
export type BaseMappingDefinition = Record<string, AttributeDefinitionData<any>>;
export type MappingDefinition<T> = {
    [P in keyof T]: AttributeDefinitionData<T[P]>;
}

/**
 * Create a Mapping given a table name and a definition object.
 */
export function defineMapping<T>(
    tableName: string,
    mappingDefinition: MappingDefinition<T> & BaseMappingDefinition
): Mapping<T> {
    let mappingData = new BaseMappingData<T>(tableName, mappingDefinition);
    return wrapMappingData(mappingData);
}

export function defineMappingAndInstanceStub<T>(
    tableName: string,
    mappingDefinition: MappingDefinition<T> & BaseMappingDefinition
): [Mapping<T>, T] {
    let mapping = defineMapping<T>(tableName, mappingDefinition);
    return [mapping, getInstanceStub(mapping)];
}

function wrapMappingData<T>(mappingData: BaseMappingData<T>) {
    let wrapper = Object.create(mappingData, {});
    
    for (let prop of mappingData.getAttributes()) {
        Object.defineProperty(wrapper, prop, {
            enumerable: true,
            get: () => wrapper.getAttributeDefinition(prop)
        });
    }
    
    return wrapper as Mapping<T>;
}

export type OperandType = BaseAttribute | ValueType;
