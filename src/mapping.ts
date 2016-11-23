
let md5 = require('md5');

import {
    DataType,
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
        prototypeDefinition: T
    ) {
        let metadata: Metadata = {
            tableName: tableName,
            attributes: {},
            fields: {}
        };

        let attributeNames = Object.keys(prototypeDefinition);

        for (let attributeName of attributeNames) {
            let fieldDefinition = {
                dataType: prototypeDefinition[attributeName]._type,
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

    getAttributeDefinition(name: string): AttributeDefinition {
        let fieldDefinition = this.__metadata.attributes[name];
        if (fieldDefinition) {
            return new AttributeDefinition(
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

export class AttributeDefinition {
    constructor(
        public mapping: Mapping<{}>,
        public dataType: DataType,
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

export interface AttributeDefinitionMap {
    [key: string]: AttributeDefinition;
}

export class WrappedMappingData<T> {
    constructor(
        protected __mappingData: BaseMappingData<T>
    ) {
    }

    static getMappingData<T>(wrapper: WrappedMappingData<T>) {
        return (wrapper as any) as BaseMappingData<T>;
    }
}

export function getInstanceStub<T>(mapping: Mapping<T>) {
    return WrappedMappingData.getMappingData(mapping).getInstanceStub();
}

/**
 * A type representing a mapping between an SQL table and a JavaScript/Typescript object.
 */
export type Mapping<T> = WrappedMappingData<T> & T;

/**
 * Create a Mapping given a table name and a definition object.
 */
export function defineMapping<T>(tableName: string, prototypeDefinition: T): Mapping<T> {
    let mappingData = new BaseMappingData(tableName, prototypeDefinition);
    return wrapMappingData(mappingData);
}

function wrapMappingData<T>(mappingData: BaseMappingData<T>) {
    let wrapper = Object.create(mappingData, {});
    
    for (let prop of mappingData.getAttributes()) {
        Object.defineProperty(wrapper, prop, {
            enumerable: true,
            get: () => mappingData.getAttributeDefinition(prop)
        });
    }
    
    return wrapper as Mapping<T>;
}

export type OperandType = AttributeDefinition | ValueType;
