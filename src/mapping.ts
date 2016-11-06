
let md5 = require('md5');

import {
    AttributeDefinition,
    AttributeDefinitionMap,
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
            let attributeDefinition = new AttributeDefinition();
            attributeDefinition.dataType = prototypeDefinition[attributeName]._type;
            attributeDefinition.attributeName = attributeName;
            attributeDefinition.fieldName = prototypeDefinition[attributeName].fieldName || attributeName;
            attributeDefinition.tableName = tableName;

            // TODO: Check for duplicate field names
            //definition.prototypeDefinition[attributeName] = attributeDefinition;
            metadata.attributes[attributeName] = attributeDefinition;
            metadata.fields[attributeDefinition.fieldName] = attributeDefinition;
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
        return this.getAttributeDefinitions().map(BaseMappingData.getAbsoluteFieldName);
    }

    getAttributeDefinitionMap() {
        return this.__metadata.attributes;
    }

    getAttributeDefinitions() {
        return this.getAttributes().map(key => this.__metadata.attributes[key]);
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

    static getAbsoluteFieldName(attributeDefinition: AttributeDefinition) {
        return attributeDefinition.getAbsoluteFieldName();
    }

    static getAliasedName(name: string) {
        if (name.length > 60) {
            return md5(name);
        } else {
            return name;
        }
    }

    static getAliasedAttributeName(attributeDefinition: AttributeDefinition) {
        return attributeDefinition.getAliasedAttributeName();
    }

    static getIdentityAliasedName(name: string) {
        return `${name} as ${BaseMappingData.getAliasedName(name)}`;
    }
}

export class WrappedMappingData<T> {
    constructor(
        protected __mapping: BaseMappingData<T>
    ) {
    }

    static getMappingData<T>(wrapper: WrappedMappingData<T>) {
        return wrapper.__mapping;
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
    let wrapper = new WrappedMappingData(mappingData);
    let proxy = new Proxy(wrapper, {
        get: function (target, name) {
            let mappingData = WrappedMappingData.getMappingData(target); 
            if (name === '__mapping') {
                return mappingData;
            } else {
                return mappingData.getAttributeDefinitionMap()[name];
            }
        }
    });
    return proxy as Mapping<T>;
}
