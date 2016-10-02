
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
        let map: AttributeDefinitionMap = {};
        this.getAttributeDefinitions().map(definition => {
            map[BaseMappingData.getAliasedName(BaseMappingData.getAbsoluteFieldName(definition))] = definition;
        });
        return map;
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

    getTableName() {
        return this.__metadata.tableName;
    }

    static getAbsoluteFieldName(attributeDefinition: AttributeDefinition) {
        return `${attributeDefinition.tableName}.${attributeDefinition.fieldName}`;
    }

    static getAliasedName(name: string) {
        if (name.length > 60) {
            return md5(name);
        } else {
            return name;
        }
    }

    static getAliasedAttributeName(attributeDefinition: AttributeDefinition) {
        return `${BaseMappingData.getAbsoluteFieldName(attributeDefinition)} as ${attributeDefinition.attributeName}`;
    }

    static getIdentityAliasedName(name: string) {
        return `${name} as ${BaseMappingData.getAliasedName(name)}`;
    }
}

export class WrappedMappingData<T> {
    constructor(
        private __mapping: BaseMappingData<T>
    ) {
    }

    static getMapping<T>(wrapper: WrappedMappingData<T>) {
        return wrapper.__mapping;
    }
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
    return Object.assign(wrapper, (mappingData.getAttributeDefinitionMap() as any) as T);
}
