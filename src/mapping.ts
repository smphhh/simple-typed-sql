
let md5 = require('md5');

import {
    AttributeDefinition,
    AttributeDefinitionMap,
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

    getAttributeDefinition(name: string) {
        let fieldDefinition = this.__metadata.attributes[name];
        if (fieldDefinition) {
            return new AttributeDefinition(
                fieldDefinition.dataType,
                name,
                fieldDefinition.fieldName,
                this.getTableName()
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
        get: function (target, property) {
            let mappingData = WrappedMappingData.getMappingData(target); 
            if (property === '__mapping') {
                return mappingData;
            } else if (typeof property === 'string') {
                return mappingData.getAttributeDefinition(property);
            } else {
                throw new Error(`Invalid attribute type: ${typeof property}`);
            }
        },

        set: function (target, property, value): boolean {
            throw new Error("Modifying the properties of a Mapping object is not allowed.");
        }
    });
    
    return proxy as Mapping<T>;
}
