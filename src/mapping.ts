export class BaseMappingData<T> {
    constructor(
        private __prototype: T
    ) {

    }

    protected __metadata = { m: 'hi' };
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

export type Mapping<T> = WrappedMappingData<T> & T;

/**
 * 
 */
export function createTableMapping<T>(tableName: string, definitionPrototype: T): Mapping<T> {
    let wrapper = new WrappedMappingData(new BaseMappingData(definitionPrototype));
    return Object.assign(wrapper, definitionPrototype);
}
