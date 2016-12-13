import * as Bluebird from 'bluebird';
import * as chai from 'chai';
import * as chaiAsPromised from 'chai-as-promised';

import {
    Attribute,
    defineMapping,
    defineNumber,
    defineString,
    WrappedMappingData
} from '../';
import { BaseAttribute } from '../mapping';

chai.use(chaiAsPromised);

let expect = chai.expect;

describe("Mapping", function () {

    it("should only have the attributes as own property names", function () {
        let mapping = defineMapping(
            'foo',
            {
                foo: defineString(),
                bar: defineNumber()
            }
        );

        expect(Object.getOwnPropertyNames(mapping)).to.deep.equal(['foo', 'bar']);
    });

    it("mapping referenced by an AttributeDefinition should be same as the original mapping", function () {
        let mapping = defineMapping(
            'foo',
            {
                foo: defineString(),
                bar: defineNumber()
            }
        );

        let mappingData = WrappedMappingData.getMappingData(mapping);
        let fooAttributeDefinition = mappingData.getAttributeDefinition('foo');
        let fooAttribute = mapping.foo;
        expect(fooAttributeDefinition.mapping).to.equal(mapping);
        expect(Attribute.getBaseAttribute(fooAttribute).mapping).to.equal(mapping);
    });

    it("should error on property set attempt", function () {
        let mapping = defineMapping(
            'foo',
            {
                bar: defineString()
            }
        );

        expect(function () { mapping.bar = "a" as any; }).to.throw(Error);
    });
});


