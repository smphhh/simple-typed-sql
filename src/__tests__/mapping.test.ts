import * as Bluebird from 'bluebird';
import * as chai from 'chai';
import * as chaiAsPromised from 'chai-as-promised';

import {
    defineMapping,
    defineNumber,
    defineString,
    WrappedMappingData
} from '../';
import { BaseAttribute } from '../mapping';

chai.use(chaiAsPromised);

let expect = chai.expect;

describe("Simple typed SQL mapping", function () {

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
        let fooAttributeDefinition2: BaseAttribute = mapping.foo as any;
        expect(fooAttributeDefinition.mapping).to.equal(mapping);
        expect(fooAttributeDefinition2.mapping).to.equal(mapping);
    });

    it("should error on property set attempt", async function () {
        let mapping = defineMapping(
            'foo',
            {
                bar: defineString()
            }
        ) as any;

        expect(function () { mapping.bar = "a"; }).to.throw(Error);
    });
});


