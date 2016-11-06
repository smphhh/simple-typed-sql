import * as Bluebird from 'bluebird';
import * as chai from 'chai';
import * as chaiAsPromised from 'chai-as-promised';

import {
    defineMapping,
    defineString
} from '../';

chai.use(chaiAsPromised);

let expect = chai.expect;

describe("Simple typed SQL mapping", function () {

    it("should error on property set attempt", async function () {
        let mapping = defineMapping(
            'foo',
            {
                bar: defineString()
            }
        );

        expect(function () { mapping.bar = "a"; }).to.throw(Error);
    });
});


