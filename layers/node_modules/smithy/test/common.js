'use strict';

var chai = require('chai');
chai.Assertion.includeStack = true;

//
// Expose smithy
//
exports.smithy = require('../');
exports.import = require('../import');
exports.fs = require('fs');
exports.canihaz = require('canihaz')('smithy');


//
// Expose our assertions.
//
exports.expect = chai.expect;
