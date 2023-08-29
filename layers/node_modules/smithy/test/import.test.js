describe('Importer', function () {
  'use strict';

  var common = require('./common')
    , smithy = common.smithy
    , fs = common.fs
    , expect = common.expect
    , importer = common.import
    , standard = importer();

  it('returns proper import function', function () {
    expect(importer).to.be.a('function');
  });

  it('returns the paths of imported files', function () {
    var result = standard(__dirname + '/fixtures/less.less');

    expect(result).to.be.an('array');
    expect(result[0]).to.include('test/fixtures/imports/less.less');
    expect(result).to.have.length(1);
  });

  it('returns empty array if there are no files to import', function () {
    var result = standard(__dirname + '/fixtures/stylus.stylus');
    expect(result).to.be.an('array');
    expect(result).to.have.length(0);
  });
});
