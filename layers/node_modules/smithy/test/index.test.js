describe('Smithy', function () {
  'use strict';

  var common = require('./common')
    , smithy = common.smithy
    , fs = common.fs
    , canihaz = common.canihaz
    , expect = common.expect;

  // preload the processors.
  before(function (done) {
    this.timeout(6E5);

    var i = 0;
    function iterate () { if (i++ === 3) done(); }

    canihaz['coffee-script'](iterate);
    canihaz.less(iterate);
    canihaz['node-sass'](iterate);
    canihaz.stylus(iterate);
  });

  it('exposes each of the available meta-languages', function () {
    expect(smithy).to.have.property('styl');
    expect(smithy).to.have.property('sass');
    expect(smithy).to.have.property('less');
    expect(smithy).to.have.property('coffee');
  });

  describe('#coffeescript', function () {
    it('can compile to extensions: [ JS ]', function () {
      expect(smithy.coffee).to.have.property('extensions');
      expect(smithy.coffee.extensions).to.have.include('js');
    });

    it('by default exports to extension: JS', function () {
      expect(smithy.coffee).to.have.property('export', 'js');
    });

    it('handles require statements in the content', function () {
      expect(smithy.coffee).to.have.property('regexp');
      expect(smithy.coffee.regexp.toString()).to.equal(/require.['"]\.([^'"]+)['"]/gm.toString());
      expect(smithy.coffee.regexp.test('require "./random.coffee"')).to.equal(true);
      expect(smithy.coffee.regexp.test('require "./path/random.js"')).to.equal(true);
    });

    it('exposes the coffeescript compiler',function (done) {
      var content = fs.readFileSync(__dirname + '/fixtures/coffeescript.coffee', 'utf-8');

      smithy.coffee(content, {}, function (error, processed) {
        expect(error).to.equal(null);
        expect(processed).to.include("square;\n\n  number = 42;\n\n  opposite = ");
        done();
      });
    });

    it('will return an error on false input', function (done) {
      smithy.coffee(undefined, {}, function (error, content) {
        expect(error).to.be.an.instanceof(Error);
        expect(content).to.equal(undefined);
        done();
      });
    });
  });

  describe('#stylus', function () {
    it('can compile to extensions: [ CSS ]', function () {
      expect(smithy.styl).to.have.property('extensions');
      expect(smithy.styl.extensions).to.have.include('css');
    });

    it('by default exports to extension: CSS', function () {
      expect(smithy.styl).to.have.property('export', 'css');
    });

    it('defers handling of import statements to general function', function () {
      expect(smithy.styl).to.not.have.property('regexp');
    });

    it('exposes the stylus compiler',function (done) {
      var content = fs.readFileSync(__dirname + '/fixtures/stylus.styl', 'utf-8');

      this.timeout(2E4);
      smithy.styl(content, {}, function (error, processed) {
        expect(error).to.equal(null);
        expect(processed).to.include("#header {\n  color: #4d926f;\n");
        done();
      });
    });

    it('will return an error on false input', function (done) {
      smithy.styl('false css #garbage\ncolor:lol', {}, function (error, content) {
        expect(error).to.be.an.instanceof(Error);
        expect(error.message).to.include('2| color:lol\n---------------^\n\nexpected "indent"');
        expect(content).to.equal(undefined);
        done();
      });
    });
  });

  describe('#rework', function () {
    var content = fs.readFileSync(__dirname + '/fixtures/rework.css', 'utf-8');

    it('can compile to extensions: [ CSS ]', function () {
      expect(smithy.css).to.have.property('extensions');
      expect(smithy.css.extensions).to.have.include('css');
    });

    it('by default exports to extension: CSS', function () {
      expect(smithy.css).to.have.property('export', 'css');
    });

    it('defers handling of import statements to general function', function () {
      expect(smithy.css).to.not.have.property('regexp');
    });

    it('exposes the rework compiler', function (done) {
      this.timeout(2E4);
      smithy.css(content, function (error, processed) {
        expect(error).to.equal(null);
        expect(processed).to.include("#header {\n  color: #4d926f;\n");
        done();
      });
    });

    it('exposes default plugins via instance', function (done) {
      this.timeout(2E4);

      function plug() {
        var rework = this;

        expect(rework).to.have.property('prefix');
        expect(rework).to.have.property('prefixSelectors');
        expect(rework.prefix).to.be.a('function');
        expect(rework.prefixSelectors).to.be.a('function');
      }

      smithy.css(content, { plugins: [ plug ] }, function (error, processed) {
        expect(processed).to.include("#header {\n  color: #4d926f;\n");
        done();
      });
    });

    it('can namespace all selectors', function (done) {
      this.timeout(2E4);

      expect(smithy.css.plugins).to.be.an('object');
      expect(smithy.css.plugins).to.have.property('namespace');
      expect(smithy.css.plugins.namespace).to.be.an('function');

      var plug = smithy.css.plugins.namespace('mynamespace');
      smithy.css(content, { plugins: [ plug ] }, function (error, processed) {
        expect(error).to.equal(null);
        expect(processed).to.include("mynamespace #header {\n  color: #4d926f;\n");
        done();
      });
    });

    it('can namespace inside media queries',function (done) {
      var content = fs.readFileSync(__dirname + '/fixtures/reworkmedia.css', 'utf-8')
        , plug = smithy.css.plugins.namespace('mynamespace');

      this.timeout(2E4);
      smithy.css(content, { plugins: [ plug ] }, function (error, processed) {
        expect(error).to.equal(null);
        expect(processed).to.include("@media handheld, only screen and (max-width: 767px) {\n  mynamespace .box {\n");
        done();
      });
    });

    it('can namespace but ignores @font-face',function (done) {
      var content = fs.readFileSync(__dirname + '/fixtures/reworkfont.css', 'utf-8')
        , plug = smithy.css.plugins.namespace('mynamespace');

      this.timeout(2E4);
      smithy.css(content, { plugins: [ plug ] }, function (error, processed) {
        expect(error).to.equal(null);
        expect(processed).to.equal("@font-face {\n  font-family: \"SSSocial\";\n}");
        done();
      });
    });
  });

  describe('#less', function () {
    it('can compile to extensions: [ CSS ]', function () {
      expect(smithy.less).to.have.property('extensions');
      expect(smithy.less.extensions).to.have.include('css');
    });

    it('by default exports to extension: CSS', function () {
      expect(smithy.less).to.have.property('export', 'css');
    });

    it('defers handling of import statements to general function', function () {
      expect(smithy.less).to.not.have.property('regexp');
    });

    it('exposes the less compiler',function (done) {
      var content = fs.readFileSync(__dirname + '/fixtures/less.less', 'utf-8');

      this.timeout(2E4);
      smithy.less(content, { paths: [  __dirname + '/fixtures' ] }, function (error, processed) {
        expect(error).to.equal(null);
        expect(processed).to.include("#header {\n  color:");
        expect(processed).to.include("#body {\n  color:");

        done();
      });
    });

    it('will return an error on false input', function (done) {
      smithy.less('false css #garbage\ncolor:lol', {}, function (error, content) {
        expect(error).to.be.an.instanceof(Error);
        expect(error.message).to.include(' > 2| color:lol\n\n Unrecognised input');
        expect(content).to.equal(undefined);
        done();
      });
    });
  });

  describe('#sass', function () {
    it('can compile to extensions: [ CSS ]', function () {
      expect(smithy.sass).to.have.property('extensions');
      expect(smithy.sass.extensions).to.have.include('css');
    });

    it('by default exports to extension: CSS', function () {
      expect(smithy.sass).to.have.property('export', 'css');
    });

    it('defers handling of import statements to general function', function () {
      expect(smithy.sass).to.not.have.property('regexp');
    });

    it('exposes the sass compiler',function (done) {
      var content = fs.readFileSync(__dirname + '/fixtures/sass.scss', 'utf-8');

      this.timeout(2E4);
      smithy.sass(content, { includePaths: [ __dirname + '/fixtures' ] }, function (error, processed) {
        expect(error).to.equal(null);
        expect(processed).to.equal("body {\n  color: #4D926F; }\n\nbody {\n  font: 100% Helvetica, sans-serif;\n  color: #333; }\n");
        done();
      });
    });

    it('will return an error on false input', function (done) {
      smithy.sass('false css #garbage\ncolor:lol', {}, function (error) {
        expect(error).to.be.a('object');
        expect(error.message).to.equal('invalid top-level expression');
        done();
      });
    });
  });

  describe('#jsx', function () {
    it('can compile to extensions: [ JS ]', function () {
      expect(smithy.jsx).to.have.property('extensions');
      expect(smithy.jsx.extensions).to.have.include('js');
    });

    it('by default exports to extension: JS', function () {
      expect(smithy.jsx).to.have.property('export', 'js');
    });

    it('exposes the jsx compiler',function (done) {
      var content = fs.readFileSync(__dirname + '/fixtures/react.jsx', 'utf-8');

      this.timeout(2E4);
      smithy.jsx(content, {}, function (error, processed) {
        expect(processed).to.include('displayName: \'jsxClass\',\n  render: function render() {\n    return (\n      React.createElement("div", null)\n    );\n');
        done();
      });
    });

    it('will return an error on false input', function (done) {
      smithy.jsx('', {}, function (error) {
        expect(error).to.be.a('object');
        expect(error).to.be.instanceof(Error);
        expect(error.message).to.equal('expected null to be an object');
        done();
      });
    });
  });
});
