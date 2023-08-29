'use strict';

//
// Third party modules.
//
var canihaz = require('../canihaz');

/**
 * Process Stylus files. The processor requires the following options:
 *  - filename: name of the processed file
 *
 * @param {String} content the raw file content that needs to be processed
 * @param {Object} options supply optional options
 * @param {Function} done
 * @api public
 */
var styl = module.exports = function stylus(content, options, done) {
  if ('function' === typeof options) {
    done = options;
    options = Object.create(null);
  }

  //
  // Get the stylus processor.
  //
  canihaz.stylus(function requiredStylus(error, stylus) {
    if (error) return done(error);

    //
    // You can't have stylus without some nibbles.
    //
    canihaz.nib(function requiredNib(error, nib) {
      if (error) return done(error);

      var compiler = stylus(content, options)
        .set('filename', options.filename)
        .set('include css', options.css || true)
        .use(nib())
        .import('nib');

      //
      // Import files, similar to @import.
      //
      if (options.import && !Array.isArray(options.import)) options.import = [options.import]
      if (options.import) options.import.forEach(function imports(file) {
        compiler.import(file);
      });

      //
      // Process the options.
      //
      if (options.compress) compiler.define('compress', true);
      if (options.datauri) compiler.define('url', stylus.url());
      if (options.evil) compiler.define('eval', function evil(str) {
        return new stylus.nodes.String(eval(str.val));
      });

      // @TODO process the platform list by exposing them as modules.
      if (options.define) {
        Object.keys(options.define).forEach(function each(def) {
          compiler.define(
            def,
            options.define[def] ? stylus.nodes.true : stylus.nodes.false
          );
        });
      }

      // Everything is configured, compile.
      compiler.render(done);
    });
  });
};

//
// Configure the import check.
//
styl.imports = require('../import')();

//
// Set of possible extensions this pre-processor can generate.
//
styl.extensions = [ 'css' ];

//
// Default configuration on export extension.
//
styl.export = 'css';
