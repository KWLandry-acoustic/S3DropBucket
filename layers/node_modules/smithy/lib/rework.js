'use strict';

//
// Third party modules.
//
var canihaz = require('../canihaz');


/**
 * Process CSS files. The processor allows the following options:
 *  - compress: reduce size by removing optional spaces
 *  - sourcemap: provide content with inline source map
 *  - plugins: supply an array of functions
 *
 * @param {String} content the raw file content that needs to be processed
 * @param {Object} options supply optional options
 * @param {Function} done
 * @api public
 */
var rework = module.exports = function rework(content, options, done) {
  if ('function' === typeof options) {
    done = options;
    options = Object.create(null);
  }

  //
  // Get the rework processor.
  //
  canihaz.rework(function requiredRework(error, rework) {
    if (error) return done(error);

    var compiler = rework(content);

    //
    // Add plugins to the rework instance.
    //
    if ('plugins' in options) options.plugins.forEach(function map(fn) {
      compiler.use(fn.bind(rework));
    });

    //
    // Process the content and supply options.
    //
    done(null, compiler.toString(options));
  });
};

//
// Configure the import check.
//
rework.imports = require('../import')();

//
// Set of possible extensions this pre-processor can generate.
//
rework.extensions = [ 'css' ];

//
// Provide plugins.
//
rework.plugins = require('../plugins/rework');

//
// Default configuration on export extension.
//
rework.export = 'css';
