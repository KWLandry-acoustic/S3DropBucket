'use strict';

//
// Native modules.
//
var path = require('path');

//
// Third party modules.
//
var canihaz = require('../canihaz');

/**
 * Process less files. The processor requires the following options:
 *  - filename: name of the processed file
 *  - paths: array of directories for imports
 *
 * @param {String} content the raw file content that needs to be processed
 * @param {Object} options supply optional options
 * @param {Function} done
 * @api public
 */
var less = module.exports = function less(content, options, done) {
  if ('function' === typeof options) {
    done = options;
    options = Object.create(null);
  }

  //
  // Get the less processor.
  //
  canihaz.less(function requiredLess(err, less) {
    if (err) return done(err);

    options.strictImports = options.strictImports || false;
    options.optimization = options.optimization || 1;

    less.render(content, options, function parsed(err, output) {
      if (err) return done(module.exports.format(err));

      try { return done(null, output.css); }
      catch (e) { return done(module.exports.format(e)); }
    });
  });
};

/**
 * For some odd reason, the less guys find it funny to generate and throw
 * pointless custom error messages.. so we need to provide our own
 * unfuckingifyzor for these errors.
 *
 * @param {Error} err some fake fucked up error obj
 * @returns {Error}
 */
less.format = function format(err) {
  var message = [
      ' Error type: ' + err.type + ' ' + err.filename + ':' + err.line
    , err.extract[1] ? (' > ' + err.line + '| ' + err.extract[1] || '') : ''
    , ''
    , ' ' + err.message
  ];

  var better = new Error(message.join('\n'));
  better.stack = err.stack;

  return better;
};

//
// Configure the import check.
//
less.imports = require('../import')();

//
// Set of possible extensions this pre-processor can generate.
//
less.extensions = [ 'css' ];

//
// Default configuration on export extension.
//
less.export = 'css';
