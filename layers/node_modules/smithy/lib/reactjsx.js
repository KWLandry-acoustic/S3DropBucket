'use strict';

//
// Third party modules.
//
var canihaz = require('../canihaz');

/**
 * Process React JSX files. The processor has no required options.
 *
 * @param {String} content the raw file content that needs to be processed
 * @param {Object} options supply optional options
 * @param {Function} done callback
 * @api public
 */
var reactjsx = module.exports = function reactjsx(content, options, done) {
  if ('function' === typeof options) {
    done = options;
    options = Object.create(null);
  }

  canihaz['react-tools'](function requiredReactTools(err, jsx) {
    if (err) return done(err);

    try { return done(null, jsx.transform(content, options)); }
    catch (e) { done(e); }
  });
};

//
// Set of possible extensions this pre-processor can generate.
//
reactjsx.extensions = [ 'js' ];

//
// Default configuration on export extension.
//
reactjsx.export = 'js';
