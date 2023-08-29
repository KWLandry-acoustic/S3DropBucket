'use strict';

//
// Third party modules.
//
var canihaz = require('../canihaz');

/**
 * Process CoffeeScript files. The processor has no required options
 *
 * @param {String} content the raw file content that needs to be processed
 * @param {Object} options supply optional options
 * @param {Function} done callback
 * @api public
 */
var coffee = module.exports = function coffeescript(content, options, done) {
  if ('function' === typeof options) {
    done = options;
    options = Object.create(null);
  }

  canihaz['coffee-script'](function requiredCoffee(err, coffeescript) {
    if (err) return done(err);

    try { return done(null, coffeescript.compile(content, options)); }
    catch (e) { done(e); }
  });
};

//
// Regular expression to handle imports, as getter for multiple uses.
//
Object.defineProperty(coffee, 'regexp', {
  get: function get() {
    return (/require.['"]\.([^'"]+)['"]/gm);
  }
});

//
// Configure the import check.
//
coffee.imports = require('../import')(coffee.regexp);

//
// Set of possible extensions this pre-processor can generate.
//
coffee.extensions = [ 'js' ];

//
// Default configuration on export extension.
//
coffee.export = 'js';
