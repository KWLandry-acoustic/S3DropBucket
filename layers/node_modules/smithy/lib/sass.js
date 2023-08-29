'use strict';

//
// Third party modules.
//
var canihaz = require('../canihaz');

/**
 * Process sass files. THe processor has no required options, note however that
 * the content and callback function are supplied to the data and success/error
 * keys respectively.
 *
 * @param {String} content the raw file content that needs to be processed
 * @param {Function} done
 * @api public
 */
var sass = module.exports = function sass(content, options, done) {
  if ('function' === typeof options) {
    done = options;
    options = Object.create(null);
  }

  canihaz['node-sass'](function requiredSass(err, sass) {
    if (err) return done(err);

    //
    // Node-sass requires a single object to which the arguments should be passed.
    //
    options.data = content;
    options.error = done;
    options.success = function success(content) {
      done(null, content.css);
    };

    sass.render(options);
  });
};

//
// Configure the import check.
//
sass.imports = require('../import')();

//
// Set of possible extensions this pre-processor can generate.
//
sass.extensions = [ 'css' ];

//
// Default configuration on export extension.
//
sass.export = 'css';
