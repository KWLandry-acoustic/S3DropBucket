'use strict';

//
// Native modules.
//
var fs = require('fs')
  , path = require('path');

/**
 * Expose the importer and allow configuration of the import/require statement.
 * The regular expression defaults to /@import\s["']?([^'"]+)["']?/gm
 *
 * @param {RegExp} regexp regular expression to check against imports.
 * @return {Function} import functionality
 * @api public
 */
module.exports = function importer(regexp) {
  regexp = regexp || /@import\s["']?([^'"]+)["']?/gm;

  /**
   * Parse potential import statements from supplied content. It should
   * also parse the import statements recursively
   *
   * @param {String} location location of the file that needs to be parsed
   * @param {Array} paths collection to append to
   * @param {Array} current reference to currently found files
   * @returns {Array} absolute paths
   * @api public
   */
  return function imports(location, paths, current) {
    paths = paths || [];
    current = current || [];
    if (!fs.existsSync(location)) return paths;

    //
    // Get the file, unparsed so we can minimize the overhead of parsing it
    //
    var content = fs.readFileSync(location, 'utf8')
      , directory = path.dirname(location)
      , ext = path.extname(location);

    //
    // Parse out require statements for the files.
    //
    content.replace(regexp, function detect(x, match) {
      //
      // If no file extension, assume the extension equal to the includer extension.
      //
      if (!path.extname(path.basename(match))) match += ext;
      match = path.join(directory, match);

      if (!~paths.indexOf(match)) {
        paths.push(match);
        current.push(match);
      }
    });

    //
    // Iterate over all the paths to see if required files also contains files
    // that we need to watch.
    //
    current.forEach(function recursive(location) {
      paths = imports(location, paths, []);
    });

    return paths;
  };
};
