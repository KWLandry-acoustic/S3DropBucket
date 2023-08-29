'use strict';

//
// Define set of pre-processors to expose.
//
var processors = {
  styl: 'stylus',
  stylus: 'stylus',
  less: 'less',
  sass: 'sass',
  coffee: 'coffeescript',
  coffeescript: 'coffeescript',
  jsx: 'reactjsx',
  css: 'rework'
};

//
// Generate lazy requiring statements to keep the memory footprint low.
//
Object.keys(processors).forEach(function lazyrequire(extension) {
  var cache;

  Object.defineProperty(exports, extension, {
      get: function getter() {
        return cache || (cache = require('./lib/' + processors[extension]));
      }
  });
});

//
// Expose map of preprocessors by file extension.
//
exports.processors = processors;

//
// Expose list of extensions.
//
exports.extensions = Object.keys(processors).map(function map(ext) {
  return '.' + ext;
});