'use strict';

/**
 * Namespace all selector definitions in CSS content, useful for namespacing
 * CSS to specific pagelets.
 *
 * @param {Array|String} d array of valid CSS selectors
 * @api public
 */
exports.namespace = function namespace(namespaces) {
  if (!Array.isArray(namespaces)) namespaces = [namespaces];

  /**
   * Prefix each selector with each namespace.
   *
   * @param {Object} css parsed CSS representation
   * @param {Rework} rework instance
   * @api private
   */
  return function multiPrefix(css, rework) {
    function walk(node, fn) {
      node.rules.map(function map(rule) {
        if (rule.rules) return walk(rule, fn);
        fn(rule, node);
      });
    }

    walk(css, function(rule, node) {
      if (!rule.selectors) return rule;

      rule.selectors = rule.selectors.map(function selectors(selector) {
        if (':root' === selector) return namespaces.join();
        if ('@font-face' === selector) return selector;

        selector = selector.replace(/^\:root\s?/, '');
        return namespaces.map(function multi(name) {
          return name + ' ' + selector;
        }).join();
      });
    });
  };
};