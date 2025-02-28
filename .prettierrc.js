/** @type {import("prettier").Config} */
const config = {
    trailingComma: 'es5',
    tabWidth: 4,
    semi: false,
    singleQuote: true,
    parser: 'typescript',
    requirePragma: true,
    insertPragma: true,
    printWidth: 80, // Add line length limit
    bracketSpacing: true, // Add consistent spacing
    arrowParens: 'always', // Add consistent arrow function parentheses
    endOfLine: 'lf', // Add consistent line endings
}

module.exports = config
