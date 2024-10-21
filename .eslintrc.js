export const parser = "@typescript-eslint/parser"
export const parserOptions = {
  ecmaVersion: 2023, // Allows for the parsing of modern ECMAScript features
  sourceType: "module",
}
export const extends = [
  "plugin:@typescript-eslint/recommended", // recommended rules from the @typescript-eslint/eslint-plugin
  "plugin:prettier/recommended", // Enables eslint-plugin-prettier and eslint-config-prettier. This will display prettier errors as ESLint errors. Make sure this is always the last configuration in the extends array.
]
export const rules = {
  // Place to specify ESLint rules. Can be used to overwrite rules specified from the extended configs
  // e.g. "@typescript-eslint/explicit-function-return-type": "off",
  "space-infix-ops": ["error", {int32Hint: true}],
  "no-multiple-empty-lines": false,
  "prettier/prettier": ["error", {semi: false}],
}
