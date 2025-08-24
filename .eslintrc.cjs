module.exports = {
  root: true,
  parser: "@typescript-eslint/parser",
  parserOptions: {
    ecmaVersion: "latest",
    sourceType: "module",
  },
  plugins: ["@typescript-eslint"],
  extends: [
    "eslint:recommended",
    "plugin:@typescript-eslint/recommended",
  ],
  rules: {
    "@typescript-eslint/no-explicit-any": "off",
    "no-restricted-syntax": [
      "error",
      {
        selector: "TSAsExpression > TSAnyKeyword",
        message: "Do not use 'as any'.",
      },
      {
        selector: "TSTypeAssertion > TSAnyKeyword",
        message: "Do not use '<any>' assertions.",
      },
    ],
  },
  ignorePatterns: [
    "dist/**",
    "node_modules/**",
  ],
};


