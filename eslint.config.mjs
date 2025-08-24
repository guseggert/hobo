// @ts-check
import tseslint from "typescript-eslint";

/** @type {import('eslint').Linter.Config[]} */
export default [
  { ignores: ["dist/**", "node_modules/**", "test/**"] },
  ...tseslint.configs.recommended,
  {
    files: ["src/**/*.ts"],
    rules: {
      "@typescript-eslint/no-explicit-any": "off",
      "no-restricted-syntax": [
        "error",
        { selector: "TSAsExpression > TSAnyKeyword", message: "Do not use 'as any'." },
        { selector: "TSTypeAssertion > TSAnyKeyword", message: "Do not use '<any>' assertions." },
      ],
    },
  },
];


