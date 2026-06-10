import node from "eslint-plugin-n"
import { defineConfig } from "eslint/config"
import tseslint from "typescript-eslint"
import jsdoc from "eslint-plugin-jsdoc"
import compat from "eslint-plugin-compat";

export default defineConfig([
  ...tseslint.configs.recommended,
  compat.configs["flat/recommended"],
  jsdoc.configs['flat/recommended-typescript'],
  {
    files: ["**/*.ts", "**/*.tsx"],
    languageOptions: {
      parser: tseslint.parser,
    },
    plugins: {
      n: node
    },
    rules: {
      "n/no-unsupported-features/node-builtins": "error",
      "n/no-unsupported-features/es-builtins": "error",
      "n/no-unsupported-features/es-syntax": "error",
      "jsdoc/require-throws-type": "off",
      "@typescript-eslint/no-unused-vars": [
        "error",
        {
          "vars": "all",
          "args": "after-used",
          "ignoreRestSiblings": true,
          // This tells the linter to completely bypass checking names matching your docs
          "varsIgnorePattern": "^PromisePool$"
        }
      ]
    },
    settings: {
      n: {
        tryExtensions: [".js", ".json", ".node", ".ts"]
      }
    }
  }
]);