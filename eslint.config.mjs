import js from '@eslint/js';
import eslintConfigPrettier from 'eslint-config-prettier';

export default [
  js.configs.recommended,
  {
    files: ['**/*.js', '**/*.mjs'],
    languageOptions: { ecmaVersion: 2024 },
    rules: eslintConfigPrettier.rules,
  },
];
