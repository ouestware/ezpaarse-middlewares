import js from '@eslint/js';
import prettier from 'eslint-config-prettier';
import globals from 'globals';

export default [
  js.configs.recommended,
  prettier,
  {
    languageOptions: {
      ecmaVersion: 'latest',

      globals: {
        ezpaarse: false,
        describe: false,
        it: false,
        before: false,
        ...globals.node,
      },
    },
  },
];
