# Account Management Form (Vue 3)

Test task implementation for an account management form with validation and persistence.

## Stack

- Vue 3 (Composition API)
- TypeScript
- Pinia
- Element Plus

## Requirements Covered

- Header with add button, label hint, and list of accounts with field labels.
- Fields:
  - Label (optional, max 50 chars, semicolon-separated)
  - Type (LDAP or Local)
  - Login (required, max 100 chars)
  - Password (required for Local, max 100 chars)
- LDAP hides password and stores it as null.
- Validation on blur for text inputs and on change for the select.
- Invalid fields are highlighted with a red border.
- Label is stored as an array of objects: `{ text: "label" }` per segment.
- Accounts are stored in Pinia and persisted in localStorage (key: `accounts-form`).

## Project Structure

- `src/App.vue` - main form UI and validation
- `src/stores/accounts.ts` - Pinia store + localStorage persistence
- `src/style.css` - styling

## Scripts

```bash
npm install
npm run dev
npm run build
npm run preview
```
