# Store Front

A React + Vite storefront demo built in the `client/store_front` workspace.

## Project Structure

- `index.html`
  - Application entry HTML used by Vite.
- `vite.config.js`
  - Vite configuration with React and Tailwind plugins.
  - Defines `base: '/store_front/'` and an alias for `@` to map to `./src`.
- `package.json`
  - Project scripts, dependencies, and dev dependencies.
- `jsconfig.json`
  - JavaScript path mapping and editor settings for the project.
- `src/`
  - `main.jsx`
    - React application bootstrap file.
  - `App.jsx`
    - Main application component.
  - `index.css`, `App.css`
    - Base styling and component-specific styles.
  - `components/`
    - `ui/`
      - Shared UI primitives: `badge.jsx`, `button.jsx`, `card.jsx`, `input.jsx`, `textarea.jsx`.
  - `lib/`
    - `kinesis.js`
      - AWS Kinesis integration helper.
    - `telemetry.js`
      - Telemetry utilities for user event tracking.
    - `utils.js`
      - Generic helper functions used across the app.
- `public/config.json`
  - Static configuration file loaded at runtime.

## Dependencies

### Runtime dependencies

- `react` / `react-dom` — UI library and DOM renderer.
- `@aws-sdk/client-kinesis` — AWS Kinesis client.
- `@aws-sdk/credential-providers` — AWS credential provider helpers.
- `class-variance-authority`, `clsx` — utility libraries for composing class names.
- `framer-motion` — animation support.
- `lucide-react` — icon components.
- `radix-ui` — UI component primitives.
- `shadcn` — styling utilities for UI components.
- `tailwindcss`, `tailwind-merge`, `tw-animate-css` — styling and animation utilities.
- `typescript` — project uses TypeScript tooling support.

### Dev dependencies

- `vite` — development server and build tooling.
- `@vitejs/plugin-react` — React support for Vite.
- `@tailwindcss/vite` — Tailwind integration plugin.
- `eslint`, `@eslint/js`, `eslint-plugin-react-hooks`, `eslint-plugin-react-refresh` — linting.
- `@types/node`, `@types/react`, `@types/react-dom` — type definitions for editor tooling.

## Environment

- Node.js is required to install dependencies and run the app.
- The project is configured as an ES module package (`"type": "module"`).
- Vite serves the app in development and builds static assets for production.
- Tailwind CSS is enabled through the Vite plugin.

## Getting Started

1. Install dependencies:

```bash
npm install
```

2. Start the development server:

```bash
npm run dev
```

3. Open the local URL shown in the console, usually `http://localhost:5173`.

## Build and Preview

- Build the production bundle:

```bash
npm run build
```

- Preview the built site locally:

```bash
npm run preview
```

## Linting

- Run ESLint across the project:

```bash
npm run lint
```

## Notes

- The app uses a `base` path of `/store_front/` in `vite.config.js`, which is important for deployment behind a subdirectory.
- Shared UI primitives are located under `src/components/ui/`, while business logic helpers live in `src/lib/`.
- `public/config.json` contains runtime configuration that can be loaded without bundling.

## automate_browse.py

This repository includes `automate_browse.py`, a standalone Python script that uses Selenium and ChromeDriver to automate browsing and generate high-frequency user-like events.

### Code structure

- `automate_browse.py`
  - `setup_driver(headless=False)`
    - Creates a Chrome WebDriver instance using `webdriver_manager`.
  - `simulate_high_frequency_events(driver, events_per_second, duration_seconds)`
    - Injects a browser-side script to dispatch mousemove, click, and scroll events.
    - Targets clickable elements and specifically prefers `add-to-cart` buttons.
  - `click_checkout_button(driver)`
    - Attempts to click an element with ID `btn-checkout`.
  - `main()`
    - Parses command-line arguments and runs the automation flow.

### Dependencies

- `python` 3.x
- `selenium`
- `webdriver-manager`
- Google Chrome or Chromium installed on the machine

### Environment

- The script expects Python 3 and a locally installed Chrome/Chromium browser.
- `webdriver-manager` automatically downloads the matching ChromeDriver binary.
- The script can run in headless mode with the `--headless` flag.

### How to run

```bash
python automate_browse.py <URL> [eventsPerSecond] [durationSeconds] [--headless]
```

Example:

```bash
python automate_browse.py http://localhost:5173 150 30 --headless
```

- `<URL>`: The page to open and simulate browsing on.
- `[eventsPerSecond]`: Optional number of simulated events per second (default: `100`).
- `[durationSeconds]`: Optional duration in seconds (default: `60`).
- `[--headless]`: Optional flag to run Chrome without a visible window.

### Notes for `automate_browse.py`

- The script prints progress and total events generated.
- It waits briefly after loading the page to ensure the DOM is ready.
- `driver.quit()` is currently commented out, so the browser session may remain open at the end of execution unless manually closed.
