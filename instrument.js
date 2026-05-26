require('dotenv').config();
const Sentry = require("@sentry/node");

if (process.env.SENTRY_DSN) {
  Sentry.init({
    dsn: process.env.SENTRY_DSN,
    tracesSampleRate: 1.0,
  });
  console.log("Sentry initialized successfully.");
} else {
  console.warn("Sentry DSN not found in environment. Sentry is disabled.");
}
