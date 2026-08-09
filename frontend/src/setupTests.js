// jest-dom adds custom jest matchers for asserting on DOM nodes.
// allows you to do things like:
// expect(element).toHaveTextContent(/react/i)
// learn more: https://github.com/testing-library/jest-dom
import '@testing-library/jest-dom';

// jsdom doesn't implement ResizeObserver — FlightMap.jsx (Phase G) uses it
// for the responsive-layout breakpoint. A no-op stub is enough for
// components to mount in tests; no test here asserts on live resize
// behavior (that's a real-browser concern, verified separately).
global.ResizeObserver = class ResizeObserver {
  observe() {}
  unobserve() {}
  disconnect() {}
};
