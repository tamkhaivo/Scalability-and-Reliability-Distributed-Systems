/**
 * Telemetry Utility
 * Records user behavior events in JSON format with standard time series
 */

const TELEMETRY_BUFFER = [];
const MAX_BUFFER_SIZE = 100;
const THROTTLE_INTERVAL = 100; // Throttle mouse moves to every 100ms
let mouseTrackingCleanup = null;
let isTracking = false;

/**
 * Get current timestamp in ISO 8601 format (standard time series)
 */
function getTimestamp() {
  return new Date().toISOString();
}

/**
 * Get screen dimensions
 */
function getScreenDimensions() {
  return {
    width: window.innerWidth,
    height: window.innerHeight,
  };
}

/**
 * Convert mouse position to percentage of screen size
 */
function getPositionPercentage(x, y) {
  const { width, height } = getScreenDimensions();
  return {
    x: Math.round((x / width) * 10000) / 100, // Round to 2 decimal places
    y: Math.round((y / height) * 10000) / 100,
  };
}

/**
 * Create a telemetry event object
 */
function createEvent(eventType, data = {}) {
  return {
    timestamp: getTimestamp(),
    eventType,
    data,
  };
}

/**
 * Add event to telemetry buffer
 */
function addEvent(event) {
  TELEMETRY_BUFFER.push(event);
  
  // Keep buffer size limited
  if (TELEMETRY_BUFFER.length > MAX_BUFFER_SIZE) {
    TELEMETRY_BUFFER.shift();
  }
  
  // Log to console in development
  if (process.env.NODE_ENV !== 'production') {
    console.log('[Telemetry]', JSON.stringify(event, null, 2));
  }
}

/**
 * Record mouse movement
 */
export function trackMouseMove(clientX, clientY) {
  const position = getPositionPercentage(clientX, clientY);
  const event = createEvent('mouse_move', {
    position,
    screen: getScreenDimensions(),
  });
  addEvent(event);
}

/**
 * Record mouse click
 */
export function trackMouseClick(clientX, clientY, targetInfo = {}) {
  const position = getPositionPercentage(clientX, clientY);
  const event = createEvent('mouse_click', {
    position,
    screen: getScreenDimensions(),
    target: {
      tag: targetInfo.tag || 'unknown',
      id: targetInfo.id || null,
      className: targetInfo.className || null,
    },
  });
  addEvent(event);
}

/**
 * Record product added to cart
 */
export function trackAddToCart(product) {
  const event = createEvent('product_add_to_cart', {
    product: {
      id: product.id,
      name: product.name,
      price: product.price,
      category: product.category,
    },
    cartSize: product.quantity || 1,
  });
  addEvent(event);
}

/**
 * Record product removed from cart
 */
export function trackRemoveFromCart(product) {
  const event = createEvent('product_remove_from_cart', {
    product: {
      id: product.id,
      name: product.name,
      price: product.price,
      category: product.category,
    },
    remainingQuantity: product.quantity || 0,
  });
  addEvent(event);
}

/**
 * Record tab enter (user returns to the tab)
 */
export function trackTabEnter() {
  const event = createEvent('tab_enter', {
    pageTitle: document.title,
    pageUrl: window.location.href,
  });
  addEvent(event);
}

/**
 * Record tab leave (user switches away from tab but doesn't close)
 */
export function trackTabLeave() {
  const event = createEvent('tab_leave', {
    pageTitle: document.title,
    pageUrl: window.location.href,
  });
  addEvent(event);
}

/**
 * Record tab close (user closes the browser tab)
 */
export function trackTabClose() {
  const event = createEvent('tab_close', {
    pageTitle: document.title,
    pageUrl: window.location.href,
    finalBufferSize: TELEMETRY_BUFFER.length,
  });
  addEvent(event);
}

/**
 * Record navigate away from the page (user clicks a link or navigates to another page)
 */
export function trackNavigateAway(destinationUrl) {
  const event = createEvent('navigate_away', {
    pageTitle: document.title,
    pageUrl: window.location.href,
    destinationUrl: destinationUrl,
    finalBufferSize: TELEMETRY_BUFFER.length,
  });
  addEvent(event);
}

/**
 * Get all telemetry data as JSON string
 */
export function getTelemetryData() {
  return JSON.stringify(TELEMETRY_BUFFER, null, 2);
}

/**
 * Get telemetry buffer array
 */
export function getTelemetryBuffer() {
  return [...TELEMETRY_BUFFER];
}

/**
 * Clear telemetry buffer
 */
export function clearTelemetry() {
  TELEMETRY_BUFFER.length = 0;
}

/**
 * Initialize mouse tracking on the document
 */
export function initMouseTracking() {
  // Always set up new tracking (even after stopTracking was called)
  // The early return was preventing re-initialization after checkout
  
  let lastMoveTime = 0;

  const handleMouseMove = (e) => {
    const now = Date.now();
    if (now - lastMoveTime >= THROTTLE_INTERVAL) {
      trackMouseMove(e.clientX, e.clientY);
      lastMoveTime = now;
    }
  };

  const handleClick = (e) => {
    trackMouseClick(e.clientX, e.clientY, {
      tag: e.target.tagName,
      id: e.target.id,
      className: e.target.className,
    });
  };

  // Cleanup function to remove event listeners
  const cleanup = () => {
    document.removeEventListener('mousemove', handleMouseMove);
    document.removeEventListener('click', handleClick);
  };

  document.addEventListener('mousemove', handleMouseMove, { passive: true });
  document.addEventListener('click', handleClick, { passive: true });

  isTracking = true;
  mouseTrackingCleanup = cleanup;

  console.log('[Telemetry] Mouse tracking initialized');

  return cleanup;
}

/**
 * Stop all telemetry tracking
 */
export function stopTracking() {
  if (mouseTrackingCleanup) {
    mouseTrackingCleanup();
    mouseTrackingCleanup = null;
  }
  isTracking = false;
}

/**
 * Check if tracking is active
 */
export function isTrackingActive() {
  return isTracking;
}

/**
 * Initialize tab visibility tracking
 * Tracks: tab enter, tab leave, tab close
 */
export function initTabTracking() {
  const handleVisibilityChange = () => {
    if (document.hidden) {
      // User left the tab (but may not have closed it)
      trackTabLeave();
    } else {
      // User returned to the tab
      trackTabEnter();
    }
  };

  // Track beforeunload for tab close
  const handleBeforeUnload = (e) => {
    trackTabClose();
    // Send telemetry data before closing
    const telemetryData = getTelemetryData();
    // Store in sessionStorage for retrieval after reopen
    try {
      sessionStorage.setItem('telemetry_final', telemetryData);
    } catch (err) {
      alert("sessionStorage is not available. Telemetry data may not be saved on tab close.");
    }
  };

  // Cleanup function
  const cleanup = () => {
    document.removeEventListener('visibilitychange', handleVisibilityChange);
    window.removeEventListener('beforeunload', handleBeforeUnload);
  };

  document.addEventListener('visibilitychange', handleVisibilityChange);
  window.addEventListener('beforeunload', handleBeforeUnload);

  console.log('[Telemetry] Tab tracking initialized');

  return cleanup;
}

export default {
  trackMouseMove,
  trackMouseClick,
  trackAddToCart,
  trackRemoveFromCart,
  trackTabEnter,
  trackTabLeave,
  trackTabClose,
  trackNavigateAway,
  getTelemetryData,
  getTelemetryBuffer,
  clearTelemetry,
  initMouseTracking,
  initTabTracking,
  stopTracking,
  isTrackingActive,
};