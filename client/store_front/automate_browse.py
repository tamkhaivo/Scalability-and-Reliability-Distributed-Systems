#!/usr/bin/env python3
"""
Selenium script to automate web browsing with mouse movements and random clicks.
Usage: python automate_browse.py <URL> [numOfEvents] [seconds]
"""

import sys
import time
import random
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options as ChromeOptions
from webdriver_manager.chrome import ChromeDriverManager

def setup_driver(headless=False):
    """Configure Chrome driver with options."""
    options = ChromeOptions()
    if headless:
        options.add_argument("--headless=new")
    options.add_argument("--width=1920")
    options.add_argument("--height=1080")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")

    # Use ChromeDriverManager to automatically handle binary downloads
    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=options)
    return driver

def simulate_high_frequency_events(driver, events_per_second, duration_seconds):
    """Simulate a high volume of telemetry events over a specified duration."""
    print(f"Generating ~{events_per_second} events/sec for {duration_seconds} seconds...")

    # Increase script timeout to match duration + buffer
    driver.set_script_timeout(duration_seconds + 5)

    # Inject high-speed event generation script using execute_async_script
    # The last argument in 'arguments' is the callback function to return data to Python
    js_payload = f"""
    const callback = arguments[arguments.length - 1];
    (async () => {{
        const targetEPS = {events_per_second};
        const durationMs = {duration_seconds} * 1000;
        const clickable = Array.from(document.querySelectorAll("button, a, input, [role='button']"));
        const addButtons = clickable.filter(b => b.id && b.id.includes('add-to-cart'));

        let totalCount = 0;
        const startTime = performance.now();

        try {{
            while ((performance.now() - startTime) < durationMs) {{
                const cycleStartTime = performance.now();
                let cycleCount = 0;

                // Try to hit the target events for this small slice of time (100ms)
                while (cycleCount < (targetEPS / 10) && (performance.now() - cycleStartTime) < 100) {{
                    const type = Math.random();
                    if (type < 0.15) {{
                        // Mousemove (15% probability)
                        const x = Math.random() * window.innerWidth;
                        const y = Math.random() * window.innerHeight;
                        document.dispatchEvent(new MouseEvent('mousemove', {{
                            view: window, bubbles: true, clientX: x, clientY: y
                        }}));
                    }} else if (type < 0.20) {{
                        // Scroll (5% probability - minimized as requested)
                        window.scrollBy(0, Math.random() > 0.5 ? 50 : -50);
                    }} else {{
                        // Click (80% probability - prioritized)
                        // Target 'Add to Cart' buttons specifically if they exist
                        const target = addButtons.length > 0 && Math.random() < 0.8 
                            ? addButtons[Math.floor(Math.random() * addButtons.length)]
                            : clickable[Math.floor(Math.random() * clickable.length)];

                        if (target) {{
                            target.dispatchEvent(new MouseEvent('click', {{
                                view: window, bubbles: true, clientX: 100, clientY: 100
                            }}));
                        }}
                    }}
                    cycleCount++;
                    totalCount++;
                }}
                // Yield to the browser for a tiny bit to keep telemetry flushing and prevent freezing
                await new Promise(r => setTimeout(r, 10));
            }}
            callback(totalCount);
        }} catch (e) {{
            console.error("Simulation Error:", e);
            callback(totalCount || 0);
        }}
    }})();
    """

    start_time = time.time()
    total_events = driver.execute_async_script(js_payload)
    actual_duration = time.time() - start_time

    # Handle cases where total_events might still be None (though unlikely with execute_async_script)
    if total_events is None:
        total_events = 0

    print(f"Simulation complete. Created {total_events} total events in {actual_duration:.2f}s (~{total_events/max(0.1, actual_duration):.1f} EPS).")

def click_checkout_button(driver):
    """Find and click the 'Proceed to checkout' button."""
    print("Clicking 'Proceed to checkout' button...")
    try:
        elem = driver.find_element(By.ID, "btn-checkout")
        elem.click()
        print("Successfully clicked 'Proceed to checkout'!")
        return True
    except Exception as e:
        print(f"Could not click checkout button: {e}")
        return False

def main():
    if len(sys.argv) < 2:
        print("Usage: python automate_browse.py <URL> [eventsPerSecond] [durationSeconds] [--headless]")
        print("  URL              - The URL to open")
        print("  eventsPerSecond  - Events to generate per second (default: 100)")
        print("  durationSeconds  - Duration of the test in seconds (default: 60)")
        print("  --headless       - Run browser in headless mode")
        sys.exit(1)

    url = sys.argv[1]
    
    # Improved argument parsing
    events_per_second = 100
    duration_seconds = 60
    headless = False
    
    # Filter out --headless first
    args = []
    for arg in sys.argv[1:]:
        if arg == "--headless":
            headless = True
        else:
            args.append(arg)
            
    # args[0] is URL
    if len(args) > 1:
        events_per_second = int(args[1])
    if len(args) > 2:
        duration_seconds = int(args[2])

    print(f"Opening URL: {url}")
    print(f"Target throughput: {events_per_second} events/second")
    print(f"Target duration: {duration_seconds} seconds")
    if headless:
        print("Running in headless mode")

    driver = None
    try:
        driver = setup_driver(headless=headless)
        driver.get(url)
        print(f"Page title: {driver.title}")

        # Wait for page to load
        time.sleep(2)

        # Simulate high-frequency events over duration
        simulate_high_frequency_events(driver, events_per_second, duration_seconds)

        # Click proceed to checkout with some probability
        if random.random() < 0.5:
            click_checkout_button(driver)
        else:
            print("Skipping 'Proceed to checkout' click this time.")

    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)

    finally:
        if driver:
            time.sleep(2)
            # driver.quit()
            print("Browser execution finished.")


if __name__ == "__main__":
    main()