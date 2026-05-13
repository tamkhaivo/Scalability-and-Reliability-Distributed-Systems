#!/usr/bin/env python3
"""
Selenium script to automate web browsing with mouse movements and random clicks.
Usage: python automate_browse.py <URL> [seconds]
"""

import sys
import time
import random
from requests import options
from selenium import webdriver
from selenium.webdriver.common.by import By
# from selenium.webdriver.chrome.options import Options
# from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.action_chains import ActionChains
from webdriver_manager.firefox import GeckoDriverManager
from selenium.webdriver.firefox.service import Service
from selenium.webdriver.firefox.options import Options as FirefoxOptions

def setup_driver():
    """Configure Firefox driver with headless options."""
    options = FirefoxOptions()
    # options.add_argument("--headless")
    options.add_argument("--width=1920")
    options.add_argument("--height=1080")
    
    driver = webdriver.Firefox(service=Service(GeckoDriverManager().install()), options=options)
    return driver


def simulate_mouse_movements(driver, duration_seconds):
    """Simulate random mouse movements across the page."""
    print(f"Simulating mouse movements for {duration_seconds} seconds...")
    
    start_time = time.time()
    actions = ActionChains(driver)
    
    while time.time() - start_time < duration_seconds:
        try:
            # Find all clickable elements
            clickable_elements = driver.find_elements(
                By.CSS_SELECTOR, 
                "button, a, input[type='submit'], input[type='button'], "
                "[role='button'], .btn, [onclick], [tabindex]:not([tabindex='-1'])"
            )
            
            if clickable_elements:
                # Randomly choose an action
                action_choice = random.randint(0, 2)
                
                if action_choice == 0 and len(clickable_elements) > 0:
                    # Move mouse to a random element
                    elem = random.choice(clickable_elements)
                    try:
                        actions.move_to_element(elem).perform()
                        time.sleep(random.uniform(0.1, 0.5))
                    except Exception:
                        pass
                        
                elif action_choice == 1:
                    # Scroll down randomly
                    scroll_amount = random.randint(100, 500)
                    driver.execute_script(f"window.scrollBy(0, {scroll_amount})")
                    time.sleep(random.uniform(0.2, 0.8))
                    
                else:
                    # Random click on a button if available
                    buttons = driver.find_elements(By.TAG_NAME, "button")
                    if buttons:
                        button = random.choice(buttons)
                        try:
                            if button.is_displayed() and button.is_enabled():
                                button.click()
                                time.sleep(random.uniform(0.5, 1.5))
                                # Go back if we navigated away
                                driver.back()
                                time.sleep(1)
                        except Exception:
                            pass
            else:
                # Just scroll if no clickable elements
                driver.execute_script(f"window.scrollBy(0, {random.randint(100, 300)})")
                time.sleep(random.uniform(0.3, 0.7))
                
        except Exception as e:
            print(f"Warning: {e}")
            time.sleep(0.5)
    
    print("Mouse movement simulation complete.")


def click_checkout_button(driver):
    """Find and click the 'Proceed to checkout' button."""
    print("Clicking 'Proceed to checkout' button...")
    
    elem = driver.find_element(By.ID, "btn-checkout")
    elem.click()
    print("Successfully clicked 'Proceed to checkout'!")
    return True

def main():
    if len(sys.argv) < 2:
        print("Usage: python automate_browse.py <URL> [seconds]")
        print("  URL      - The URL to open")
        print("  seconds  - Duration to simulate mouse movements (default: 10)")
        sys.exit(1)
    
    url = sys.argv[1]
    duration = int(sys.argv[2]) if len(sys.argv) > 2 else 10
    
    print(f"Opening URL: {url}")
    print(f"Simulation duration: {duration} seconds")
    
    driver = None
    try:
        driver = setup_driver()
        driver.get(url)
        print(f"Page title: {driver.title}")
        
        # Wait for page to load
        time.sleep(2)
        
        # Simulate mouse movements and random clicks
        simulate_mouse_movements(driver, duration)
        
        # Click proceed to checkout
        action_choice = random.randint(0, 2)

        if action_choice == 0:
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
            print("Browser closed. Exiting.")


if __name__ == "__main__":
    main()