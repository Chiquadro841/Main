from selenium.webdriver import Chrome, ChromeService
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By


# Specifica il percorso di ChromeDriver
chrome_driver_path = r"C:\Users\Brandon\Desktop\chromedriver.exe"

# Imposta il servizio per ChromeDriver
service = ChromeService(chrome_driver_path)

# Crea un'istanza del browser Chrome
driver = Chrome(service=service)

url= "https://quotes.toscrape.com/js/"

driver.get(url)

output= WebDriverWait(driver, 5).until(EC.presence_of_element_located((By.TAG_NAME, "h1")) )

keyword= "scrape"

all_headers= []

for h_tags in ["h1","h2","h3","h4","h5","h6"]:
    h= driver.find_elements(By.TAG_NAME, h_tags)
    all_headers.extend(h)

#print(all_headers)

#ora cerchiamo la keyword nei tag trovati

found= any(keyword.lower() in header.text.lower() for header in all_headers )
print(found)