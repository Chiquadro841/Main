from selenium.webdriver import Chrome, ChromeService
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By

service = ChromeService("chromedriver.exe")
driver = Chrome(service=service)

url= "https://www.scrapethissite.com/pages/ajax-javascript/"

driver.get(url)


# Trova il div con class="col-md-12 text-center"
div_element = driver.find_element(By.CLASS_NAME, "col-md-12.text-center")

# Cerca tutti i tag <a> all'interno del div trovato
anchor_tags = div_element.find_elements(By.TAG_NAME, "a")

# Stampa il testo di ogni tag <a>
for a_tag in anchor_tags:
    print(a_tag.text)

# Chiudere il browser
driver.quit()