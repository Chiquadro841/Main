from selenium.webdriver import Chrome, ChromeService
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By

service = ChromeService("chromedriver.exe")
driver = Chrome(service=service)

url= "https://www.scrapethissite.com/pages/ajax-javascript/"

driver.get(url)

element = WebDriverWait(driver, 10).until(
        EC.element_to_be_clickable((By.ID, "2015"))
    )

# Eseguire l'azione clic
element.click()

WebDriverWait(driver, 10).until(
    EC.presence_of_element_located((By.CLASS_NAME, "film"))
    )

films = driver.find_elements(By.CLASS_NAME, "film")

for film in films:
    
    title = film.find_element(By.CLASS_NAME, "film-title")
    nom = film.find_element( By.CLASS_NAME, "film-nominations")
    awards = film.find_element( By.CLASS_NAME, "film-awards")
    
    print(f" Titolo: {title.text}, nominations: {nom.text}, awards: {awards.text}")
    

# Chiudere il browser alla fine



#classi  film-title / film-nominations / film-awards




