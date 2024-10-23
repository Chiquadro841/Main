import requests
from bs4 import BeautifulSoup
import pandas as pd


dati_libri = []

for i in range(1,6):
    url=f"https://books.toscrape.com/catalogue/page-{i}.html"
    response = requests.get(url)

    if response.status_code ==200:
        soup = BeautifulSoup(response.text , "html.parser")

        books = soup.find_all("li", class_="col-xs-6 col-sm-4 col-md-3 col-lg-3")

         # Itera attraverso ogni libro
        for book in books:

            #cerca titolo
            h3 = book.find("h3")
            titolo= h3.find("a")['title']
            
            # Cerca rating
            star_rating = book.find("p", class_="star-rating")  # Trova l'elemento <p> con la classe star-rating
            ratings = [rating for rating in ["One", "Two", "Three", "Four", "Five"] if rating in star_rating['class']]
            rating= ratings[0]

            #cerca disponibilità        
            disponibilita = book.find("p", class_="instock availability").text.strip()
           
            #cerca prezzo
            prezzo = book.find("p" , class_="price_color").text.strip("Â")

            # Aggiungi i dati del libro 
            dati_libri.append((titolo, prezzo, rating, disponibilita))


    else:
        print(f"Errore nella richiesta per la pagina {i}: {response.status_code}")  # Log dell'errore

# Creazione del DataFrame
df = pd.DataFrame(dati_libri, columns=["Titolo", "Prezzo", "Rating", "Disponibilità"])
            
            
#head del dataframe
print(df.head())

# Salva il dataframe in un file CSV 
df.to_csv("books.csv", index=False)


#controllo numero libri
print(f"Numero di libri trovati: {len(dati_libri)}")


